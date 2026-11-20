//! Source resume and canonical input-lane replacement across the native relay.

// Rust guideline compliant 2026-10-07
use super::focus::input;
use super::*;
use std::sync::Arc;
use vivid_protocol::{
    messages,
    overlay::{
        WindowOptions,
        wire::{Action, Query, SetWindow, WindowAction, WindowAddress},
    },
    registry,
    vector::{Command, Frame, HitRole},
};
use vivid_sdk::presenter::{
    BridgeSurfaceKey, NativeVectorHost, OverlayHostRequest, SourceKey, VectorInputEvent,
    VectorInputResult, VectorInputTarget,
};
use vivid_sdk::{
    CleanupPolicy, MILESTONE_OUTPUT_READY, OverlayInputLane, SLOT_VECTOR, SessionLeaseBuilder,
    SlotBinding, Surface, SurfaceBuilder, Track, TrackChannel, TrackWaitCondition,
    VectorConfiguration,
};

#[cfg(feature = "native-gpu-testing")]
#[path = "recovery/gpu.rs"]
mod gpu;

#[path = "recovery/outer.rs"]
mod outer;

#[path = "recovery/capture_pressure.rs"]
mod capture_pressure;

#[path = "recovery/late_lane.rs"]
mod late_lane;

pub(super) struct Producer {
    pub(super) session: Session,
    pub(super) lane: OverlayInputLane,
    pub(super) surface: Surface,
    track: Track,
    channel: TrackChannel,
}
fn config(inner: &VirtualVivid, authentication: ProducerAuthentication) -> ProducerConfig {
    let mut config = ProducerConfig {
        authentication,
        required_profiles: [
            registry::CORE_CONTROL,
            registry::LIVE_MEDIA,
            registry::OBSERVABILITY,
            registry::TERMINAL_SURFACE,
            registry::TERMINAL_OVERLAY,
            registry::VECTOR_SCENE,
            registry::OVERLAY_INPUT,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        optional_profiles: Vec::new(),
        ..ProducerConfig::default()
    };
    config.required_profiles.sort();
    pin_endpoints(&mut config, inner.endpoint());
    config
}
fn fixture() -> io::Result<(
    Arc<VirtualVivid>,
    Arc<VirtualVivid>,
    NativeVectorHost,
    OuterBridge,
)> {
    fixture_mode(false)
}
fn fixture_mode(
    presentations: bool,
) -> io::Result<(
    Arc<VirtualVivid>,
    Arc<VirtualVivid>,
    NativeVectorHost,
    OuterBridge,
)> {
    fixture_geometry(presentations, COLUMNS, ROWS)
}
fn fixture_geometry(
    presentations: bool,
    columns: u16,
    rows: u16,
) -> io::Result<(
    Arc<VirtualVivid>,
    Arc<VirtualVivid>,
    NativeVectorHost,
    OuterBridge,
)> {
    let (inner, _) = overlay_presenter()?;
    let inner = Arc::new(inner);
    inner.enable_overlay_host_relay();
    let (outer, endpoint) = overlay_presenter()?;
    let outer = Arc::new(outer);
    let host = NativeVectorHost::attach(Arc::clone(&outer), "")?;
    let secret = pane(&outer, 9)?;
    outer.update_metrics(9, columns, rows, CELL);
    let builder = OuterBridge::builder(
        Secret32::from_hex(&secret).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .control_endpoint(endpoint)
    .overlay_action_forwarding(&inner);
    let relay = if presentations {
        builder.overlay_presentation_forwarding(&inner)
    } else {
        builder
    }
    .build()?;
    Ok((inner, outer, host, relay))
}
impl Producer {
    pub(super) fn new(inner: &VirtualVivid, pane_id: u64, leased: bool) -> io::Result<Self> {
        let secret = pane(inner, pane_id)?;
        let authentication = if leased {
            let profiles = config(
                inner,
                ProducerAuthentication::root_hex(&secret).map_err(io::Error::other)?,
            )
            .required_profiles;
            let (definition, mut activation) = SessionLeaseBuilder::new(1, 9)
                .permitted_profiles(profiles)
                .contract(vivid_protocol::resource::ResourceContract::new(
                    [u64::MAX / 4; vivid_protocol::resource::RESOURCE_COUNT],
                ))
                .cleanup_policy(CleanupPolicy::SuspendOnUncleanLoss)
                .disconnect_grace_us(5_000_000)
                .build()
                .map_err(io::Error::other)?;
            inner.issue_lease(pane_id, definition)?;
            ProducerAuthentication::lease_activation_bytes(1, 9, activation.take().unwrap())
                .map_err(io::Error::other)?
        } else {
            ProducerAuthentication::root_hex(&secret).map_err(io::Error::other)?
        };
        let mut session = Session::connect(config(inner, authentication))?;
        let lane = session.open_overlay_input_lane(1)?;
        lane.renew(5_000_000, Duration::from_secs(1))?;
        let (surface, track, channel) =
            Self::create_window(&mut session, if pane_id == 1 { 0. } else { 64. })?;
        Ok(Self {
            session,
            lane,
            surface,
            track,
            channel,
        })
    }
    fn create_window(session: &mut Session, x: f64) -> io::Result<(Surface, Track, TrackChannel)> {
        let definition = SurfaceBuilder::new(session, 48, 48)?
            .titled(SurfaceRole::Figure, "recovery source")
            .build()?;
        let surface = session.create_surface(definition, &RequestMetadata::default())?;
        let track = session.create_track(
            TrackConfiguration {
                direction: vivid_sdk::TrackDirection::default(),
                context_id: surface.context_id(),
                surface_id: surface.id(),
                track_id: session.allocate_id()?,
                slot: SLOT_VECTOR,
                mode: TrackMode::Live,
                lane: LaneClass::Bulk,
                maximum_record_body: 65536,
                maximum_rate_millihertz: 60_000,
                maximum_encoded_bits_per_second: 65536 * 8 * 60,
                maximum_records_per_second: 60,
                maximum_inflight_body_bytes: 65536,
                target_latency_us: 0,
                maximum_latency_us: 0,
                retained_pixel_charge: 48 * 48,
                kind: KindConfiguration::VectorScene(VectorConfiguration {
                    width: 48,
                    height: 48,
                    maximum_scene_bytes: 65524,
                }),
            },
            &RequestMetadata::default(),
        )?;
        let channel = session.open_track_channel(&track)?;
        session.set_overlay_window(&SetWindow {
            address: WindowAddress {
                context_id: surface.context_id(),
                surface_id: surface.id(),
                generation: surface.generation().get(),
            },
            expected_revision: 0,
            options: WindowOptions::new(Rect::new(x, 0., 48., 48.).unwrap(), WindowMode::Floating),
        })?;
        Ok((surface, track, channel))
    }
    pub(super) fn key(&self) -> BridgeSurfaceKey {
        BridgeSurfaceKey {
            producer: self.session.info().session_id,
            context: self.surface.context_id(),
            surface: self.surface.id(),
        }
    }
    pub(super) fn address(&self) -> WindowAddress {
        WindowAddress {
            context_id: self.surface.context_id(),
            surface_id: self.surface.id(),
            generation: self.surface.generation().get(),
        }
    }
    pub(super) fn status(&self) -> io::Result<vivid_protocol::overlay::wire::Status> {
        self.session.query_overlay_window(Query {
            context_id: self.surface.context_id(),
            surface_id: self.surface.id(),
        })
    }
    pub(super) fn paint(&mut self, revision: u64, color: u32) -> io::Result<()> {
        Self::paint_window(
            &mut self.session,
            &self.surface,
            &self.track,
            &self.channel,
            revision,
            color,
        )
    }
    fn paint_window(
        session: &mut Session,
        surface: &Surface,
        track: &Track,
        channel: &TrackChannel,
        revision: u64,
        color: u32,
    ) -> io::Result<()> {
        let mut canvas = Canvas::new();
        let path = Path::rectangle(Rect::new(0., 0., 48., 48.).unwrap()).unwrap();
        canvas
            .fill(path.clone(), Brush::Solid(Color(color)))
            .unwrap();
        canvas
            .push(Command::Hit {
                id: 7,
                path,
                role: HitRole::Input,
                cursor: None,
            })
            .unwrap();
        channel.send_vector(&Frame {
            epoch: 1,
            revision,
            canvas,
        })?;
        session.wait_track(
            track,
            TrackWaitCondition::MilestoneSet,
            Some(MILESTONE_OUTPUT_READY),
            1_000_000,
        )?;
        session.activate_tracks(
            surface,
            &[SlotBinding {
                slot: SLOT_VECTOR,
                track_id: track.id(),
                expected_channel_generation: track.channel_generation(),
                required_milestone: MILESTONE_OUTPUT_READY,
            }],
            &RequestMetadata::default(),
        )?;
        Ok(())
    }
    fn replace_lane(&mut self, generation: u64) -> io::Result<()> {
        let lane = self.session.open_overlay_input_lane(generation)?;
        lane.renew(5_000_000, Duration::from_secs(1))?;
        let old = std::mem::replace(&mut self.lane, lane);
        let _ = old.close();
        self.lane.renew(5_000_000, Duration::from_secs(1))?;
        self.session
            .open_overlay_input_lane(generation)
            .unwrap_err();
        Ok(())
    }
}
fn pending(inner: &VirtualVivid, count: usize) -> Vec<OverlayHostRequest> {
    let end = Instant::now() + Duration::from_secs(3);
    let mut requests = Vec::new();
    while requests.len() < count {
        requests.extend(inner.take_overlay_host_requests());
        assert!(Instant::now() < end, "action queue stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(requests.len(), count);
    requests
}
fn wait(inner: &VirtualVivid, relay: &mut OuterBridge, done: impl Fn() -> bool) -> io::Result<()> {
    let end = Instant::now() + Duration::from_secs(3);
    while !done() {
        input(inner, relay)?;
        for (id, result) in relay.take_overlay_host_replies() {
            inner.complete_overlay_host_request(id, result.map_err(|e| e.to_string()));
        }
        assert!(
            Instant::now() < end,
            "recovery did not retire old authority promptly"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    input(inner, relay)
}
fn project(inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    let p = snapshot(inner, &[1, 2]).bridge_projection();
    relay.rebuild(&p.surfaces, &p.sources, &p.nodes)?;
    Ok(())
}
fn action(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    p: &Producer,
    kind: WindowAction,
) -> io::Result<()> {
    let handle = p.session.overlay_host_handle().unwrap();
    let address = p.address();
    let expected_revision = p.status()?.window.expected_revision;
    std::thread::scope(|scope| {
        let job = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                address.surface_id,
                Action {
                    address,
                    expected_revision,
                    action: kind,
                }
                .payload()?,
            )
        });
        let request = pending(inner, 1).pop().unwrap();
        project(inner, relay)?;
        relay.start_overlay_host_request(request)?;
        wait(inner, relay, || job.is_finished())?;
        job.join().unwrap().map(|_| ())
    })
}
fn display(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    host: &mut NativeVectorHost,
    count: usize,
) -> io::Result<VectorInputTarget> {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        relay.retry_pending_activation()?;
        input(inner, relay)?;
        let frame = host.prepare(9, 1.)?;
        if host.presented(&frame)? && frame.drawings().len() == count {
            return Ok(host.input_target(&frame).unwrap());
        }
        assert!(Instant::now() < end, "native recovery frame stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn hydrate(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    host: &mut NativeVectorHost,
) -> io::Result<VectorInputTarget> {
    let _ = relay.take_full_frame_requests();
    for source in snapshot(inner, &[1, 2]).sources {
        for (record_type, bytes) in source.retained_vector {
            relay.media_chunk(MediaChunk {
                delivery_id: 0,
                source: source.key,
                record_type,
                offset: 0,
                total: u32::try_from(bytes.len()).unwrap(),
                last: true,
                bytes: bytes.to_vec(),
            })?;
        }
    }
    display(inner, relay, host, 2)?;
    let end = Instant::now() + Duration::from_secs(3);
    let mut completed = 0;
    while completed < 2 {
        for result in relay.take_media_completions() {
            assert!(result.delivered);
            completed += 1;
        }
        display(inner, relay, host, 2)?;
        assert!(Instant::now() < end, "recovery delivery receipt stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
    display(inner, relay, host, 2)
}
fn pointer(
    host: &mut NativeVectorHost,
    target: &VectorInputTarget,
    x: f64,
) -> io::Result<VectorInputResult> {
    host.dispatch_input(
        target,
        VectorInputEvent::Pointer {
            position: [x, 10.],
            button: Some((0, true)),
            modifiers: 0,
        },
    )
}

fn drained(relay: &mut OuterBridge) -> io::Result<Vec<vivid_sdk::presenter::OverlayRelayInput>> {
    let end = Instant::now() + Duration::from_secs(2);
    loop {
        let inputs = relay.take_overlay_routed_input()?;
        if !inputs.is_empty() {
            return Ok(inputs);
        }
        assert!(
            Instant::now() < end,
            "physical input receipt did not arrive"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn replacing_an_input_lane_retires_queued_actions_and_old_physical_input_but_preserves_control()
-> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture()?;
    let mut a = Producer::new(&inner, 1, false)?;
    let mut b = Producer::new(&inner, 2, false)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    project(&inner, &mut relay)?;
    let stale = hydrate(&inner, &mut relay, &mut host)?;
    assert!(matches!(
        pointer(&mut host, &stale, 10.)?,
        VectorInputResult::Delivered { .. }
    ));
    assert_eq!(
        relay.take_overlay_input().unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
    let saved = drained(&mut relay)?;
    let outer_a = relay.outer_surface_id(a.key()).unwrap();
    let outer_b = relay.outer_surface_id(b.key()).unwrap();
    action(&inner, &mut relay, &b, WindowAction::Focus)?;
    let handle = a.session.overlay_host_handle().unwrap();
    let address = a.address();
    std::thread::scope(|scope| -> io::Result<()> {
        let mut jobs = Vec::new();
        for kind in [
            WindowAction::Focus,
            WindowAction::Raise,
            WindowAction::Lower,
        ] {
            let handle = Arc::clone(&handle);
            jobs.push(scope.spawn(move || {
                handle(
                    messages::OVERLAY_ACTION,
                    address.surface_id,
                    Action {
                        address,
                        expected_revision: 1,
                        action: kind,
                    }
                    .payload()?,
                )
            }));
        }
        let requests = pending(&inner, 3);
        let control_handle = Arc::clone(&handle);
        let control = scope.spawn(move || {
            control_handle(
                messages::OVERLAY_ACTION,
                address.surface_id,
                Action {
                    address,
                    expected_revision: 1,
                    action: WindowAction::Center,
                }
                .payload()?,
            )
        });
        let control_request = pending(&inner, 1).pop().unwrap();
        a.replace_lane(2)?;
        for input in saved {
            assert!(!input.forward(), "drained input survived lane replacement");
        }
        wait(&inner, &mut relay, || {
            jobs.iter().all(std::thread::ScopedJoinHandle::is_finished)
        })?;
        for job in jobs {
            job.join().unwrap().unwrap_err();
        }
        for request in requests {
            assert!(relay.can_service_overlay_host_request(&request));
            relay.start_overlay_host_request(request).unwrap_err();
        }
        assert_eq!(a.status()?.window.expected_revision, 1);
        assert!(
            !control.is_finished(),
            "lane replacement cancelled a control-only Center"
        );
        project(&inner, &mut relay)?;
        relay.start_overlay_host_request(control_request)?;
        wait(&inner, &mut relay, || control.is_finished())?;
        control.join().unwrap()?;
        assert_eq!(a.status()?.window.expected_revision, 2);
        assert_ne!(relay.outer_surface_id(a.key()), Some(outer_a));
        assert_eq!(relay.outer_surface_id(b.key()), Some(outer_b));
        assert_eq!(
            pointer(&mut host, &stale, 10.)?,
            VectorInputResult::Rejected
        );
        let fresh = hydrate(&inner, &mut relay, &mut host)?;
        assert!(matches!(
            pointer(&mut host, &fresh, 74.)?,
            VectorInputResult::Delivered { .. }
        ));
        input(&inner, &mut relay)?;
        assert!(b.status()?.focused);
        a.lane.close()?;
        // A new popup created during the lane gap must stay with its parent
        // while reconciliation retires that parent's previous physical identity.
        let child_definition = SurfaceBuilder::new(&a.session, 24, 24)?.build()?;
        let child = a
            .session
            .create_surface(child_definition, &RequestMetadata::default())?;
        let mut child_options =
            WindowOptions::new(Rect::new(200., 200., 24., 24.).unwrap(), WindowMode::Popup);
        child_options.parent = Some(
            address.identity(
                vivid_protocol::identity::SessionIdentity::new(
                    vivid_protocol::identity::PresenterInstanceId([0; 16]),
                    a.session.info().session_id,
                )
                .map_err(io::Error::other)?,
            )?,
        );
        a.session.set_overlay_window(&SetWindow {
            address: WindowAddress {
                context_id: child.context_id(),
                surface_id: child.id(),
                generation: child.generation().get(),
            },
            expected_revision: 0,
            options: child_options,
        })?;
        action(&inner, &mut relay, &a, WindowAction::Center)?;
        action(&inner, &mut relay, &a, WindowAction::Close)?;
        assert!(b.status()?.focused);
        display(&inner, &mut relay, &mut host, 1)?;
        Ok(())
    })
}

#[test]
fn replacing_the_lane_after_physical_focus_cannot_deliver_old_feedback_or_acknowledgement()
-> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture()?;
    let mut a = Producer::new(&inner, 1, false)?;
    let mut b = Producer::new(&inner, 2, false)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    project(&inner, &mut relay)?;
    hydrate(&inner, &mut relay, &mut host)?;
    let handle = a.session.overlay_host_handle().unwrap();
    let address = a.address();
    std::thread::scope(|scope| -> io::Result<()> {
        let job = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                address.surface_id,
                Action {
                    address,
                    expected_revision: 1,
                    action: WindowAction::Focus,
                }
                .payload()?,
            )
        });
        let request = pending(&inner, 1).pop().unwrap();
        let reply = relay.overlay_host_request(&request)?;
        a.replace_lane(2)?;
        wait(&inner, &mut relay, || job.is_finished())?;
        job.join().unwrap().unwrap_err();
        inner.complete_overlay_host_request(request.id, Ok(reply));
        input(&inner, &mut relay)?;
        assert!(
            !a.status()?.focused,
            "old physical focus feedback was retagged for replacement lane"
        );
        assert_eq!(a.status()?.window.expected_revision, 1);
        project(&inner, &mut relay)?;
        hydrate(&inner, &mut relay, &mut host)?;
        action(&inner, &mut relay, &a, WindowAction::Focus)?;
        display(&inner, &mut relay, &mut host, 2)?;
        wait(&inner, &mut relay, || {
            a.status().is_ok_and(|status| status.focused)
        })?;
        assert_eq!(a.status()?.window.expected_revision, 2);
        Ok(())
    })
}

#[test]
fn leased_source_resume_preserves_ids_but_retires_old_actions_frames_and_channels_only_for_its_owner()
-> io::Result<()> {
    leased_source_resume(false)
}
#[test]
fn leased_source_resume_fences_old_authority_with_deferred_physical_presentation() -> io::Result<()>
{
    leased_source_resume(true)
}
fn leased_source_resume(presentations: bool) -> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture_mode(presentations)?;
    let mut a = Producer::new(&inner, 1, true)?;
    let mut b = Producer::new(&inner, 2, false)?;
    assert_eq!(a.surface.id(), b.surface.id());
    assert_eq!(a.surface.context_id(), b.surface.context_id());
    assert_eq!(a.track.id(), b.track.id());
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    project(&inner, &mut relay)?;
    let stale = hydrate(&inner, &mut relay, &mut host)?;
    assert!(matches!(
        pointer(&mut host, &stale, 10.)?,
        VectorInputResult::Delivered { .. }
    ));
    let saved = drained(&mut relay)?;
    let outer_b = relay.outer_surface_id(b.key());
    let old_owner = a.session.info().session_id;
    let old_generation = a.session.info().resume_generation;
    let authentication = a.session.resume_authentication()?;
    let handle = a.session.overlay_host_handle().unwrap();
    let address = a.address();
    std::thread::scope(|scope| -> io::Result<()> {
        let queued_handle = Arc::clone(&handle);
        let queued = scope.spawn(move || {
            queued_handle(
                messages::OVERLAY_ACTION,
                address.surface_id,
                Action {
                    address,
                    expected_revision: 1,
                    action: WindowAction::Focus,
                }
                .payload()?,
            )
        });
        let queued_request = pending(&inner, 1).pop().unwrap();
        let claimed = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                address.surface_id,
                Action {
                    address,
                    expected_revision: 1,
                    action: WindowAction::Center,
                }
                .payload()?,
            )
        });
        let claimed_request = pending(&inner, 1).pop().unwrap();
        let reply = relay.overlay_host_request(&claimed_request)?;
        (a.session.cancel_handle())();
        wait(&inner, &mut relay, || {
            inner
                .desktop_session_generation(SourceKey {
                    producer: old_owner,
                    context: address.context_id,
                    surface: address.surface_id,
                    track: a.track.id(),
                })
                .is_none()
        })?;
        wait(&inner, &mut relay, || {
            queued.is_finished() && claimed.is_finished()
        })?;
        queued.join().unwrap().unwrap_err();
        claimed.join().unwrap().unwrap_err();
        let mut resumed = Session::connect(config(&inner, authentication))?;
        assert_eq!(resumed.info().session_id, old_owner);
        assert_eq!(resumed.info().resume_generation, old_generation + 1);
        resumed.adopt_surface(&a.surface)?;
        resumed.adopt_track(&a.track)?;
        resumed.advance_channel(&a.track, 1, &RequestMetadata::default())?;
        let channel = resumed.open_track_channel(&a.track)?;
        resumed
            .open_overlay_input_lane(1)
            .expect_err("old lane generation reopened after resume");
        let lane = resumed.open_overlay_input_lane(2)?;
        lane.renew(5_000_000, Duration::from_secs(1))?;
        a.session = resumed;
        for input in saved {
            assert!(!input.forward(), "drained input survived source resume");
        }
        let old_lane = std::mem::replace(&mut a.lane, lane);
        old_lane
            .renew(5_000_000, Duration::from_secs(1))
            .unwrap_err();
        let _ = old_lane.close();
        a.lane.renew(5_000_000, Duration::from_secs(1))?;
        let old_channel = std::mem::replace(&mut a.channel, channel);
        old_channel
            .send_vector(&Frame {
                epoch: 1,
                revision: 99,
                canvas: Canvas::new(),
            })
            .unwrap_err();
        let _ = old_channel.close();
        inner.complete_overlay_host_request(claimed_request.id, Ok(reply));
        for request in [queued_request, claimed_request] {
            assert!(relay.can_service_overlay_host_request(&request));
            relay.start_overlay_host_request(request).unwrap_err();
        }
        assert_eq!(a.status()?.window.expected_revision, 1);
        project(&inner, &mut relay)?;
        assert_eq!(relay.outer_surface_id(b.key()), outer_b);
        assert_eq!(
            pointer(&mut host, &stale, 10.)?,
            VectorInputResult::Rejected
        );
        a.paint(2, 0x00ff_00ff)?;
        b.paint(2, 0xffff_ffff)?;
        project(&inner, &mut relay)?;
        let fresh = hydrate(&inner, &mut relay, &mut host)?;
        assert!(matches!(
            pointer(&mut host, &fresh, 74.)?,
            VectorInputResult::Delivered { .. }
        ));
        input(&inner, &mut relay)?;
        wait(&inner, &mut relay, || {
            b.status().is_ok_and(|status| status.focused)
        })?;
        assert!(b.status()?.focused);
        action(&inner, &mut relay, &a, WindowAction::Focus)?;
        display(&inner, &mut relay, &mut host, 2)?;
        wait(&inner, &mut relay, || {
            a.status().is_ok_and(|status| status.focused)
        })?;
        assert_eq!(a.session.query_track(&a.track)?.attachment_state, 1);
        assert_eq!(b.session.query_track(&b.track)?.attachment_state, 1);
        a.paint(3, 0xffff_00ff)?;
        project(&inner, &mut relay)?;
        hydrate(&inner, &mut relay, &mut host)?;
        a.session
            .destroy_surface(&a.surface, &RequestMetadata::default())?;
        project(&inner, &mut relay)?;
        display(&inner, &mut relay, &mut host, 1)?;
        assert_eq!(relay.outer_surface_id(b.key()), outer_b);
        b.session.query_surface(&b.surface)?;
        Ok(())
    })
}

#[test]
fn a_slow_routed_input_consumer_cannot_refresh_its_receipt_deadline() -> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture()?;
    let mut a = Producer::new(&inner, 1, false)?;
    let mut b = Producer::new(&inner, 2, false)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    project(&inner, &mut relay)?;
    let frame = hydrate(&inner, &mut relay, &mut host)?;
    assert!(matches!(
        pointer(&mut host, &frame, 10.)?,
        VectorInputResult::Delivered { .. }
    ));
    let saved = drained(&mut relay)?;
    // Exercise the production one-second local receipt deadline.
    std::thread::sleep(Duration::from_millis(1100));
    for input in saved {
        assert!(!input.forward(), "slow consumer revived physical input");
    }
    action(&inner, &mut relay, &b, WindowAction::Focus)?;
    display(&inner, &mut relay, &mut host, 2)?;
    wait(&inner, &mut relay, || {
        b.status().is_ok_and(|status| status.focused)
    })?;
    assert!(!a.status()?.focused);
    Ok(())
}

/// Submit retained bytes without granting the native host a display outcome.
fn forward_unshown(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    producer: &Producer,
    revision: u64,
) -> io::Result<()> {
    let accepted = Instant::now() + Duration::from_secs(3);
    while producer.status()?.accepted_revision != revision {
        assert!(Instant::now() < accepted, "source acceptance stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
    // Intentionally leave physical outcomes queued until recovery. Processing
    // them here would hide unresolved original-scene retention across retirement.
    project(inner, relay)?;
    let source = snapshot(inner, &[1, 2])
        .sources
        .into_iter()
        .find(|source| source.key.producer == producer.session.info().session_id)
        .unwrap();
    for (record_type, bytes) in source.retained_vector {
        relay.media_chunk(MediaChunk {
            delivery_id: 0,
            source: source.key,
            record_type,
            offset: 0,
            total: u32::try_from(bytes.len()).unwrap(),
            last: true,
            bytes: bytes.to_vec(),
        })?;
    }
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(result) = relay.take_media_completions().into_iter().next() {
            assert!(
                result.delivered,
                "fresh source frame {revision} could not forward"
            );
            assert!(!result.superseded);
            return Ok(());
        }
        assert!(Instant::now() < end, "unshown forwarding stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn unshown_frame(
    host: &mut NativeVectorHost,
    relay: &mut OuterBridge,
    surface: u64,
    revision: u64,
) -> io::Result<vivid_sdk::presenter::VectorFrame> {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        relay.retry_pending_activation()?;
        let frame = host.prepare(9, 1.)?;
        if frame.drawings().len() == 2
            && frame.drawings().iter().any(|drawing| {
                drawing.window.surface_id() == surface && drawing.submission.revision == revision
            })
        {
            return Ok(frame);
        }
        assert!(
            Instant::now() < end,
            "fresh physical source frame {revision} stalled: {:?}",
            frame
                .drawings()
                .iter()
                .map(|d| (d.window.surface_id(), d.submission.revision))
                .collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn unresolved_forwarded_scenes_retire_across_channel_advance_and_lease_resume() -> io::Result<()> {
    unresolved_scenes(|_, _| Ok(()))
}

fn unresolved_scenes(
    mut render: impl FnMut(&vivid_sdk::presenter::VectorFrame, [u8; 4]) -> io::Result<()>,
) -> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture_mode(true)?;
    let mut a = Producer::new(&inner, 1, true)?;
    let mut b = Producer::new(&inner, 2, false)?;
    assert_eq!(a.surface.id(), b.surface.id());
    assert_eq!(a.track.id(), b.track.id());
    b.paint(1, 0x0000_ffff)?;
    forward_unshown(&inner, &mut relay, &b, 1)?;
    let peer_outer = relay.outer_surface_id(b.key());
    for revision in 1..=4 {
        a.paint(revision, 0xff00_00ff)?;
        forward_unshown(&inner, &mut relay, &a, revision)?;
    }
    let outer_a = relay.outer_surface_id(a.key()).unwrap();
    let stale_channel = unshown_frame(&mut host, &mut relay, outer_a, 4)?;
    render(&stale_channel, [255, 0, 0, 255])?;
    assert_eq!(a.status()?.scene_revision, 0);
    assert_eq!(b.status()?.scene_revision, 0);
    a.session
        .advance_channel(&a.track, 1, &RequestMetadata::default())?;
    let next_channel = a.session.open_track_channel(&a.track)?;
    let old_channel = std::mem::replace(&mut a.channel, next_channel);
    let _ = old_channel.close();
    project(&inner, &mut relay)?;
    for revision in 5..=8 {
        a.paint(revision, 0x00ff_00ff)?;
        forward_unshown(&inner, &mut relay, &a, revision)?;
    }
    let fresh_channel = unshown_frame(&mut host, &mut relay, outer_a, 8)?;
    render(&fresh_channel, [0, 255, 0, 255])?;
    drop(fresh_channel);
    // Old pixels can still be rendered, but cannot acknowledge a retired writer.
    render(&stale_channel, [255, 0, 0, 255])?;
    assert!(!host.presented(&stale_channel)?);
    drop(stale_channel);
    let stale_resume = unshown_frame(&mut host, &mut relay, outer_a, 8)?;
    let owner = a.session.info().session_id;
    let generation = a.session.info().resume_generation;
    let authentication = a.session.resume_authentication()?;
    (a.session.cancel_handle())();
    wait(&inner, &mut relay, || {
        inner
            .desktop_session_generation(SourceKey {
                producer: owner,
                context: a.surface.context_id(),
                surface: a.surface.id(),
                track: a.track.id(),
            })
            .is_none()
    })?;
    let mut resumed = Session::connect(config(&inner, authentication))?;
    assert_eq!(resumed.info().session_id, owner);
    assert_eq!(resumed.info().resume_generation, generation + 1);
    resumed.adopt_surface(&a.surface)?;
    resumed.adopt_track(&a.track)?;
    resumed.advance_channel(&a.track, 1, &RequestMetadata::default())?;
    let channel = resumed.open_track_channel(&a.track)?;
    let lane = resumed.open_overlay_input_lane(2)?;
    lane.renew(5_000_000, Duration::from_secs(1))?;
    a.session = resumed;
    a.channel = channel;
    a.lane = lane;
    project(&inner, &mut relay)?;
    assert_eq!(relay.outer_surface_id(b.key()), peer_outer);
    for revision in 9..=12 {
        a.paint(revision, 0xffff_00ff)?;
        forward_unshown(&inner, &mut relay, &a, revision)?;
    }
    let resumed_outer = relay.outer_surface_id(a.key()).unwrap();
    assert_ne!(
        resumed_outer, outer_a,
        "resume reused a physical window identity"
    );
    let fresh_resume = unshown_frame(&mut host, &mut relay, resumed_outer, 4)?;
    render(&fresh_resume, [255, 255, 0, 255])?;
    render(&stale_resume, [0, 255, 0, 255])?;
    assert!(!host.presented(&stale_resume)?);
    drop(stale_resume);
    // Readback alone must not settle either source, including the same-ID peer.
    assert_eq!(a.status()?.scene_revision, 0);
    assert_eq!(b.status()?.scene_revision, 0);
    render(&fresh_resume, [255, 255, 0, 255])?;
    assert!(host.presented(&fresh_resume)?);
    wait(&inner, &mut relay, || {
        a.status().is_ok_and(|s| s.scene_revision == 12)
            && b.status().is_ok_and(|s| s.scene_revision == 1)
    })?;
    assert!(relay.take_source_losses().is_empty());
    assert_eq!(a.status()?.accepted_revision, 12);
    assert_eq!(b.status()?.accepted_revision, 1);
    Ok(())
}
