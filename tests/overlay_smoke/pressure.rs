//! Real-socket saturation and deadlines across original and physical host queues.

// Rust guideline compliant 2026-10-07
use super::focus::input;
use super::*;
use std::sync::Arc;
use vivid_protocol::{
    messages,
    overlay::wire::{Action, Query, WindowAction, WindowAddress},
};
use vivid_sdk::overlay::{OverlayHostHandle, Text};
use vivid_sdk::presenter::OverlayHostRequest;
type Fixture = (Arc<VirtualVivid>, VirtualVivid, OuterBridge);
fn fixture() -> io::Result<Fixture> {
    let (inner, _) = overlay_presenter()?;
    let inner = Arc::new(inner);
    inner.enable_overlay_host_relay();
    let (outer, endpoint) = overlay_presenter()?;
    outer.enable_overlay_host_relay();
    let relay = OuterBridge::builder(
        Secret32::from_hex(&pane(&outer, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .control_endpoint(endpoint)
    .overlay_action_forwarding(&inner)
    .build()?;
    Ok((inner, outer, relay))
}
fn producer(inner: &VirtualVivid, id: u64) -> io::Result<(OverlaySession, OverlayHostHandle)> {
    let mut config = ProducerConfig {
        authentication: ProducerAuthentication::root_hex(&pane(inner, id)?)
            .map_err(io::Error::other)?,
        required_profiles: [
            vivid_protocol::registry::CORE_CONTROL,
            vivid_protocol::registry::LIVE_MEDIA,
            vivid_protocol::registry::TERMINAL_SURFACE,
            vivid_protocol::registry::TERMINAL_OVERLAY,
            vivid_protocol::registry::VECTOR_SCENE,
            vivid_protocol::registry::OVERLAY_INPUT,
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        optional_profiles: vec![vivid_protocol::registry::OVERLAY_TEXT.into()],
        ..ProducerConfig::default()
    };
    config.required_profiles.sort();
    config.required_profiles.dedup();
    pin_endpoints(&mut config, inner.endpoint());
    let session = Session::connect(config)?;
    let handle = session.overlay_host_handle().unwrap();
    Ok((OverlaySession::from_session(session)?, handle))
}
fn address(inner: &VirtualVivid, pane: u64) -> WindowAddress {
    let surface = &snapshot(inner, &[pane]).surfaces[0];
    WindowAddress {
        context_id: surface.context,
        surface_id: surface.surface,
        generation: 1,
    }
}
fn project(inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    let p = snapshot(inner, &[1, 2]).bridge_projection();
    relay.rebuild(&p.surfaces, &p.sources, &p.nodes)?;
    Ok(())
}
fn pending(presenter: &VirtualVivid, count: usize) -> Vec<OverlayHostRequest> {
    let end = Instant::now() + Duration::from_secs(3);
    let mut requests = Vec::new();
    while requests.len() < count {
        requests.extend(presenter.take_overlay_host_requests());
        assert!(Instant::now() < end, "host requests did not arrive");
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(requests.len(), count);
    requests
}
fn pump(inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    input(inner, relay)?;
    for (id, result) in relay.take_overlay_host_replies() {
        inner.complete_overlay_host_request(id, result.map_err(|error| error.to_string()));
    }
    Ok(())
}
fn wait(inner: &VirtualVivid, relay: &mut OuterBridge, done: impl Fn() -> bool) -> io::Result<()> {
    // Production action deadlines are ten seconds. Keep the real duration here;
    // the extra four seconds bound scheduling and teardown on a loaded host.
    let end = Instant::now() + Duration::from_secs(14);
    while !done() {
        pump(inner, relay)?;
        assert!(Instant::now() < end, "bounded host work did not finish");
        std::thread::sleep(Duration::from_millis(2));
    }
    pump(inner, relay)
}
fn peer_action(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    window: &OverlayWindow,
    action: fn(&OverlayWindow) -> io::Result<()>,
) -> io::Result<()> {
    std::thread::scope(|scope| {
        let job = scope.spawn(|| action(window));
        let request = pending(inner, 1).pop().unwrap();
        project(inner, relay)?;
        assert!(relay.can_service_overlay_host_request(&request));
        let start = Instant::now();
        relay.start_overlay_host_request(request)?;
        wait(inner, relay, || job.is_finished())?;
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "peer action blocked by slow owner"
        );
        job.join().unwrap()
    })
}
fn query(handle: &OverlayHostHandle, address: WindowAddress) -> io::Result<()> {
    let start = Instant::now();
    handle(
        messages::QUERY_OVERLAY,
        address.surface_id,
        Query {
            context_id: address.context_id,
            surface_id: address.surface_id,
        }
        .payload()?,
    )?;
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "host work blocked its control reader"
    );
    Ok(())
}

#[test]
fn saturated_actions_leave_control_and_same_id_peer_live_then_expire_without_dispatch()
-> io::Result<()> {
    let (inner, _outer, mut relay) = fixture()?;
    let (first, handle) = producer(&inner, 1)?;
    let (second, _) = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    let address = address(&inner, 1);
    assert_eq!(address.surface_id, self::address(&inner, 2).surface_id);
    project(&inner, &mut relay)?;
    std::thread::scope(|scope| -> io::Result<()> {
        let mut jobs = Vec::new();
        let started = Instant::now();
        for action in [
            WindowAction::Focus,
            WindowAction::Raise,
            WindowAction::Lower,
            WindowAction::Center,
            WindowAction::Close,
            WindowAction::Raise,
            WindowAction::Center,
            WindowAction::Close,
        ] {
            let handle = Arc::clone(&handle);
            jobs.push(scope.spawn(move || {
                handle(
                    messages::OVERLAY_ACTION,
                    address.surface_id,
                    Action {
                        address,
                        expected_revision: 1,
                        action,
                    }
                    .payload()?,
                )
            }));
        }
        let requests = pending(&inner, 8);
        // The ninth action fails promptly at the original owner's eight-job limit.
        let start = Instant::now();
        handle(
            messages::OVERLAY_ACTION,
            address.surface_id,
            Action {
                address,
                expected_revision: 1,
                action: WindowAction::Close,
            }
            .payload()?,
        )
        .unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(2));
        query(&handle, address)?;
        peer_action(&inner, &mut relay, &b, OverlayWindow::center)?;
        wait(&inner, &mut relay, || {
            jobs.iter().all(std::thread::ScopedJoinHandle::is_finished)
        })?;
        assert!(
            started.elapsed() >= Duration::from_secs(10),
            "production deadline was shortened"
        );
        for job in jobs {
            job.join().unwrap().unwrap_err();
        }
        assert_eq!(a.bounds()?, Rect::new(0., 0., 240., 120.).unwrap());
        assert_eq!(a.reconcile()?.window_revision, 1);
        // Late completion cannot resurrect a dismissed admission or mutate state.
        for request in &requests {
            inner.complete_overlay_host_request(request.id, Ok(messages::ok(0)));
        }
        a.close()?;
        project(&inner, &mut relay)?;
        for request in requests {
            assert!(
                relay.can_service_overlay_host_request(&request),
                "expired action stranded behind absent projection"
            );
            let start = Instant::now();
            assert!(relay.start_overlay_host_request(request).is_err());
            assert!(start.elapsed() < Duration::from_secs(1));
        }
        peer_action(&inner, &mut relay, &b, OverlayWindow::request_focus)?;
        assert!(b.reconcile()?.focused);
        Ok(())
    })
}

#[test]
fn a_late_claimed_action_reply_cannot_advance_the_original_window() -> io::Result<()> {
    let (inner, outer, mut relay) = fixture()?;
    let (first, _) = producer(&inner, 1)?;
    let (second, _) = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    project(&inner, &mut relay)?;
    std::thread::scope(|scope| -> io::Result<()> {
        let job = scope.spawn(|| a.center());
        let request = pending(&inner, 1).pop().unwrap();
        // The physical result is real, but its return to the original host is stalled.
        let reply = relay.overlay_host_request(&request)?;
        assert!(
            snapshot(&outer, &[9])
                .surfaces
                .iter()
                .any(|s| s.overlay_window.is_some_and(|w| w.x == 200))
        );
        peer_action(&inner, &mut relay, &b, OverlayWindow::request_focus)?;
        wait(&inner, &mut relay, || job.is_finished())?;
        job.join().unwrap().unwrap_err();
        inner.complete_overlay_host_request(request.id, Ok(reply));
        assert_eq!(a.bounds()?, Rect::new(0., 0., 240., 120.).unwrap());
        assert_eq!(a.reconcile()?.window_revision, 1);
        assert!(b.reconcile()?.focused);
        // Only a new explicit request may reconcile independent physical revisions.
        peer_action(&inner, &mut relay, &a, OverlayWindow::center)?;
        assert_eq!(a.bounds()?, Rect::new(200., 132., 240., 120.).unwrap());
        Ok(())
    })
}

#[test]
fn slow_font_jobs_are_bounded_per_origin_while_peer_actions_media_and_revocation_progress()
-> io::Result<()> {
    use vivid_protocol::overlay::wire::text::MeasureText;
    use vivid_protocol::vector::{Point, Scalar};
    let (inner, outer, mut relay) = fixture()?;
    let (first, handle) = producer(&inner, 1)?;
    let (second, _) = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    let address = address(&inner, 1);
    project(&inner, &mut relay)?;
    let text = Text {
        text: "slow font service".into(),
        origin: Point::default(),
        size: Scalar::new(16.).unwrap(),
        family: String::new(),
        weight: 400,
        italic: false,
        color: Color(0xffff_ffff),
        max_width: None,
    };
    std::thread::scope(|scope| -> io::Result<()> {
        let mut jobs = Vec::new();
        for _ in 0..8 {
            let handle = Arc::clone(&handle);
            let payload = MeasureText {
                address,
                text: text.clone(),
            }
            .payload()?;
            jobs.push(scope.spawn(move || {
                handle(messages::MEASURE_OVERLAY_TEXT, address.surface_id, payload)
            }));
        }
        let requests = pending(&inner, 8);
        for request in requests.iter().take(4) {
            relay.start_overlay_host_request(request.clone())?;
        }
        let physical = pending(&outer, 4);
        assert!(!relay.can_service_overlay_host_request(&requests[4]));
        assert_eq!(
            relay
                .start_overlay_host_request(requests[4].clone())
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        query(&handle, address)?;
        peer_action(&inner, &mut relay, &b, OverlayWindow::request_focus)?;
        assert!(b.reconcile()?.focused);
        // A font callback cannot hold up independent retained media and its receipt.
        b.present(scene(0x55cc_22ff))?;
        project(&inner, &mut relay)?;
        let source = snapshot(&inner, &[2]).sources.remove(0);
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
            relay.retry_pending_activation()?;
            pump(&inner, &mut relay)?;
            let completions = relay.take_media_completions();
            if !completions.is_empty() {
                assert!(completions.iter().all(|r| r.delivered));
                break;
            }
            assert!(Instant::now() < end, "font callbacks blocked peer media");
            std::thread::sleep(Duration::from_millis(2));
        }
        outer.set_overlay_pane_focus(9, false);
        let end = Instant::now() + Duration::from_secs(2);
        while b.reconcile()?.focused {
            pump(&inner, &mut relay)?;
            assert!(
                Instant::now() < end,
                "font callbacks blocked input revocation"
            );
        }
        outer.set_overlay_pane_focus(9, true);
        for request in physical {
            outer
                .complete_overlay_host_request(request.id, Err("slow font fixture refused".into()));
        }
        // Reap the first batch, then prove queued capacity is usable again.
        wait(&inner, &mut relay, || {
            jobs.iter().filter(|job| job.is_finished()).count() >= 4
        })?;
        pump(&inner, &mut relay)?;
        for request in requests.into_iter().skip(4) {
            assert!(relay.can_service_overlay_host_request(&request));
            relay.start_overlay_host_request(request)?;
        }
        for request in pending(&outer, 4) {
            outer
                .complete_overlay_host_request(request.id, Err("slow font fixture refused".into()));
        }
        wait(&inner, &mut relay, || {
            jobs.iter().all(std::thread::ScopedJoinHandle::is_finished)
        })?;
        for job in jobs {
            job.join().unwrap().unwrap_err();
        }
        assert_eq!(a.bounds()?, Rect::new(0., 0., 240., 120.).unwrap());
        peer_action(&inner, &mut relay, &b, OverlayWindow::center)?;
        Ok(())
    })
}
