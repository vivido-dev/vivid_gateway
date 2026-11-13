//! Native window actions preserve original ownership and pane-local geometry.

// Rust guideline compliant 2026-10-07
use super::focus::{input, producer, request};
use super::*;
use std::sync::Arc;
use vivid_protocol::vector::Scalar;
use vivid_sdk::presenter::{BridgeSurfaceKey, NativeVectorHost, OverlayHostRequest};

const OFFSET: (i64, i64) = (47, 29);

fn fixture() -> io::Result<(
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
    outer.update_metrics(9, 100, 30, CELL);
    let relay = OuterBridge::builder(
        Secret32::from_hex(&secret).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .control_endpoint(endpoint)
    .overlay_action_forwarding(&inner)
    .build()?;
    Ok((inner, outer, host, relay))
}

fn project(inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    let mut projection = snapshot(inner, &[1, 2]).bridge_projection();
    for surface in &mut projection.surfaces {
        if let Some(window) = &mut surface.overlay_window {
            window.offset_x = OFFSET.0;
            window.offset_y = OFFSET.1;
            window.x += OFFSET.0;
            window.y += OFFSET.1;
        }
    }
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    Ok(())
}
fn display(
    host: &mut NativeVectorHost,
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    count: usize,
) -> io::Result<Vec<u64>> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        relay.retry_pending_activation()?;
        input(inner, relay)?;
        let frame = host.prepare(9, 1.)?;
        if host.presented(&frame)? && frame.drawings().len() == count {
            return Ok(frame
                .drawings()
                .iter()
                .map(|drawing| drawing.window.surface_id())
                .collect());
        }
        assert!(Instant::now() < deadline, "native action frame stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn hydrate(
    host: &mut NativeVectorHost,
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    count: usize,
) -> io::Result<()> {
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
    display(host, inner, relay, count)?;
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut completed = 0;
    loop {
        for result in relay.take_media_completions() {
            assert!(result.delivered);
            completed += 1;
        }
        if completed == count {
            break;
        }
        assert!(Instant::now() < deadline, "native action hydration stalled");
        display(host, inner, relay, count)?;
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}
fn action(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    window: &OverlayWindow,
    operation: fn(&OverlayWindow) -> io::Result<()>,
) -> io::Result<()> {
    std::thread::scope(|scope| {
        let job = scope.spawn(|| operation(window));
        let request = request(inner)?;
        project(inner, relay)?;
        relay.start_overlay_host_request(request)?;
        let deadline = Instant::now() + Duration::from_secs(3);
        while !job.is_finished() {
            input(inner, relay)?;
            for (id, result) in relay.take_overlay_host_replies() {
                inner.complete_overlay_host_request(id, result.map_err(|error| error.to_string()));
            }
            assert!(Instant::now() < deadline, "window action RPC stalled");
            std::thread::sleep(Duration::from_millis(1));
        }
        input(inner, relay)?;
        job.join().unwrap()
    })
}
fn keys(inner: &VirtualVivid) -> (BridgeSurfaceKey, BridgeSurfaceKey, BridgeSurfaceKey) {
    let surfaces = snapshot(inner, &[1, 2]).surfaces;
    let key = |x, child| {
        let surface = surfaces
            .iter()
            .find(|surface| {
                surface
                    .overlay_window
                    .is_some_and(|window| window.x == x && window.parent.is_some() == child)
            })
            .unwrap();
        BridgeSurfaceKey {
            producer: surface.producer,
            context: surface.context,
            surface: surface.surface,
        }
    };
    (key(0, false), key(260, false), key(480, true))
}
fn dismissal_count(session: &OverlaySession) -> io::Result<usize> {
    let end = Instant::now() + Duration::from_millis(100);
    let mut count = 0;
    while Instant::now() < end {
        if let Some(vivid_sdk::OverlayLaneEvent::Input(event)) =
            session.wait_event(Duration::from_millis(2))?
            && matches!(event.event, vivid_protocol::overlay::Event::Dismissed(_))
        {
            count += 1;
        }
    }
    Ok(count)
}

#[test]
fn stacking_and_translated_center_keep_peer_focus_and_close_only_the_parent_group() -> io::Result<()>
{
    let (inner, _outer, mut host, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    let child = second.create_child(
        &b,
        OverlayWindowOptions::new(Rect::new(480., 20., 70., 50.).unwrap(), WindowMode::Popup),
    )?;
    for window in [&a, &b, &child] {
        window.present(scene(0xff22_4488))?;
    }
    project(&inner, &mut relay)?;
    hydrate(&mut host, &inner, &mut relay, 3)?;
    let (ka, kb, kc) = keys(&inner);
    assert_eq!((ka.context, ka.surface), (kb.context, kb.surface));
    let (oa, ob, oc) = (
        relay.outer_surface_id(ka).unwrap(),
        relay.outer_surface_id(kb).unwrap(),
        relay.outer_surface_id(kc).unwrap(),
    );
    action(&inner, &mut relay, &a, OverlayWindow::request_focus)?;
    assert_eq!(display(&mut host, &inner, &mut relay, 3)?, vec![ob, oc, oa]);
    action(&inner, &mut relay, &b, OverlayWindow::raise)?;
    assert_eq!(display(&mut host, &inner, &mut relay, 3)?, vec![oa, ob, oc]);
    assert!(a.reconcile()?.focused);
    action(&inner, &mut relay, &b, OverlayWindow::lower)?;
    assert_eq!(display(&mut host, &inner, &mut relay, 3)?, vec![ob, oc, oa]);
    assert!(a.reconcile()?.focused);
    action(&inner, &mut relay, &b, OverlayWindow::center)?;
    assert_eq!(b.bounds()?, Rect::new(200., 132., 240., 120.).unwrap());
    let frame = host.prepare(9, 1.)?;
    let centered = frame
        .drawings()
        .iter()
        .find(|drawing| drawing.window.surface_id() == ob)
        .unwrap();
    assert_eq!(centered.bounds.origin.x, Scalar::new(247.).unwrap());
    assert_eq!(centered.bounds.origin.y, Scalar::new(161.).unwrap());
    assert!(a.reconcile()?.focused);
    while second.wait_event(Duration::ZERO)?.is_some() {}
    action(&inner, &mut relay, &b, OverlayWindow::request_close)?;
    assert_eq!(display(&mut host, &inner, &mut relay, 1)?, vec![oa]);
    assert!(a.reconcile()?.focused);
    b.submit(scene(1)).unwrap_err();
    child.submit(scene(1)).unwrap_err();
    assert_eq!(
        dismissal_count(&second)?,
        2,
        "one dismissal per parent/child"
    );
    child.close()?;
    b.close()?;
    project(&inner, &mut relay)?;
    assert_eq!(display(&mut host, &inner, &mut relay, 1)?, vec![oa]);
    Ok(())
}

#[test]
fn close_acknowledgement_and_physical_dismissal_can_arrive_in_either_order() -> io::Result<()> {
    for input_first in [false, true] {
        let (inner, _outer, mut host, mut relay) = fixture()?;
        let first = producer(&inner, 1)?;
        let second = producer(&inner, 2)?;
        let a = window_of(&first, 0., 0.)?;
        let b = window_of(&second, 260., 0.)?;
        let child = second.create_child(
            &b,
            OverlayWindowOptions::new(Rect::new(480., 20., 70., 50.).unwrap(), WindowMode::Popup),
        )?;
        for window in [&a, &b, &child] {
            window.present(scene(1))?;
        }
        project(&inner, &mut relay)?;
        hydrate(&mut host, &inner, &mut relay, 3)?;
        while second.wait_event(Duration::ZERO)?.is_some() {}
        std::thread::scope(|scope| -> io::Result<()> {
            let job = scope.spawn(|| b.request_close());
            let pending = request(&inner)?;
            let reply = relay.overlay_host_request(&pending)?;
            if input_first {
                let end = Instant::now() + Duration::from_secs(3);
                while snapshot(&inner, &[2])
                    .surfaces
                    .iter()
                    .any(|surface| surface.overlay_window.is_some())
                {
                    input(&inner, &mut relay)?;
                    assert!(
                        Instant::now() < end,
                        "physical close did not retire original windows"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            inner.complete_overlay_host_request(pending.id, Ok(reply));
            job.join().unwrap()?;
            input(&inner, &mut relay)?;
            Ok(())
        })?;
        assert_eq!(dismissal_count(&second)?, 2);
        assert_eq!(display(&mut host, &inner, &mut relay, 1)?.len(), 1);
    }
    Ok(())
}

#[test]
fn a_queued_center_refuses_a_changed_original_viewport_and_a_peer_retarget() -> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    for window in [&a, &b] {
        window.present(scene(1))?;
    }
    project(&inner, &mut relay)?;
    hydrate(&mut host, &inner, &mut relay, 2)?;
    let peer = snapshot(&inner, &[1]).surfaces[0].producer;
    std::thread::scope(|scope| -> io::Result<()> {
        let job = scope.spawn(|| b.center());
        let pending = request(&inner)?;
        let mut tampered: OverlayHostRequest = pending.clone();
        tampered.surface.producer = peer;
        relay.overlay_host_request(&tampered).unwrap_err();
        inner.update_metrics(2, 70, ROWS, CELL);
        let result = relay.overlay_host_request(&pending);
        assert!(
            result.is_err(),
            "queued center must retain its original viewport"
        );
        inner.complete_overlay_host_request(pending.id, result.map_err(|error| error.to_string()));
        assert!(job.join().unwrap().is_err());
        Ok(())
    })?;
    assert_eq!(b.bounds()?, Rect::new(260., 0., 240., 120.).unwrap());
    action(&inner, &mut relay, &b, OverlayWindow::center)?;
    assert_eq!(b.bounds()?, Rect::new(160., 132., 240., 120.).unwrap());
    assert_eq!(a.bounds()?, Rect::new(0., 0., 240., 120.).unwrap());
    Ok(())
}

#[test]
fn acknowledged_focus_revision_does_not_republish_identical_geometry_or_revoke_first_key()
-> io::Result<()> {
    use vivid_sdk::presenter::{VectorInputEvent, VectorInputResult};
    let (inner, outer, mut host, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    a.present(scene(1))?;
    b.present(scene(2))?;
    project(&inner, &mut relay)?;
    hydrate(&mut host, &inner, &mut relay, 2)?;
    action(&inner, &mut relay, &a, OverlayWindow::request_focus)?;
    display(&mut host, &inner, &mut relay, 2)?;
    let frame = host.prepare(9, 1.)?;
    assert!(host.presented(&frame)?);
    let target = host.input_target(&frame).unwrap();
    let before = snapshot(&outer, &[9]);
    project(&inner, &mut relay)?;
    let after = snapshot(&outer, &[9]);
    let key = host.dispatch_input(
        &target,
        VectorInputEvent::Key {
            usage: 21,
            down: true,
            repeat: false,
            modifiers: 0,
            text: None,
        },
    )?;
    assert_eq!(
        overlay_windows(&before),
        overlay_windows(&after),
        "acknowledgement revision republished persistent geometry"
    );
    assert_eq!(key, VectorInputResult::Delivered { text: false });
    assert!(
        frame.same_content(&host.prepare(9, 1.)?),
        "revision-only reconciliation invalidated displayed frame"
    );
    assert_eq!(
        host.dispatch_input(
            &target,
            VectorInputEvent::Key {
                usage: 21,
                down: false,
                repeat: false,
                modifiers: 0,
                text: None,
            }
        )?,
        VectorInputResult::Delivered { text: false }
    );
    let mut pressed = 0;
    let mut released = 0;
    let end = Instant::now() + Duration::from_secs(3);
    while released == 0 {
        input(&inner, &mut relay)?;
        if let Some(vivid_sdk::OverlayLaneEvent::Input(event)) =
            first.wait_event(Duration::from_millis(2))?
            && let vivid_protocol::overlay::Event::Key {
                physical: 21, down, ..
            } = event.event
        {
            if down {
                pressed += 1;
            } else {
                released += 1;
            }
        }
        assert!(Instant::now() < end, "first key feedback stalled");
    }
    assert_eq!(pressed, 1, "first physical key must arrive exactly once");
    assert_eq!(released, 1, "first physical key must release exactly once");
    while let Some(event) = second.wait_event(Duration::ZERO)? {
        assert!(
            !matches!(
                event,
                vivid_sdk::OverlayLaneEvent::Input(vivid_protocol::overlay::wire::InputEvent {
                    event: vivid_protocol::overlay::Event::Key { .. },
                    ..
                })
            ),
            "first key crossed owners"
        );
    }
    a.set_bounds(Rect::new(8., 8., 240., 120.).unwrap())?;
    project(&inner, &mut relay)?;
    assert!(
        !frame.same_content(&host.prepare(9, 1.)?),
        "real geometry change lost its display boundary"
    );
    assert_eq!(
        host.dispatch_input(
            &target,
            VectorInputEvent::Key {
                usage: 4,
                down: true,
                repeat: false,
                modifiers: 0,
                text: None,
            }
        )?,
        VectorInputResult::Rejected,
        "old image gained input after real geometry changed"
    );
    display(&mut host, &inner, &mut relay, 2)?;
    action(&inner, &mut relay, &a, OverlayWindow::center)?;
    display(&mut host, &inner, &mut relay, 2)?;
    let centered = host.prepare(9, 1.)?;
    assert!(host.presented(&centered)?);
    let current = host.input_target(&centered).unwrap();
    let before_center = overlay_windows(&snapshot(&outer, &[9]));
    project(&inner, &mut relay)?;
    assert_eq!(
        before_center,
        overlay_windows(&snapshot(&outer, &[9])),
        "acknowledged Center republished its already-applied bounds"
    );
    assert_eq!(
        host.dispatch_input(
            &current,
            VectorInputEvent::Key {
                usage: 22,
                down: true,
                repeat: false,
                modifiers: 0,
                text: None,
            }
        )?,
        VectorInputResult::Delivered { text: false }
    );
    assert_eq!(
        host.dispatch_input(
            &current,
            VectorInputEvent::Key {
                usage: 22,
                down: false,
                repeat: false,
                modifiers: 0,
                text: None,
            }
        )?,
        VectorInputResult::Delivered { text: false }
    );
    assert!(a.reconcile()?.focused);
    assert!(!b.reconcile()?.focused);
    Ok(())
}

#[test]
fn focus_waits_for_preceding_owner_popup_but_not_for_a_same_id_peer() -> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let peer = window_of(&second, 260., 0.)?;
    let child = first.create_child(
        &a,
        OverlayWindowOptions::new(Rect::new(160., 40., 48., 32.).unwrap(), WindowMode::Popup),
    )?;
    a.present(scene(1))?;
    peer.present(scene(2))?;
    let full = snapshot(&inner, &[1, 2]).bridge_projection();
    let owner = snapshot(&inner, &[1]).surfaces[0].producer;
    let mut partial = full.clone();
    let child_key = full
        .surfaces
        .iter()
        .find(|surface| {
            surface.key.producer == owner
                && surface
                    .overlay_window
                    .is_some_and(|window| window.parent.is_some())
        })
        .unwrap()
        .key;
    let is_child = |key: BridgeSurfaceKey| key == child_key;
    partial.surfaces.retain(|surface| !is_child(surface.key));
    partial.sources.retain(|source| {
        !is_child(BridgeSurfaceKey {
            producer: source.key.producer,
            context: source.key.context,
            surface: source.key.surface,
        })
    });
    partial.nodes.retain(|node| !is_child(node.surface));
    relay.rebuild(&partial.surfaces, &partial.sources, &partial.nodes)?;
    std::thread::scope(|scope| -> io::Result<()> {
        let focused = scope.spawn(|| a.request_focus());
        let request = super::focus::request(&inner)?;
        let overtook = relay.can_service_overlay_host_request(&request);
        if overtook {
            inner.complete_overlay_host_request(
                request.id,
                Err("missing owner projection barrier".into()),
            );
        }
        assert!(
            !overtook,
            "focus overtook a popup created earlier by the original owner"
        );
        assert_eq!(
            relay
                .start_overlay_host_request(request.clone())
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let peer_job = scope.spawn(|| peer.request_focus());
        let peer_request = super::focus::request(&inner)?;
        assert!(
            relay.can_service_overlay_host_request(&peer_request),
            "owner barrier blocked an independent same-ID peer"
        );
        let response = relay.overlay_host_request(&peer_request)?;
        inner.complete_overlay_host_request(peer_request.id, Ok(response));
        peer_job.join().unwrap()?;
        relay.rebuild(&full.surfaces, &full.sources, &full.nodes)?;
        assert!(relay.can_service_overlay_host_request(&request));
        let response = relay.overlay_host_request(&request)?;
        inner.complete_overlay_host_request(request.id, Ok(response));
        focused.join().unwrap()?;
        let end = Instant::now() + Duration::from_secs(3);
        loop {
            input(&inner, &mut relay)?;
            if a.reconcile()?.focused {
                break;
            }
            assert!(Instant::now() < end, "ordered focus feedback stalled");
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(!child.reconcile()?.focused);
        assert!(!peer.reconcile()?.focused);
        // The role barrier does not wait for popup media or GPU presentation.
        assert_eq!(display(&mut host, &inner, &mut relay, 0)?.len(), 0);
        Ok(())
    })
}

#[test]
fn static_vector_waits_for_its_original_window_role_without_blocking_a_peer() -> io::Result<()> {
    let (inner, _outer, mut host, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let parent = window_of(&first, 0., 0.)?;
    let peer = window_of(&second, 260., 0.)?;
    let child = first.create_child(
        &parent,
        OverlayWindowOptions::new(Rect::new(160., 40., 48., 32.).unwrap(), WindowMode::Popup),
    )?;
    child.present(scene(1))?;
    peer.present(scene(2))?;
    let snapshot = snapshot(&inner, &[1, 2]);
    let full = snapshot.bridge_projection();
    let child_key = full
        .surfaces
        .iter()
        .find(|surface| {
            surface
                .overlay_window
                .is_some_and(|window| window.parent.is_some())
        })
        .unwrap()
        .key;
    let mut partial = full.clone();
    partial
        .surfaces
        .iter_mut()
        .find(|surface| surface.key == child_key)
        .unwrap()
        .overlay_window = None;
    relay.rebuild(&partial.surfaces, &partial.sources, &partial.nodes)?;
    let source = snapshot
        .sources
        .iter()
        .find(|source| {
            source.key.producer == child_key.producer
                && source.key.context == child_key.context
                && source.key.surface == child_key.surface
        })
        .unwrap();
    assert!(relay.outer_track_id(source.key).is_some());
    assert!(
        !relay.can_accept_media(source.key),
        "static frame overtook its original window role"
    );
    let (record_type, bytes) = &source.retained_vector[0];
    let chunk = || MediaChunk {
        delivery_id: 0,
        source: source.key,
        record_type: *record_type,
        offset: 0,
        total: u32::try_from(bytes.len()).unwrap(),
        last: true,
        bytes: bytes.to_vec(),
    };
    assert_eq!(
        relay.media_chunk(chunk()).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let peer_source = snapshot
        .sources
        .iter()
        .find(|source| source.key.producer != child_key.producer)
        .unwrap();
    assert!(
        relay.can_accept_media(peer_source.key),
        "role barrier blocked a same-ID peer"
    );
    for (record_type, bytes) in &peer_source.retained_vector {
        relay.media_chunk(MediaChunk {
            delivery_id: 0,
            source: peer_source.key,
            record_type: *record_type,
            offset: 0,
            total: u32::try_from(bytes.len()).unwrap(),
            last: true,
            bytes: bytes.to_vec(),
        })?;
    }
    display(&mut host, &inner, &mut relay, 1)?;
    relay.rebuild(&full.surfaces, &full.sources, &full.nodes)?;
    assert!(relay.can_accept_media(source.key));
    relay.media_chunk(chunk())?;
    display(&mut host, &inner, &mut relay, 2)?;
    Ok(())
}
