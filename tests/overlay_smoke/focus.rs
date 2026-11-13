//! Explicit producer focus through independently authenticated, owner-qualified hops.

// Rust guideline compliant 2026-10-07
use super::*;
use std::sync::Arc;
use vivid_protocol::messages;
use vivid_sdk::presenter::OverlayHostRequest;

pub(super) fn fixture() -> io::Result<(Arc<VirtualVivid>, VirtualVivid, OuterBridge)> {
    let (inner, _) = overlay_presenter()?;
    let inner = Arc::new(inner);
    inner.enable_overlay_host_relay();
    let (outer, endpoint) = overlay_presenter()?;
    let relay = OuterBridge::builder(
        Secret32::from_hex(&pane(&outer, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .control_endpoint(endpoint)
    .overlay_action_forwarding(&inner)
    .build()?;
    Ok((inner, outer, relay))
}
pub(super) fn producer(inner: &VirtualVivid, id: u64) -> io::Result<OverlaySession> {
    overlay_producer(&inner.endpoint(), &pane(inner, id)?, "focus-owner")
}
fn project(inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    let projection = snapshot(inner, &[1, 2]).bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    Ok(())
}
pub(super) fn input(_inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    for input in relay.take_overlay_routed_input()? {
        input.forward();
    }
    Ok(())
}
pub(super) fn request(inner: &VirtualVivid) -> io::Result<OverlayHostRequest> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(request) = inner.take_overlay_host_requests().into_iter().next() {
            assert_eq!(request.record_type, messages::OVERLAY_ACTION);
            return Ok(request);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("focus request did not reach relay"));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn focus(inner: &VirtualVivid, relay: &mut OuterBridge, window: &OverlayWindow) -> io::Result<()> {
    std::thread::scope(|scope| {
        let job = scope.spawn(|| window.request_focus());
        let request = request(inner)?;
        project(inner, relay)?;
        assert!(relay.can_service_overlay_host_request(&request));
        relay.start_overlay_host_request(request)?;
        let deadline = Instant::now() + Duration::from_secs(3);
        while !job.is_finished() {
            input(inner, relay)?;
            for (id, result) in relay.take_overlay_host_replies() {
                if let Err(error) = &result {
                    eprintln!("focus relay refusal: {error}");
                }
                inner.complete_overlay_host_request(id, result.map_err(|error| error.to_string()));
            }
            assert!(Instant::now() < deadline, "focus RPC stalled");
            std::thread::sleep(Duration::from_millis(1));
        }
        input(inner, relay)?;
        job.join().unwrap()
    })
}
fn wait(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    ready: impl Fn() -> io::Result<bool>,
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        input(inner, relay)?;
        if ready()? {
            return Ok(());
        }
        assert!(Instant::now() < deadline, "focus feedback stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn focus_remaps_window_revisions_and_remembers_an_inactive_pane_without_activating_peers()
-> io::Result<()> {
    let (inner, outer, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    for x in [2., 4.] {
        a.set_bounds(Rect::new(x, 0., 240., 120.).unwrap())?;
    }
    assert_eq!(a.reconcile()?.window_revision, 3);
    let local = snapshot(&inner, &[1, 2]);
    assert_eq!(local.surfaces[0].context, local.surfaces[1].context);
    assert_eq!(local.surfaces[0].surface, local.surfaces[1].surface);
    assert_ne!(local.surfaces[0].producer, local.surfaces[1].producer);
    project(&inner, &mut relay)?;
    // Both physical windows were independently created at revision 1.
    assert!(
        snapshot(&outer, &[9]).surfaces.iter().all(|surface| surface
            .overlay_window
            .unwrap()
            .revision
            == 1)
    );
    let neighbor = overlay_producer(&outer.endpoint(), &pane(&outer, 10)?, "physical-peer")?;
    let neighbor_window = window_of(&neighbor, 0., 0.)?;
    neighbor_window.request_focus()?;
    focus(&inner, &mut relay, &a)?;
    wait(&inner, &mut relay, || Ok(a.reconcile()?.focused))?;
    assert!(!b.reconcile()?.focused);
    focus(&inner, &mut relay, &b)?;
    wait(&inner, &mut relay, || {
        Ok(!a.reconcile()?.focused && b.reconcile()?.focused)
    })?;
    outer.set_overlay_pane_focus(9, false);
    wait(&inner, &mut relay, || Ok(!b.reconcile()?.focused))?;
    focus(&inner, &mut relay, &a)?;
    assert!(
        !a.reconcile()?.focused,
        "request cannot activate an inactive physical pane"
    );
    assert!(
        neighbor_window.reconcile()?.focused,
        "unrelated physical pane retains focus"
    );
    outer.set_overlay_pane_focus(9, true);
    wait(&inner, &mut relay, || {
        Ok(a.reconcile()?.focused && !b.reconcile()?.focused)
    })?;
    assert!(neighbor_window.reconcile()?.focused);
    // A hidden original window cannot manufacture focus in its physical counterpart.
    a.set_visible(false)?;
    assert!(a.request_focus().is_err());
    assert!(inner.take_overlay_host_requests().is_empty());
    Ok(())
}

#[test]
fn focus_admission_is_one_shot_and_cannot_be_retargeted_to_a_same_id_peer() -> io::Result<()> {
    let (inner, _outer, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    project(&inner, &mut relay)?;
    let peer = snapshot(&inner, &[2]).surfaces[0].producer;
    std::thread::scope(|scope| -> io::Result<()> {
        let job = scope.spawn(|| a.request_focus());
        let original = request(&inner)?;
        let mut tampered = original.clone();
        tampered.surface.producer = peer;
        relay.overlay_host_request(&tampered).unwrap_err();
        let reply = relay.overlay_host_request(&original)?;
        assert!(
            relay.overlay_host_request(&original).is_err(),
            "duplicate focus dispatch refused"
        );
        inner.complete_overlay_host_request(original.id, Ok(reply));
        job.join().unwrap()?;
        Ok(())
    })?;
    wait(&inner, &mut relay, || Ok(a.reconcile()?.focused))?;
    assert!(!b.reconcile()?.focused);
    Ok(())
}

#[test]
fn drained_focus_requests_expire_on_owner_removal_and_outer_replacement() -> io::Result<()> {
    let (inner, _outer, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    project(&inner, &mut relay)?;
    focus(&inner, &mut relay, &b)?;
    std::thread::scope(|scope| -> io::Result<()> {
        let job = scope.spawn(|| a.request_focus());
        let old = request(&inner)?;
        inner.revoke_pane(1);
        relay.overlay_host_request(&old).unwrap_err();
        assert!(job.join().unwrap().is_err());
        Ok(())
    })?;
    assert!(b.reconcile()?.focused);
    let replacement = producer(&inner, 1)?;
    let fresh = window_of(&replacement, 0., 0.)?;
    project(&inner, &mut relay)?;
    assert!(
        b.reconcile()?.focused,
        "same-ID replacement cannot disturb peer"
    );
    std::thread::scope(|scope| -> io::Result<()> {
        let job = scope.spawn(|| fresh.request_focus());
        let old = request(&inner)?;
        let projection = snapshot(&inner, &[1, 2]).bridge_projection();
        relay.replace_session(&projection.surfaces, &projection.sources, &projection.nodes)?;
        assert!(
            !b.reconcile()?.focused,
            "retiring the physical route revokes old focus"
        );
        assert!(
            relay.overlay_host_request(&old).is_err(),
            "old request cannot select a replacement outer window"
        );
        assert!(job.join().unwrap().is_err());
        Ok(())
    })?;
    focus(&inner, &mut relay, &fresh)?;
    wait(&inner, &mut relay, || Ok(fresh.reconcile()?.focused))?;
    Ok(())
}

#[test]
fn dropped_action_route_fails_closed_and_cannot_revive_a_drained_request() -> io::Result<()> {
    let (inner, outer, mut relay) = fixture()?;
    assert_eq!(
        inner.attach_overlay_action_relay().unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    let producer = producer(&inner, 1)?;
    let window = window_of(&producer, 0., 0.)?;
    project(&inner, &mut relay)?;
    let old = std::thread::scope(|scope| -> io::Result<OverlayHostRequest> {
        let job = scope.spawn(|| window.request_focus());
        let old = request(&inner)?;
        drop(relay);
        assert!(job.join().unwrap().is_err());
        Ok(old)
    })?;
    assert!(
        window.request_focus().is_err(),
        "retired relay cannot fabricate local focus"
    );
    assert!(!window.reconcile()?.focused);
    let mut fresh = OuterBridge::builder(
        Secret32::from_hex(&pane(&outer, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .control_endpoint(outer.endpoint())
    .overlay_action_forwarding(&inner)
    .build()?;
    project(&inner, &mut fresh)?;
    fresh.overlay_host_request(&old).unwrap_err();
    focus(&inner, &mut fresh, &window)?;
    wait(&inner, &mut fresh, || Ok(window.reconcile()?.focused))?;
    Ok(())
}

#[test]
fn bridge_cancellation_retires_pending_focus_and_preserves_an_unrelated_physical_pane()
-> io::Result<()> {
    let (inner, outer, mut relay) = fixture()?;
    let first = producer(&inner, 1)?;
    let second = producer(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    project(&inner, &mut relay)?;
    focus(&inner, &mut relay, &b)?;
    let neighbor = overlay_producer(&outer.endpoint(), &pane(&outer, 10)?, "physical-peer")?;
    let neighbor_window = window_of(&neighbor, 0., 0.)?;
    neighbor_window.request_focus()?;
    std::thread::scope(|scope| -> io::Result<()> {
        let job = scope.spawn(|| a.request_focus());
        let original = request(&inner)?;
        relay.cancel_handle().cancel();
        relay.overlay_host_request(&original).unwrap_err();
        assert!(job.join().unwrap().is_err());
        Ok(())
    })?;
    assert!(!a.reconcile()?.focused && !b.reconcile()?.focused);
    assert!(neighbor_window.reconcile()?.focused);
    Ok(())
}
