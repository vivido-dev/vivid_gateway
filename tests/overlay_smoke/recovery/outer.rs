//! Explicit outer root replacement after real TCP shutdown, with retained inner scenes.

// Rust guideline compliant 2026-10-07
use super::*;
use crate::transport::Carrier;
use vivid_sdk::presenter::VectorFrame;

#[test]
fn outer_tcp_loss_replaces_only_physical_authority_and_retains_pending_scenes() -> io::Result<()> {
    retained_replacements(|_, _| Ok(()))
}

pub(super) fn retained_replacements(
    mut render: impl FnMut(&VectorFrame, [u8; 4]) -> io::Result<()>,
) -> io::Result<()> {
    let (inner, _) = overlay_presenter()?;
    let inner = Arc::new(inner);
    inner.enable_overlay_host_relay();
    let (outer, endpoint) = overlay_presenter()?;
    let outer = Arc::new(outer);
    let mut host = NativeVectorHost::attach(Arc::clone(&outer), "")?;
    let carrier = Arc::new(Carrier::new(&endpoint));
    let mut relay = OuterBridge::builder(
        Secret32::from_hex(&pane(&outer, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .connection_factory(Arc::<Carrier>::clone(&carrier))
    .overlay_action_forwarding(&inner)
    .overlay_presentation_forwarding(&inner)
    .build()?;
    let mut a = Producer::new(&inner, 1, false)?;
    let mut b = Producer::new(&inner, 2, false)?;
    // This physical peer uses an independent outer root session, with the same local IDs.
    let mut peer = Producer::new(&outer, 2, false)?;
    assert_eq!(
        (a.surface.id(), a.track.id()),
        (b.surface.id(), b.track.id())
    );
    assert_eq!(
        (a.surface.id(), a.track.id()),
        (peer.surface.id(), peer.track.id())
    );
    peer.paint(1, 0x0000_ffff)?;
    let peer_frame = prepared_peer(&mut host)?;
    render(&peer_frame, [0; 4])?;
    let peer_owner = peer.session.info().session_id;
    let original_owners = [a.session.info().session_id, b.session.info().session_id];
    let original_channels = [a.track.channel_generation(), b.track.channel_generation()];
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    forward_unshown(&inner, &mut relay, &a, 1)?;
    forward_unshown(&inner, &mut relay, &b, 1)?;
    let initial_surface = relay_surface(&relay, &a);
    let initial = unshown_frame(&mut host, &mut relay, initial_surface, 1)?;
    render(&initial, [255, 0, 0, 255])?;
    assert!(host.presented(&initial)?);
    wait(&inner, &mut relay, || {
        a.status().is_ok_and(|s| s.scene_revision == 1)
            && b.status().is_ok_and(|s| s.scene_revision == 1)
    })?;
    let old_input = host.input_target(&initial).unwrap();
    assert!(matches!(
        pointer(&mut host, &old_input, 10.)?,
        VectorInputResult::Delivered { .. }
    ));
    let saved_input = drained(&mut relay)?;
    drop(initial);
    let colors = [
        (0x00ff_00ff, [0, 255, 0, 255]),
        (0xffff_00ff, [255, 255, 0, 255]),
        (0xff00_00ff, [255, 0, 0, 255]),
    ];
    let mut latest = None;
    for (index, (color, rgba)) in colors.into_iter().enumerate() {
        drop(latest.take());
        let revision = u64::try_from(index).unwrap() + 2;
        a.paint(revision, color)?;
        forward_unshown(&inner, &mut relay, &a, revision)?;
        let old_surface = relay_surface(&relay, &a);
        let stale = unshown_frame(&mut host, &mut relay, old_surface, 2)?;
        let old_owner = stale.drawings()[0].window.context().session().session_id();
        render(&stale, rgba)?;
        let generation = relay.diagnostic_instance_generation();
        carrier.disconnect()?;
        let end = Instant::now() + Duration::from_secs(3);
        loop {
            match relay.service_session_events() {
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
                    break;
                }
                Ok(_) => assert!(Instant::now() < end, "outer TCP loss was not observed"),
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            relay.service_session_events().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        let projection = snapshot(&inner, &[1, 2]).bridge_projection();
        relay.replace_session(&projection.surfaces, &projection.sources, &projection.nodes)?;
        assert_eq!(relay.diagnostic_instance_generation(), generation + 1);
        // The application owns replay. No new source submission is offered after replacement.
        forward_unshown(&inner, &mut relay, &a, revision)?;
        forward_unshown(&inner, &mut relay, &b, 1)?;
        let fresh_surface = relay_surface(&relay, &a);
        let fresh = unshown_frame(&mut host, &mut relay, fresh_surface, 1)?;
        let fresh_owner = fresh.drawings()[0].window.context().session().session_id();
        assert_ne!(fresh_owner, old_owner);
        assert_eq!(
            fresh_surface, old_surface,
            "fixture must reuse outer numeric IDs"
        );
        render(&fresh, rgba)?;
        render(&stale, rgba)?;
        assert!(
            !host.presented(&stale)?,
            "retired outer root acknowledged display"
        );
        drop(stale);
        assert_eq!(
            pointer(&mut host, &old_input, 10.)?,
            VectorInputResult::Rejected
        );
        render(&peer_frame, [0; 4])?;
        assert_eq!(peer.status()?.scene_revision, 0);
        for ((producer, owner), channel) in [(&a, original_owners[0]), (&b, original_owners[1])]
            .into_iter()
            .zip(original_channels)
        {
            assert_eq!(producer.session.info().session_id, owner);
            assert_eq!(producer.session.info().resume_generation, 0);
            assert_eq!(producer.track.channel_generation(), channel);
            assert_eq!(
                producer.status()?.scene_revision,
                1,
                "readback/replacement invented display"
            );
        }
        assert!(relay.take_source_losses().is_empty());
        latest = Some(fresh);
    }
    for input in saved_input {
        assert!(
            !input.forward(),
            "drained physical input crossed outer root replacement"
        );
    }
    let fresh = latest.unwrap();
    assert!(host.presented(&fresh)?);
    wait(&inner, &mut relay, || {
        a.status().is_ok_and(|s| s.scene_revision == 4)
    })?;
    assert_eq!(b.status()?.scene_revision, 1);
    assert!(
        host.presented(&peer_frame)?,
        "unrelated physical peer lost its pending frame"
    );
    assert_eq!(peer.status()?.scene_revision, 1);
    assert_eq!(peer.session.info().session_id, peer_owner);
    // A fresh physical grant works; the obsolete target above remains rejected.
    let fresh_input = host.input_target(&fresh).unwrap();
    assert!(matches!(
        pointer(&mut host, &fresh_input, 74.)?,
        VectorInputResult::Delivered { .. }
    ));
    input(&inner, &mut relay)?;
    wait(&inner, &mut relay, || b.status().is_ok_and(|s| s.focused))?;
    Ok(())
}

fn relay_surface(relay: &OuterBridge, producer: &Producer) -> u64 {
    relay.outer_surface_id(producer.key()).unwrap()
}

fn prepared_peer(host: &mut NativeVectorHost) -> io::Result<VectorFrame> {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        let frame = host.prepare(2, 1.)?;
        if frame.drawings().len() == 1 {
            return Ok(frame);
        }
        assert!(
            Instant::now() < end,
            "independent physical peer did not prepare"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(feature = "testing")]
#[test]
fn outer_disconnect_hook_rejects_peer_and_retired_owner_ids() -> io::Result<()> {
    let (inner, outer, _host, mut relay) = fixture_mode(true)?;
    let mut a = Producer::new(&inner, 1, false)?;
    a.paint(1, 0xff00_00ff)?;
    forward_unshown(&inner, &mut relay, &a, 1)?;
    let mut peer = Producer::new(&outer, 2, false)?;
    peer.paint(1, 0x0000_ffff)?;
    assert_eq!(relay_surface(&relay, &a), peer.surface.id());
    let old = relay.outer_session_id();
    assert_ne!(old, peer.session.info().session_id);
    assert_eq!(
        relay
            .disconnect_outer_session_for_test(peer.session.info().session_id)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    relay.service_session_events()?;
    relay.disconnect_outer_session_for_test(old)?;
    let end = Instant::now() + Duration::from_secs(3);
    while relay.service_session_events().is_ok() {
        assert!(
            Instant::now() < end,
            "exact outer disconnect did not close control"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let projection = snapshot(&inner, &[1, 2]).bridge_projection();
    relay.replace_session(&projection.surfaces, &projection.sources, &projection.nodes)?;
    assert_ne!(relay.outer_session_id(), old);
    assert_eq!(
        relay
            .disconnect_outer_session_for_test(old)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    relay.service_session_events()?;
    peer.session.query_surface(&peer.surface)?;
    assert_eq!(peer.status()?.scene_revision, 0);
    Ok(())
}
