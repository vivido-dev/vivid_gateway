//! Stack-preserving projection and outer reconstruction over authenticated sockets.

// Rust guideline compliant 2026-10-07
use super::*;

fn order(presenter: &VirtualVivid, pane: u64, owner: Option<u64>) -> Vec<(i64, bool)> {
    snapshot(presenter, &[pane])
        .surfaces
        .into_iter()
        .filter(|surface| owner.is_none_or(|owner| surface.producer == owner))
        .filter_map(|surface| {
            surface
                .overlay_window
                .map(|window| (window.x, window.visible))
        })
        .collect()
}

#[test]
fn hidden_same_id_window_stack_survives_outer_replacement_and_owner_close() -> io::Result<()> {
    let (inner, endpoint) = overlay_presenter()?;
    let secret = pane(&inner, 1)?;
    let first = overlay_producer(&endpoint, &secret, "stack-a")?;
    let peer = overlay_producer(&endpoint, &secret, "stack-b")?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&peer, 16., 0.)?;
    a.present(scene(0xff00_00ff))?;
    b.present(scene(0x0000_ffff))?;
    let original = snapshot(&inner, &[1]);
    assert_eq!(original.sources.len(), 2);
    let one = original.sources[0].key;
    let two = original.sources[1].key;
    assert_eq!(
        (one.context, one.surface, one.track),
        (two.context, two.surface, two.track)
    );
    assert_ne!(one.producer, two.producer);
    assert_eq!(order(&inner, 1, None), [(0, true), (16, true)]);
    a.raise()?;
    assert_eq!(order(&inner, 1, None), [(16, true), (0, true)]);
    a.set_visible(false)?;
    assert_eq!(order(&inner, 1, None), [(16, true), (0, false)]);

    let (outer, endpoint) = overlay_presenter()?;
    let secret = pane(&outer, 9)?;
    let unrelated = overlay_producer(&endpoint, &secret, "stack-peer")?;
    let unrelated_window = window_of(&unrelated, 300., 0.)?;
    unrelated_window.present(scene(0x00ff_00ff))?;
    let unrelated_revision = unrelated_window.reconcile()?.window_revision;
    let mut relay = bridge(&endpoint, &secret)?;
    let projected = snapshot(&inner, &[1]).bridge_projection();
    relay.rebuild(&projected.surfaces, &projected.sources, &projected.nodes)?;
    assert_eq!(
        order(&outer, 9, Some(relay.outer_session_id())),
        [(16, true), (0, false)]
    );
    for _ in 0..4 {
        relay.replace_session(&projected.surfaces, &projected.sources, &projected.nodes)?;
        assert_eq!(
            order(&outer, 9, Some(relay.outer_session_id())),
            [(16, true), (0, false)]
        );
        assert_eq!(
            unrelated_window.reconcile()?.window_revision,
            unrelated_revision
        );
    }
    a.set_visible(true)?;
    a.lower()?;
    assert_eq!(order(&inner, 1, None), [(0, true), (16, true)]);
    let projected = snapshot(&inner, &[1]).bridge_projection();
    relay.replace_session(&projected.surfaces, &projected.sources, &projected.nodes)?;
    assert_eq!(
        order(&outer, 9, Some(relay.outer_session_id())),
        [(0, true), (16, true)]
    );
    a.close()?;
    first.close()?;
    let projected = snapshot(&inner, &[1]).bridge_projection();
    relay.rebuild(&projected.surfaces, &projected.sources, &projected.nodes)?;
    assert_eq!(
        order(&outer, 9, Some(relay.outer_session_id())),
        [(16, true)]
    );
    assert_eq!(
        unrelated_window.reconcile()?.window_revision,
        unrelated_revision
    );
    b.close()?;
    peer.close()?;
    unrelated_window.close()?;
    unrelated.close()?;
    Ok(())
}
