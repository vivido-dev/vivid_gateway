//! Remote one-shot action admission leaves control and same-ID peers responsive.

// Rust guideline compliant 2026-10-07
use super::recovery::Producer;
use super::*;
use std::collections::HashMap;
use std::sync::Arc;
use vivid_gateway::ExternalActionClaim;
use vivid_protocol::messages;
use vivid_protocol::overlay::wire::{Action, WindowAction};
use vivid_sdk::presenter::{
    BridgeSurfaceKey, OverlayActionRoute, OverlayHostRequest, OverlayInputEpoch,
};

struct Fixture {
    inner: Arc<VirtualVivid>,
    outer: VirtualVivid,
    route: OverlayActionRoute,
    relay: OuterBridge,
    epochs: HashMap<BridgeSurfaceKey, OverlayInputEpoch>,
}
impl Fixture {
    fn new() -> io::Result<Self> {
        let (inner, _) = overlay_presenter()?;
        let inner = Arc::new(inner);
        inner.enable_overlay_host_relay();
        let route = inner.attach_overlay_action_relay()?;
        let (outer, endpoint) = overlay_presenter()?;
        let mut relay = bridge(&endpoint, &pane(&outer, 9)?)?;
        relay.set_external_overlay_input_epochs(&[])?;
        Ok(Self {
            inner,
            outer,
            route,
            relay,
            epochs: HashMap::new(),
        })
    }
    fn project(&mut self) -> io::Result<()> {
        let mut projection = snapshot(&self.inner, &[1, 2]).bridge_projection();
        let mut epochs = Vec::new();
        self.epochs.clear();
        for surface in &mut projection.surfaces {
            if let Some(window) = &mut surface.overlay_window {
                window.offset_x = 47;
                window.offset_y = 29;
                window.x += 47;
                window.y += 29;
                if let Some(epoch) = self.route.input_epoch(surface.key) {
                    self.epochs.insert(surface.key, epoch);
                    epochs.push((surface.key, surface.key.producer));
                }
            }
        }
        self.relay.set_external_overlay_input_epochs(&epochs)?;
        self.relay
            .rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
        Ok(())
    }
    fn input(&mut self) -> io::Result<()> {
        for input in self.relay.take_overlay_input_with_epochs()? {
            assert_eq!(input.correlation, input.surface.producer);
            if let Some(epoch) = self.epochs.get(&input.surface)
                && let Some(input) = self.route.input(*epoch, &input.body)?
            {
                input.forward();
            }
        }
        Ok(())
    }
    fn claim(&mut self) -> io::Result<ExternalActionClaim> {
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            self.input()?;
            let mut claims = self.relay.take_external_overlay_action_claims();
            if !claims.is_empty() {
                assert_eq!(claims.len(), 1);
                return Ok(claims.pop().unwrap());
            }
            assert!(
                Instant::now() < until,
                "physical query did not yield a claim"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn finish(&mut self, id: u64) -> io::Result<bool> {
        let until = Instant::now() + Duration::from_secs(3);
        loop {
            self.input()?;
            let mut replies = self.relay.take_overlay_host_replies();
            if !replies.is_empty() {
                assert_eq!(replies.len(), 1);
                let (reply, result) = replies.pop().unwrap();
                assert_eq!(reply, id);
                let success = result.is_ok();
                self.inner
                    .complete_overlay_host_request(id, result.map_err(|e| e.to_string()));
                self.input()?;
                return Ok(success);
            }
            assert!(Instant::now() < until, "external action reply stalled");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn start(&mut self, request: &OverlayHostRequest) -> io::Result<()> {
        let admission = self.route.request(request);
        let preceding = admission.preceding_windows().unwrap();
        self.project()?;
        self.relay
            .start_external_overlay_action(request, &preceding, admission.centered_bounds())
    }
    fn action(&mut self, producer: &Producer, kind: WindowAction, allow: bool) -> io::Result<()> {
        let handle = producer.session.overlay_host_handle().unwrap();
        let action = Action {
            address: producer.address(),
            expected_revision: producer.status()?.window.expected_revision,
            action: kind,
        };
        std::thread::scope(|scope| {
            let caller = scope.spawn(move || {
                handle(
                    messages::OVERLAY_ACTION,
                    action.address.surface_id,
                    action.payload()?,
                )
            });
            let request = super::focus::request(&self.inner)?;
            let id = request.id;
            let admission = self.route.request(&request);
            self.start(&request)?;
            let claim = self.claim()?;
            assert_eq!(claim.request_id(), id);
            assert!(!caller.is_finished());
            assert!(self.relay.take_external_overlay_action_claims().is_empty());
            assert!(claim.complete(allow && admission.claim()));
            assert_eq!(self.finish(id)?, allow);
            let result = caller.join().unwrap();
            if allow {
                result?;
            } else {
                result.unwrap_err();
            }
            Ok(())
        })
    }
}
fn node(producer: &mut Producer) -> io::Result<SceneNode> {
    let node = SceneNode {
        owning_context_id: producer.surface.context_id(),
        node_id: 1,
        surface_context_id: producer.surface.context_id(),
        surface_id: producer.surface.id(),
        geometry: vec![
            (0, Value::Unsigned(1)),
            (1, Value::Unsigned(0)),
            (2, Value::Unsigned(0)),
            (3, Value::Unsigned(1 << 32)),
            (4, Value::Unsigned(1 << 32)),
            (5, Value::Unsigned(0)),
        ],
        fit: Fit::Contain,
        linear_sampling: true,
        z_index: 0,
        visible: true,
        opacity: u16::MAX,
        clip: None,
    };
    producer
        .session
        .create_node(&node, &RequestMetadata::default())?;
    Ok(node)
}
fn physical(f: &Fixture, key: BridgeSurfaceKey) -> vivid_sdk::presenter::SnapshotOverlayWindow {
    let id = f.relay.outer_surface_id(key).unwrap();
    snapshot(&f.outer, &[9])
        .surfaces
        .into_iter()
        .find(|s| s.surface == id)
        .unwrap()
        .overlay_window
        .unwrap()
}

#[test]
fn a_delayed_positive_decision_cannot_start_a_fresh_original_lifetime() -> io::Result<()> {
    let mut f = Fixture::new()?;
    let mut a = Producer::new(&f.inner, 1, false)?;
    let mut b = Producer::new(&f.inner, 2, false)?;
    node(&mut a)?;
    let mut b_node = node(&mut b)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    f.project()?;
    let before = physical(&f, a.key());
    let handle = a.session.overlay_host_handle().unwrap();
    let action = Action {
        address: a.address(),
        expected_revision: a.status()?.window.expected_revision,
        action: WindowAction::Center,
    };
    std::thread::scope(|scope| {
        let caller = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                action.address.surface_id,
                action.payload()?,
            )
        });
        let request = super::focus::request(&f.inner)?;
        let id = request.id;
        let original = f.route.request(&request);
        f.start(&request)?;
        let claim = f.claim()?;
        let sent = Instant::now();
        assert!(f.route.request(&request).claim());
        assert!(original.remaining_lifetime().is_some());
        std::thread::sleep(Duration::from_millis(20));
        assert!(!claim.complete_before(true, sent + Duration::from_millis(1)));
        assert!(!f.finish(id)?);
        caller.join().unwrap().unwrap_err();
        assert!(original.remaining_lifetime().is_none());
        assert_eq!(
            (physical(&f, a.key()).x, physical(&f, a.key()).y),
            (before.x, before.y)
        );
        Ok::<_, io::Error>(())
    })?;
    a.session.close()?;
    f.project()?;
    b_node.z_index = 31;
    b.session
        .update_node(&b_node, &RequestMetadata::default())?;
    f.project()?;
    assert_eq!(snapshot(&f.inner, &[2]).nodes[0].config.node.z_index, 31);
    f.action(&b, WindowAction::Focus, true)?;
    b.session.close()?;
    Ok(())
}

#[test]
fn remote_claims_gate_all_window_actions_and_preserve_a_same_id_peer_update() -> io::Result<()> {
    let mut f = Fixture::new()?;
    let mut a = Producer::new(&f.inner, 1, false)?;
    let mut b = Producer::new(&f.inner, 2, false)?;
    let _a_node = node(&mut a)?;
    let mut b_node = node(&mut b)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    assert_eq!(
        (a.key().context, a.key().surface),
        (b.key().context, b.key().surface)
    );
    f.project()?;
    let before = physical(&f, b.key());
    f.action(&b, WindowAction::Center, false)?;
    assert_eq!(physical(&f, b.key()).x, before.x);
    assert_eq!(physical(&f, b.key()).y, before.y);
    for kind in [
        WindowAction::Focus,
        WindowAction::Raise,
        WindowAction::Lower,
        WindowAction::Center,
    ] {
        f.action(&a, kind, true)?;
    }
    assert_eq!(
        a.status()?.window.options.bounds,
        Rect::new(296., 168., 48., 48.).unwrap()
    );
    let centered = physical(&f, a.key());
    assert_eq!((centered.x, centered.y), (343, 197));
    f.action(&a, WindowAction::Close, true)?;
    a.session.close()?;
    f.project()?;
    let current = snapshot(&f.inner, &[1, 2]);
    assert_eq!(current.sources.len(), 1);
    assert_eq!(current.sources[0].key.producer, b.key().producer);
    assert_eq!(current.nodes.len(), 1);
    assert_eq!(current.nodes[0].producer, b.key().producer);
    b_node.z_index = 3;
    b.session
        .update_node(&b_node, &RequestMetadata::default())?;
    let current = snapshot(&f.inner, &[2]);
    assert_eq!(current.nodes[0].config.node.z_index, 3);
    f.project()?;
    assert_eq!(physical(&f, b.key()).x, before.x);
    f.action(&b, WindowAction::Raise, true)?;
    b.session.close()?;
    Ok(())
}

#[test]
fn retired_physical_claim_cannot_authorize_a_replacement_or_block_peer_control() -> io::Result<()> {
    let mut f = Fixture::new()?;
    let mut a = Producer::new(&f.inner, 1, false)?;
    let mut b = Producer::new(&f.inner, 2, false)?;
    node(&mut a)?;
    let mut b_node = node(&mut b)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    f.project()?;
    let handle = a.session.overlay_host_handle().unwrap();
    let action = Action {
        address: a.address(),
        expected_revision: a.status()?.window.expected_revision,
        action: WindowAction::Center,
    };
    std::thread::scope(|scope| {
        let caller = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                action.address.surface_id,
                action.payload()?,
            )
        });
        let request = super::focus::request(&f.inner)?;
        let id = request.id;
        f.start(&request)?;
        let claim = f.claim()?;
        // While A's admission is withheld, B still mutates over its real socket.
        b_node.z_index = 2;
        b.session
            .update_node(&b_node, &RequestMetadata::default())?;
        assert_eq!(snapshot(&f.inner, &[2]).nodes[0].config.node.z_index, 2);
        let projection = snapshot(&f.inner, &[1, 2]).bridge_projection();
        f.relay
            .replace_session(&projection.surfaces, &projection.sources, &projection.nodes)?;
        assert!(!claim.complete(true));
        assert!(!f.finish(id)?);
        caller.join().unwrap().unwrap_err();
        assert!(f.relay.take_external_overlay_action_claims().is_empty());
        Ok::<_, io::Error>(())
    })?;
    a.session.close()?;
    f.project()?;
    b_node.z_index = 4;
    b.session
        .update_node(&b_node, &RequestMetadata::default())?;
    f.project()?;
    assert_eq!(snapshot(&f.inner, &[2]).nodes[0].config.node.z_index, 4);
    f.action(&b, WindowAction::Center, true)?;
    b.session.close()?;
    Ok(())
}

#[test]
fn withheld_owner_claims_are_bounded_and_do_not_block_a_peer_action() -> io::Result<()> {
    let mut f = Fixture::new()?;
    let mut a = Producer::new(&f.inner, 1, false)?;
    let mut b = Producer::new(&f.inner, 2, false)?;
    node(&mut a)?;
    let mut b_node = node(&mut b)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    f.project()?;
    let action = Action {
        address: a.address(),
        expected_revision: a.status()?.window.expected_revision,
        action: WindowAction::Center,
    };
    std::thread::scope(|scope| {
        let mut callers = Vec::new();
        for _ in 0..5 {
            let handle = a.session.overlay_host_handle().unwrap();
            callers.push(scope.spawn(move || {
                handle(
                    messages::OVERLAY_ACTION,
                    action.address.surface_id,
                    action.payload()?,
                )
            }));
        }
        let mut requests = Vec::new();
        let until = Instant::now() + Duration::from_secs(3);
        while requests.len() < 5 {
            requests.extend(f.inner.take_overlay_host_requests());
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        for request in &requests[..4] {
            f.start(request)?;
        }
        assert_eq!(
            f.start(&requests[4]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        f.inner.complete_overlay_host_request(
            requests[4].id,
            Err("owner worker budget is full".into()),
        );
        let mut held = Vec::new();
        while held.len() < 4 {
            held.extend(f.relay.take_external_overlay_action_claims());
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        b_node.z_index = 7;
        b.session
            .update_node(&b_node, &RequestMetadata::default())?;
        f.action(&b, WindowAction::Raise, true)?;
        assert_eq!(snapshot(&f.inner, &[2]).nodes[0].config.node.z_index, 7);
        // Dropping the four one-shot receipts is an immediate refusal, not a
        // wait for the original ten-second admission deadline.
        drop(held);
        let mut replies = 0;
        while replies < 4 {
            for (id, result) in f.relay.take_overlay_host_replies() {
                result.unwrap_err();
                f.inner
                    .complete_overlay_host_request(id, Err("remote claim was dropped".into()));
                replies += 1;
            }
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        for caller in callers {
            caller.join().unwrap().unwrap_err();
        }
        Ok::<_, io::Error>(())
    })?;
    a.session.close()?;
    f.project()?;
    b_node.z_index = 8;
    b.session
        .update_node(&b_node, &RequestMetadata::default())?;
    f.project()?;
    assert_eq!(snapshot(&f.inner, &[2]).nodes[0].config.node.z_index, 8);
    b.session.close()?;
    Ok(())
}

#[test]
fn removing_one_owner_retires_only_its_outstanding_physical_claim() -> io::Result<()> {
    let mut f = Fixture::new()?;
    let mut a = Producer::new(&f.inner, 1, false)?;
    let mut b = Producer::new(&f.inner, 2, false)?;
    node(&mut a)?;
    let mut b_node = node(&mut b)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    f.project()?;
    let handle = a.session.overlay_host_handle().unwrap();
    let cancel = a.session.cancel_handle();
    let action = Action {
        address: a.address(),
        expected_revision: a.status()?.window.expected_revision,
        action: WindowAction::Center,
    };
    std::thread::scope(|scope| {
        let caller = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                action.address.surface_id,
                action.payload()?,
            )
        });
        let request = super::focus::request(&f.inner)?;
        let id = request.id;
        f.start(&request)?;
        let claim = f.claim()?;
        cancel();
        let until = Instant::now() + Duration::from_secs(3);
        while snapshot(&f.inner, &[1, 2]).sources.len() != 1 {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        f.project()?;
        assert!(!claim.complete(true));
        assert!(!f.finish(id)?);
        caller.join().unwrap().unwrap_err();
        Ok::<_, io::Error>(())
    })?;
    let current = snapshot(&f.inner, &[2]);
    assert_eq!(current.sources.len(), 1);
    assert_eq!(current.nodes.len(), 1);
    assert_eq!(current.nodes[0].producer, b.key().producer);
    b_node.z_index = 9;
    b.session
        .update_node(&b_node, &RequestMetadata::default())?;
    f.project()?;
    assert_eq!(snapshot(&f.inner, &[2]).nodes[0].config.node.z_index, 9);
    f.action(&b, WindowAction::Focus, true)?;
    b.session.close()?;
    Ok(())
}

#[test]
fn external_action_metadata_is_validated_before_a_claim_or_physical_mutation() -> io::Result<()> {
    let mut f = Fixture::new()?;
    let a = Producer::new(&f.inner, 1, false)?;
    let b = Producer::new(&f.inner, 2, false)?;
    f.project()?;
    let handle = a.session.overlay_host_handle().unwrap();
    let action = Action {
        address: a.address(),
        expected_revision: a.status()?.window.expected_revision,
        action: WindowAction::Center,
    };
    std::thread::scope(|scope| {
        let caller = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                action.address.surface_id,
                action.payload()?,
            )
        });
        let request = super::focus::request(&f.inner)?;
        let admission = f.route.request(&request);
        let barrier = admission.preceding_windows().unwrap();
        let center = admission.centered_bounds();
        let before = physical(&f, a.key());
        for invalid in [
            Vec::new(),
            vec![barrier[0], barrier[0]],
            vec![vivid_sdk::presenter::OverlayActionWindow {
                surface: b.key(),
                ..barrier[0]
            }],
            vec![barrier[0]; vivid_protocol::overlay::MAX_WINDOWS_PER_OWNER + 1],
        ] {
            assert_eq!(
                f.relay
                    .start_external_overlay_action(&request, &invalid, center)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert_eq!(
            f.relay
                .start_external_overlay_action(&request, &barrier, None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(f.relay.take_external_overlay_action_claims().is_empty());
        assert_eq!(
            (physical(&f, a.key()).x, physical(&f, a.key()).y),
            (before.x, before.y)
        );
        let mut update = a.status()?.window;
        update.options.bounds = Rect::new(3., 0., 48., 48.).unwrap();
        a.session.set_overlay_window(&update)?;
        f.project()?;
        assert_eq!(
            f.relay
                .start_external_overlay_action(&request, &barrier, center)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert!(f.relay.take_external_overlay_action_claims().is_empty());
        assert_eq!((physical(&f, a.key()).x, physical(&f, a.key()).y), (50, 29));
        f.inner
            .complete_overlay_host_request(request.id, Err("invalid metadata refused".into()));
        caller.join().unwrap().unwrap_err();
        Ok::<_, io::Error>(())
    })?;
    a.session.close()?;
    b.session.close()?;
    Ok(())
}

#[test]
fn unanswered_external_claim_expires_without_dispatch_while_peer_control_stays_live()
-> io::Result<()> {
    let mut f = Fixture::new()?;
    let mut a = Producer::new(&f.inner, 1, false)?;
    let mut b = Producer::new(&f.inner, 2, false)?;
    node(&mut a)?;
    let mut b_node = node(&mut b)?;
    a.paint(1, 0xff00_00ff)?;
    b.paint(1, 0x0000_ffff)?;
    f.project()?;
    let before = physical(&f, a.key());
    let handle = a.session.overlay_host_handle().unwrap();
    let action = Action {
        address: a.address(),
        expected_revision: a.status()?.window.expected_revision,
        action: WindowAction::Center,
    };
    std::thread::scope(|scope| {
        let caller = scope.spawn(move || {
            handle(
                messages::OVERLAY_ACTION,
                action.address.surface_id,
                action.payload()?,
            )
        });
        let request = super::focus::request(&f.inner)?;
        let id = request.id;
        let started = Instant::now();
        f.start(&request)?;
        let claim = f.claim()?;
        f.action(&b, WindowAction::Raise, true)?;
        // Exercise the actual admission wait. Renew both independent lanes and
        // perform real peer UPDATE_NODE requests throughout the unanswered hold.
        let mut renewed = Instant::now();
        let mut updates = 0;
        while started.elapsed() < Duration::from_millis(10_250) {
            f.input()?;
            if renewed.elapsed() >= Duration::from_millis(500) {
                a.lane.renew(5_000_000, Duration::from_secs(1))?;
                b.lane.renew(5_000_000, Duration::from_secs(1))?;
                updates += 1;
                b_node.z_index = updates;
                b.session
                    .update_node(&b_node, &RequestMetadata::default())?;
                assert_eq!(
                    snapshot(&f.inner, &[2]).nodes[0].config.node.z_index,
                    updates
                );
                renewed = Instant::now();
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(updates >= 10);
        assert!(!claim.complete(true));
        assert!(!f.finish(id)?);
        caller.join().unwrap().unwrap_err();
        assert_eq!(
            (physical(&f, a.key()).x, physical(&f, a.key()).y),
            (before.x, before.y)
        );
        Ok::<_, io::Error>(())
    })?;
    a.session.close()?;
    f.project()?;
    b_node.z_index = 30;
    b.session
        .update_node(&b_node, &RequestMetadata::default())?;
    f.project()?;
    assert_eq!(snapshot(&f.inner, &[2]).nodes[0].config.node.z_index, 30);
    f.action(&b, WindowAction::Focus, true)?;
    b.session.close()?;
    Ok(())
}
