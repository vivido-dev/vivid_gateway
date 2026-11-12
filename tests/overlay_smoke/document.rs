//! Native document events cross independent authenticated hops with opaque owner grants.

// Rust guideline compliant 2026-10-07
use super::*;
use std::sync::Arc;
use vivid_protocol::{
    overlay::wire::text::editor::{Context, Edit},
    registry,
};
use vivid_sdk::presenter::{
    NativeVectorHost, VectorDocumentEdit, VectorInputEvent, VectorInputResult, VectorInputTarget,
    VectorTextTarget,
};

fn context(editor: u64, revision: u64) -> Context {
    Context {
        editor,
        revision,
        text: "a😀éz".into(),
        cursor: 7,
        anchor: 5,
        sensitive: false,
    }
}
fn canvas() -> Canvas {
    let mut canvas = scene(0xff22_4488);
    canvas
        .push(vivid_sdk::overlay::Command::Hit {
            id: 7,
            path: Path::rectangle(Rect::new(0., 0., 120., 60.).unwrap()).unwrap(),
            role: vivid_sdk::overlay::HitRole::Input,
            cursor: None,
        })
        .unwrap();
    canvas
}
fn connect(inner: &VirtualVivid, pane_id: u64) -> io::Result<OverlaySession> {
    let mut config = ProducerConfig {
        authentication: ProducerAuthentication::root_hex(&pane(inner, pane_id)?)
            .map_err(io::Error::other)?,
        optional_profiles: vec![registry::OVERLAY_EDITOR.into()],
        ..ProducerConfig::default()
    };
    pin_endpoints(&mut config, inner.endpoint());
    OverlaySession::connect(config)
}
fn input(inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    for (key, body) in relay.take_overlay_input()? {
        inner.relay_overlay_input(key, &body)?;
    }
    Ok(())
}
fn display(
    host: &mut NativeVectorHost,
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    drawings: usize,
) -> VectorInputTarget {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        relay.retry_pending_activation().unwrap();
        input(inner, relay).unwrap();
        let frame = host.prepare(9, 1.).unwrap();
        let count = frame.drawings().len();
        let presented = host.presented(&frame).unwrap();
        if count == drawings && presented {
            return host.input_target(&frame).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "relayed native scene was not displayed: drawings={count}, expected={drawings}, presented={presented}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn click(host: &mut NativeVectorHost, target: &VectorInputTarget, x: f64) -> VectorInputTarget {
    for down in [true, false] {
        let result = host
            .dispatch_input(
                target,
                VectorInputEvent::Pointer {
                    position: [x, 10.],
                    button: Some((1, down)),
                    modifiers: 0,
                },
            )
            .unwrap();
        assert!(
            matches!(
                result,
                VectorInputResult::Delivered { .. } | VectorInputResult::Deferred
            ),
            "pointer x={x} down={down}: {result:?}"
        );
    }
    let frame = host.prepare(9, 1.).unwrap();
    assert!(host.presented(&frame).unwrap());
    let target = host.input_target(&frame).unwrap();
    host.flush_input();
    target
}
fn publish(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    host: &mut NativeVectorHost,
    window: &OverlayWindow,
    scene: u64,
    context: Context,
) -> io::Result<()> {
    std::thread::scope(|scope| {
        let job = scope.spawn(|| window.set_text_context(scene, Some(context)));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !job.is_finished() {
            host.poll();
            input(inner, relay)?;
            for request in inner.take_overlay_host_requests() {
                relay.start_overlay_host_request(request)?;
            }
            for (id, result) in relay.take_overlay_host_replies() {
                if let Err(error) = &result {
                    eprintln!("document reply failure: {error:?}");
                }
                inner.complete_overlay_host_request(id, result.map_err(|error| error.to_string()));
            }
            assert!(Instant::now() < deadline, "document publication stalled");
            std::thread::sleep(Duration::from_millis(1));
        }
        job.join().unwrap()
    })
}
fn received(producer: &OverlaySession) -> Edit {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(vivid_sdk::OverlayLaneEvent::EditorEdit(edit)) =
            producer.wait_event(Duration::from_millis(5)).unwrap()
        {
            return edit;
        }
        assert!(
            Instant::now() < end,
            "relayed edit did not reach original producer"
        );
    }
}
fn pending(inner: &VirtualVivid, relay: &mut OuterBridge) -> vivid_gateway::DocumentInput {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        input(inner, relay).unwrap();
        if let Some(edit) = relay.take_overlay_document_input().pop() {
            return edit;
        }
        assert!(
            Instant::now() < end,
            "outer document event did not reach bridge"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn send(host: &mut NativeVectorHost, target: &VectorTextTarget, commit: &str) {
    let context = host.editor_state(target).unwrap().document.unwrap();
    assert_eq!(
        host.dispatch_document(
            target,
            &VectorDocumentEdit {
                editor: context.editor,
                revision: context.revision,
                commit: Some(commit.into()),
                ..Default::default()
            }
        )
        .unwrap(),
        VectorInputResult::Delivered { text: true }
    );
}
fn project(inner: &VirtualVivid, relay: &mut OuterBridge) -> io::Result<()> {
    let current = snapshot(inner, &[1, 2]);
    let projection = current.bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    for source in current.sources {
        for (kind, body) in source.retained_vector {
            relay.media_chunk(MediaChunk {
                delivery_id: 0,
                source: source.key,
                record_type: kind,
                offset: 0,
                total: u32::try_from(body.len()).unwrap(),
                last: true,
                bytes: body.to_vec(),
            })?;
            finish_delivery(inner, relay, 0)?;
        }
    }
    Ok(())
}

#[test]
fn native_documents_reoriginate_revisions_and_revoke_delayed_input_without_harming_a_peer()
-> io::Result<()> {
    let (inner, _) = overlay_presenter()?;
    let inner = Arc::new(inner);
    inner.enable_overlay_host_relay();
    let listener = TcpPresenterListener::bind()?;
    let endpoint = listener.endpoint();
    let mut config = PresenterConfig::terminal_with_overlay(MediaConfig::default());
    config
        .supported_profiles
        .push(registry::OVERLAY_EDITOR.into());
    let outer = Arc::new(VirtualVivid::start_configured_eventless(listener, config)?);
    let mut host = NativeVectorHost::attach(Arc::clone(&outer), "")?;
    let mut relay = OuterBridge::builder(
        Secret32::from_hex(&pane(&outer, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .control_endpoint(endpoint)
    .overlay_document_editing(&inner)
    .build()?;
    assert!(
        relay
            .overlay_host_profiles()
            .iter()
            .any(|p| p == registry::OVERLAY_EDITOR)
    );
    inner.set_overlay_document_host_profiles(&relay.overlay_host_profiles());
    let first = connect(&inner, 1)?;
    let second = connect(&inner, 2)?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    for window in [&a, &b] {
        for _ in 0..2 {
            let submission = window.submit(canvas())?;
            assert_eq!(
                submission.wait(Duration::from_secs(3))?,
                Some(vivid_sdk::overlay::PresentationOutcome::Presented)
            );
        }
    }
    project(&inner, &mut relay)?;
    let target = display(&mut host, &inner, &mut relay, 2);
    let target = click(&mut host, &target, 10.);
    input(&inner, &mut relay)?;
    let text = host.text_target(&target).unwrap();
    let original = context(41, 73);
    publish(&inner, &mut relay, &mut host, &a, 2, original.clone())?;
    let mapped = host.editor_state(&text).unwrap().document.unwrap();
    assert_ne!(mapped.editor, original.editor);
    assert_ne!(mapped.revision, original.revision);
    assert_eq!(mapped.text, original.text);
    assert_eq!(
        host.dispatch_document(
            &text,
            &VectorDocumentEdit {
                editor: mapped.editor,
                revision: mapped.revision,
                replacement: Some(1..7),
                commit: Some("好".into()),
                ..Default::default()
            }
        )?,
        VectorInputResult::Delivered { text: true }
    );
    assert!(pending(&inner, &mut relay).forward()?);
    let edited = received(&first);
    assert_eq!(
        (edited.editor, edited.revision, edited.scene_revision),
        (41, 73, 2)
    );
    assert!(first.editor_event_targets(&edited, &a)?);
    assert!(!first.editor_event_targets(&edited, &b)?);
    edited.validate_context(&original).unwrap();
    let updated = Context {
        editor: 41,
        revision: 74,
        text: "a好z".into(),
        cursor: 4,
        anchor: 4,
        sensitive: false,
    };
    publish(&inner, &mut relay, &mut host, &a, 2, updated)?;
    send(&mut host, &text, "old");
    let delayed = pending(&inner, &mut relay);
    publish(&inner, &mut relay, &mut host, &a, 2, context(42, 1))?;
    assert!(
        !delayed.forward()?,
        "external editor replacement must revoke held input"
    );
    send(&mut host, &text, "scope-old");
    let old_scope = pending(&inner, &mut relay);
    let temporary = window_of(&first, 600., 0.)?;
    temporary.request_focus()?;
    a.request_focus()?;
    publish(&inner, &mut relay, &mut host, &a, 2, context(42, 2))?;
    assert!(
        !old_scope.forward()?,
        "republishing the same editor cannot lend a new inner focus grant"
    );
    temporary.close()?;
    send(&mut host, &text, "focus-old");
    let delayed = pending(&inner, &mut relay);
    let target = click(&mut host, &target, 270.);
    input(&inner, &mut relay)?;
    assert!(
        !delayed.forward()?,
        "focus loss must retire the bridge grant"
    );
    let peer_text = host.text_target(&target).unwrap();
    publish(&inner, &mut relay, &mut host, &b, 2, context(41, 73))?;
    send(&mut host, &peer_text, "peer");
    assert!(pending(&inner, &mut relay).forward()?);
    let peer = received(&second);
    assert_eq!(
        (peer.editor, peer.revision),
        (41, 73),
        "reused local editor IDs stay owner-scoped"
    );
    assert_eq!(peer.commit.as_deref(), Some("peer"));
    let mut peer_context = context(41, 74);
    peer_context.text = "a😀peerz".into();
    peer_context.cursor = 9;
    peer_context.anchor = 9;
    publish(&inner, &mut relay, &mut host, &b, 2, peer_context.clone())?;
    let masked = Context {
        editor: 43,
        revision: 1,
        text: String::new(),
        cursor: 0,
        anchor: 0,
        sensitive: true,
    };
    publish(&inner, &mut relay, &mut host, &b, 2, masked)?;
    let mapped = host.editor_state(&peer_text).unwrap().document.unwrap();
    assert!(mapped.sensitive && mapped.text.is_empty());
    assert_eq!(
        host.dispatch_document(
            &peer_text,
            &VectorDocumentEdit {
                editor: mapped.editor,
                revision: mapped.revision,
                selection: Some((0, 0)),
                ..Default::default()
            }
        )?,
        VectorInputResult::Rejected
    );
    send(&mut host, &peer_text, "masked");
    assert!(pending(&inner, &mut relay).forward()?);
    assert_eq!(received(&second).commit.as_deref(), Some("masked"));
    peer_context.editor = 44;
    peer_context.revision = 1;
    publish(&inner, &mut relay, &mut host, &b, 2, peer_context.clone())?;
    a.close()?;
    first.close()?;
    await_windows(&inner, &[1, 2], 1);
    project(&inner, &mut relay)?;
    let target = display(&mut host, &inner, &mut relay, 1);
    let peer_text = host.text_target(&target).unwrap();
    publish(&inner, &mut relay, &mut host, &b, 2, peer_context.clone())?;
    send(&mut host, &peer_text, "after-close");
    assert!(pending(&inner, &mut relay).forward()?);
    assert_eq!(received(&second).commit.as_deref(), Some("after-close"));
    peer_context.revision += 1;
    publish(&inner, &mut relay, &mut host, &b, 2, peer_context.clone())?;
    send(&mut host, &peer_text, "old-session");
    let old_session = pending(&inner, &mut relay);
    let projection = snapshot(&inner, &[1, 2]).bridge_projection();
    relay.replace_session(&projection.surfaces, &projection.sources, &projection.nodes)?;
    assert!(
        !old_session.forward()?,
        "outer session replacement must retire drained input"
    );
    project(&inner, &mut relay)?;
    let target = display(&mut host, &inner, &mut relay, 1);
    let target = click(&mut host, &target, 270.);
    input(&inner, &mut relay)?;
    let peer_text = host.text_target(&target).unwrap();
    peer_context.revision += 1;
    publish(&inner, &mut relay, &mut host, &b, 2, peer_context.clone())?;
    send(&mut host, &peer_text, "expired");
    let expired = pending(&inner, &mut relay);
    std::thread::sleep(Duration::from_millis(1100));
    assert!(
        !expired.forward()?,
        "a slow consumer cannot refresh the native input deadline"
    );
    peer_context.revision += 1;
    publish(&inner, &mut relay, &mut host, &b, 2, peer_context)?;
    send(&mut host, &peer_text, "after-cancel");
    let delayed = pending(&inner, &mut relay);
    relay.cancel_handle().cancel();
    assert!(
        !delayed.forward()?,
        "cancellation must revoke already drained input"
    );
    assert!(!format!("{edited:?}").contains("a😀éz"));
    Ok(())
}

#[test]
fn document_negotiation_requires_both_a_capable_physical_host_and_an_opted_in_bridge()
-> io::Result<()> {
    let (inner, _) = overlay_presenter()?;
    let inner = Arc::new(inner);
    inner.enable_overlay_host_relay();
    let (generic, endpoint) = overlay_presenter()?;
    let bridge = OuterBridge::builder(
        Secret32::from_hex(&pane(&generic, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .control_endpoint(endpoint)
    .overlay_document_editing(&inner)
    .build()?;
    assert!(
        !bridge
            .overlay_host_profiles()
            .iter()
            .any(|profile| profile == registry::OVERLAY_EDITOR)
    );
    inner.set_overlay_document_host_profiles(&bridge.overlay_host_profiles());
    inner.set_overlay_host_profiles(&[registry::OVERLAY_EDITOR.into()]);
    let producer = connect(&inner, 1)?;
    let window = window_of(&producer, 0., 0.)?;
    assert!(window.set_text_context(1, Some(context(1, 1))).is_err());
    let listener = TcpPresenterListener::bind()?;
    let endpoint = listener.endpoint();
    let mut config = PresenterConfig::terminal_with_overlay(MediaConfig::default());
    config
        .supported_profiles
        .push(registry::OVERLAY_EDITOR.into());
    let outer = Arc::new(VirtualVivid::start_configured_eventless(listener, config)?);
    let _host = NativeVectorHost::attach(Arc::clone(&outer), "")?;
    let bridge = super::bridge(&endpoint, &pane(&outer, 9)?)?;
    assert!(
        !bridge
            .overlay_host_profiles()
            .iter()
            .any(|profile| profile == registry::OVERLAY_EDITOR),
        "legacy mux consumers must not negotiate a typed channel they cannot carry"
    );
    Ok(())
}
