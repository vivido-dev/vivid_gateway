fn audit_bridge() -> OuterBridge {
    OuterBridge::from_session(
        vivid_sdk::Session::connect(ProducerConfig::offline()).unwrap(),
        Secret32::new([7; 32]),
        None,
        None,
        None,
        None,
        DisplayMetrics::default(),
    )
    .unwrap()
}

#[test]
fn unknown_sources_cannot_retain_partial_records() {
    let mut bridge = audit_bridge();
    for producer in 1..=100 {
        let key = BridgeSourceKey {
            producer,
            context: 1,
            surface: 1,
            track: 1,
        };
        assert!(
            bridge
                .media_chunk(1, key, messages::RASTER_FRAME, 0, 1024, false, vec![])
                .is_err()
        );
    }
    assert!(bridge.pending.is_empty());
    assert!(bridge.tracks.is_empty());
    assert_eq!(
        bridge
            .pending
            .values()
            .map(|p| p.bytes.capacity())
            .sum::<usize>(),
        0
    );
}

#[test]
fn delta_recovery_waits_for_full_frame() {
    let (_session, mut writer) = raster_writer(1);
    writer
        .forward_media(messages::RASTER_FRAME, &full_frame_body(1))
        .unwrap();
    let pixels = [0xaa, 0xbb, 0xcc, 0xff];
    let skipped = media::raster_delta_frame_body(
        1,
        2,
        1,
        0,
        0,
        RASTER_WIDTH,
        RASTER_HEIGHT,
        INNER_DELTA_OPERATIONS,
        &[
            RasterDeltaOperation::Overwrite {
                x: 0,
                y: 0,
                width: 1,
                height: 1,
                rgba: &pixels,
            },
            RasterDeltaOperation::Overwrite {
                x: 1,
                y: 0,
                width: 1,
                height: 1,
                rgba: &pixels,
            },
        ],
        false,
    )
    .unwrap();
    assert!(
        writer
            .forward_media(messages::RASTER_FRAME, &skipped)
            .is_err()
    );
    assert!(writer.needs_full_frame);
    writer.needs_full_frame = false; // The worker resets this before each command.
    // Frame 3 depends on omitted frame 2 and must wait for a new full frame.
    assert!(
        writer
            .forward_media(messages::RASTER_FRAME, &delta_body(3, 2))
            .is_err()
    );
    writer
        .forward_media(messages::RASTER_FRAME, &full_frame_body(4))
        .unwrap();
    writer
        .forward_media(messages::RASTER_FRAME, &delta_body(5, 4))
        .unwrap();
}

#[test]
fn failed_node_delete_retains_retry_identity() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let mut bridge = OuterBridge::connect(
        presenter.endpoint().into(),
        Zeroizing::new(vivid_sdk::testing::ROOT_SECRET_HEX.into()),
        DisplayMetrics::default(),
    )
    .unwrap();
    let key = BridgeSurfaceKey {
        producer: 1,
        context: 1,
        surface: 1,
    };
    let surface = BridgeSurface {
        key,
        overlay_window: None,
            overlay_layouts: Vec::new(),
        logical_width: 16,
        logical_height: 16,
        capture_policy: 0,
        descriptor: crate::BridgeSourceDescriptor {
            role: 1,
            title: "audit".into(),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let node = BridgeNode {
        producer: 1,
        node: 1,
        fragment: 0,
        surface: key,
        x: 0,
        y: 0,
        width: 16,
        height: 16,
        z_index: 0,
        visible: true,
        clip: crate::BridgeClipRect {
            x: 0,
            y: 0,
            width: 16,
            height: 16,
        },
    };
    bridge.rebuild(&[surface], &[], &[node]).unwrap();
    assert_eq!(bridge.nodes.len(), 1);
    drop(presenter);
    assert!(bridge.update_nodes(&[]).is_err());
    assert_eq!(bridge.nodes.len(), 1);
    assert!(bridge.update_nodes(&[]).is_err());
}

#[test]
fn overlay_window_relays_create_update_and_teardown() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let mut bridge = OuterBridge::connect(
        presenter.endpoint().into(),
        Zeroizing::new(vivid_sdk::testing::ROOT_SECRET_HEX.into()),
        DisplayMetrics::default(),
    )
    .unwrap();
    let surface_key = BridgeSurfaceKey {
        producer: 1,
        context: 1,
        surface: 1,
    };
    let source_key = BridgeSourceKey {
        producer: 1,
        context: 1,
        surface: 1,
        track: 1,
    };
    let window = |revision, x, y, width, height| BridgeOverlayWindow {
        parent: None,
        min_width: 1,
        min_height: 1,
        offset_x: 0,
        offset_y: 0,
        generation: 1,
        revision,
        x,
        y,
        width,
        height,
        mode: 0,
        visible: true,
    };
    let surface = BridgeSurface {
        key: surface_key,
        overlay_window: Some(window(1, 10, 10, 200, 100)),
        overlay_layouts: Vec::new(),
        logical_width: 200,
        logical_height: 100,
        capture_policy: 0,
        descriptor: crate::BridgeSourceDescriptor {
            role: 1,
            title: "overlay".into(),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let source = BridgeSource {
        key: source_key,
        kind: BridgeSourceKind::VectorScene {
            width: 200,
            height: 100,
            maximum_scene_bytes: 65536,
        },
        decoder_reset_serial: 1,
        live: true,
        active: false,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: false,
        play_request: default_play_request(),
        eos_epoch: None,
        causation_id: None,
    };

    bridge
        .rebuild(
            std::slice::from_ref(&surface),
            std::slice::from_ref(&source),
            &[],
        )
        .unwrap();
    let count_windows = |presenter: &vivid_sdk::testing::TestPresenter| {
        presenter
            .observed()
            .into_iter()
            .filter(|record| record.record_type == messages::SET_OVERLAY_WINDOW)
            .count()
    };
    assert_eq!(
        count_windows(&presenter),
        1,
        "the outer session receives one SET_OVERLAY_WINDOW on create"
    );
    assert_eq!(bridge.overlay_windows.len(), 1);

    let moved = BridgeSurface {
        overlay_window: Some(window(2, 50, 60, 220, 120)),
        ..surface.clone()
    };
    bridge
        .rebuild(
            std::slice::from_ref(&moved),
            std::slice::from_ref(&source),
            &[],
        )
        .unwrap();
    assert_eq!(
        count_windows(&presenter),
        2,
        "moved bounds relay a second SET_OVERLAY_WINDOW"
    );
    let tracked = bridge
        .overlay_windows
        .get(&surface_key)
        .expect("tracked window state");
    assert_eq!(tracked.projected.revision, 2, "the relayed inner revision advances");
    assert!(
        tracked.outer_revision > 0,
        "the outer presenter's own revision was recorded"
    );

    // An unchanged inner revision must not re-send the same geometry.
    bridge
        .rebuild(std::slice::from_ref(&moved), std::slice::from_ref(&source), &[])
        .unwrap();
    assert_eq!(
        count_windows(&presenter),
        2,
        "an unchanged revision does not re-send the window"
    );

    bridge.rebuild(&[], &[], &[]).unwrap();
    assert!(
        bridge.overlay_windows.is_empty(),
        "removing the surface clears its tracked window state"
    );
}

#[test]
fn poll_preserves_terminal_connection_event() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let mut bridge = OuterBridge::connect(
        presenter.endpoint().into(),
        Zeroizing::new(vivid_sdk::testing::ROOT_SECRET_HEX.into()),
        DisplayMetrics::default(),
    )
    .unwrap();
    drop(presenter);
    // Allow the SDK dispatcher to publish the terminal event; poll consumes it.
    for _ in 0..20 {
        bridge.poll_outer_session();
        thread::sleep(Duration::from_millis(5));
    }
    assert!(bridge.service_session_events().is_err());
    assert!(bridge.service_session_events().is_err());
}

fn audit_projection(producer: u64) -> (BridgeSurface, BridgeSource) {
    let key = BridgeSurfaceKey {
        producer,
        context: 1,
        surface: 1,
    };
    let surface = BridgeSurface {
        key,
        overlay_window: None,
            overlay_layouts: Vec::new(),
        logical_width: 16,
        logical_height: 16,
        capture_policy: 0,
        descriptor: crate::BridgeSourceDescriptor {
            role: 1,
            title: "audit".into(),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let source = BridgeSource {
        key: BridgeSourceKey {
            producer,
            context: 1,
            surface: 1,
            track: 1,
        },
        kind: BridgeSourceKind::Raster {
            width: 16,
            height: 16,
            alpha_mode: 1,
            compression_mode: 0,
            delta_operation_limit: None,
        },
        decoder_reset_serial: 1,
        live: true,
        active: false,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: false,
        play_request: default_play_request(),
        eos_epoch: None,
        causation_id: None,
    };
    (surface, source)
}

#[test]
fn assembly_checks_bounds_delivery_and_isolates_owners() {
    let mut bridge = audit_bridge();
    let (a, x) = audit_projection(1);
    let (b, y) = audit_projection(2);
    bridge
        .rebuild(&[a, b], &[x.clone(), y.clone()], &[])
        .unwrap();
    for key in [x.key, y.key] {
        assert!(
            !bridge
                .media_chunk(7, key, messages::RASTER_FRAME, 0, 100, false, vec![0])
                .unwrap()
        );
    }
    assert!(
        bridge
            .media_chunk(8, x.key, messages::RASTER_FRAME, 1, 100, false, vec![0])
            .is_err()
    );
    assert!(!bridge.pending.contains_key(&x.key));
    assert_eq!(bridge.pending[&y.key].received, 1);
    for total in [0, u32::MAX, 16 * 16 * 4 + 1000] {
        assert!(
            bridge
                .media_chunk(9, x.key, messages::RASTER_FRAME, 0, total, false, vec![0])
                .is_err()
        );
    }
    assert!(
        bridge
            .media_chunk(9, x.key, messages::VIDEO_PACKET, 0, 100, false, vec![0])
            .is_err()
    );
    assert!(
        bridge
            .media_chunk(9, x.key, messages::RASTER_FRAME, 0, 1, false, vec![0])
            .is_err()
    );
    assert_eq!(bridge.pending.len(), 1);
    bridge.pending.get_mut(&y.key).unwrap().started = Instant::now() - PENDING_TIMEOUT;
    assert!(
        !bridge
            .media_chunk(10, y.key, messages::RASTER_FRAME, 0, 100, false, vec![0])
            .unwrap()
    );
    assert_eq!(bridge.pending[&y.key].delivery_id, 10);
    // Charge the existing assembly to the aggregate cap without allocating a large buffer.
    bridge.pending.get_mut(&y.key).unwrap().total = MAX_PENDING_BYTES;
    assert!(
        bridge
            .media_chunk(11, x.key, messages::RASTER_FRAME, 0, 100, false, vec![0])
            .is_err()
    );
    assert_eq!(bridge.pending.len(), 1);
}

#[test]
fn failed_channel_setup_destroys_allocated_track_before_retry() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let authentication = Secret32::from_hex(vivid_sdk::testing::ROOT_SECRET_HEX).unwrap();
    let unavailable = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let unavailable_endpoint = format!("tcp:{}", unavailable.local_addr().unwrap());
    drop(unavailable);
    let config = producer_config(
        Some(presenter.endpoint().into()),
        None,
        Some(unavailable_endpoint),
        &authentication,
        registry::TERMINAL_SURFACE,
    )
    .unwrap();
    let session = vivid_sdk::Session::connect(config).unwrap();
    let mut bridge = OuterBridge::from_session(
        session,
        authentication,
        None,
        None,
        None,
        None,
        DisplayMetrics::default(),
    )
    .unwrap();
    let (surface, source) = audit_projection(1);
    for count in 1..=3 {
        assert!(
            bridge
                .rebuild(
                    std::slice::from_ref(&surface),
                    std::slice::from_ref(&source),
                    &[]
                )
                .is_err()
        );
        assert!(bridge.tracks.is_empty());
        assert!(bridge.unfinished_tracks.is_empty());
        assert_eq!(presenter.destroys().len(), count);
    }
}

#[test]
fn replacement_cancels_old_session_and_retires_notifications() {
    let old = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let new = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let mut bridge = OuterBridge::connect(
        old.endpoint().into(),
        Zeroizing::new(vivid_sdk::testing::ROOT_SECRET_HEX.into()),
        DisplayMetrics::default(),
    )
    .unwrap();
    let (a, x) = audit_projection(1);
    let (b, y) = audit_projection(2);
    let surfaces = vec![a, b];
    let sources = vec![x, y];
    bridge.rebuild(&surfaces, &sources, &[]).unwrap();
    old.script().drop_reply(messages::GOODBYE, 1);
    bridge.endpoint_control = Some(new.endpoint().into());
    bridge.endpoint_bulk = Some(new.endpoint().into());
    bridge.endpoint_realtime = Some(new.endpoint().into());
    for producer in [1, 2] {
        let (_, source) = audit_projection(producer);
        bridge.losses.insert(source.key);
        bridge.full_frames.insert(source.key);
        bridge.keyframes.push(BridgeKeyframeRequest {
            source: source.key,
            minimum_epoch: Some(7),
            reason: 1,
        });
        bridge.playback.push((
            source.key,
            PlaybackSnapshot {
                decoder_reset_serial: source.decoder_reset_serial,
                state: 1,
                eos_state: 1,
            },
        ));
    }
    let (send, receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = bridge.replace_session(&surfaces, &sources, &[]);
        send.send((bridge, result)).unwrap();
    });
    let result = receive.recv_timeout(Duration::from_secs(3));
    drop(old); // Also releases a regressed implementation before joining the worker.
    worker.join().unwrap();
    let (mut bridge, result) = result.expect("replacement must not wait for GOODBYE");
    result.unwrap();
    assert!(bridge.take_source_losses().is_empty());
    assert!(bridge.take_full_frame_requests().is_empty());
    assert!(bridge.take_keyframe_requests().is_empty());
    assert!(bridge.take_playback_states().is_empty());
    assert_eq!(bridge.diagnostic_instance_generation(), 2);
    assert_eq!(bridge.tracks.len(), 2);
    assert_eq!(bridge.surfaces.len(), 2);
}

#[test]
fn polling_retains_resize_for_service_consumer() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let mut bridge = OuterBridge::connect(
        presenter.endpoint().into(),
        Zeroizing::new(vivid_sdk::testing::ROOT_SECRET_HEX.into()),
        DisplayMetrics::default(),
    )
    .unwrap();
    presenter.resize_terminal(100, 30, true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while !bridge.poll_outer_session() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(bridge.display_metrics().columns, 100);
    assert_eq!(bridge.service_session_events().unwrap().unwrap().rows, 30);
    assert!(bridge.service_session_events().unwrap().is_none());
}

#[test]
fn outer_sessions_never_require_overlay_profiles_and_offer_only_the_relayed_subset() {
    // The gateway relays a nested producer's overlay window to the outer presenter rather than
    // rendering it itself, so an outer presenter with no vector renderer must keep working exactly
    // as it does today: none of these profiles may ever be required. `terminal-surface-v1` may
    // optionally offer window/scene/input and physical host services. Each remains optional;
    // the inner presenter only advertises clipboard/a11y/typography when the host accepted them.
    // `desktop-surface-v1` cannot offer any of them: `terminal-overlay-v1`'s prerequisite excludes it.
    let relayed_under_terminal = [
        registry::VECTOR_SCENE,
        registry::TERMINAL_OVERLAY,
        registry::OVERLAY_INPUT,
        registry::OVERLAY_PAINT,
        registry::OVERLAY_POINTER,
        registry::OVERLAY_TEXT,
        registry::OVERLAY_TEXT_LAYOUT,
        registry::OVERLAY_ENV,
        registry::OVERLAY_TYPOGRAPHY,
        registry::OVERLAY_CLIPBOARD,
        registry::OVERLAY_A11Y,
    ];
    for target in [registry::TERMINAL_SURFACE, registry::DESKTOP_SURFACE] {
        let config = producer_config(
            Some("tcp:127.0.0.1:1".into()),
            None,
            None,
            &Secret32::new([3; 32]),
            target,
        )
        .expect("gateway outer configuration");
        for profile in &relayed_under_terminal {
            assert!(
                !config.required_profiles.iter().any(|p| p == profile),
                "{target} must not require {profile}"
            );
        }
        for profile in relayed_under_terminal {
            let offered = config.optional_profiles.iter().any(|p| p == profile);
            assert_eq!(
                offered,
                target == registry::TERMINAL_SURFACE,
                "{target} offering {profile} should match whether it can host an overlay window"
            );
        }
    }
}
