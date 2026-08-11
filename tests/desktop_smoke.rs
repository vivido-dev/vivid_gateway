use std::io;
use std::net::{Shutdown, TcpListener};
use std::sync::Arc;

use vivid_gateway::{
    BridgeClipRect, BridgeNode, BridgeSourceDescriptor, BridgeSurface, BridgeSurfaceKey,
    ConnectionCancel, DisplayMetrics, MediaConfig, OuterBridge, PresenterConfig, PresenterListener,
    Transport, VirtualVivid,
};
use vivid_protocol::auth::{
    Secret32, channel_tag, derive_session_keys, extract_handshake_prk, verify_tag,
};
use vivid_protocol::cbor::Value;
use vivid_protocol::geometry::Rotation;
use vivid_protocol::media;
use vivid_protocol::messages;
use vivid_protocol::surface::POLICY_DENY_CAPTURE;
use vivid_protocol::target::{DesktopTarget, OutputDescriptor};
use vivid_sdk::testing::{ROOT_SECRET_HEX, TestPresenter};
use vivid_sdk::{
    CoordinateModel, Fit, KindConfiguration, LaneClass, ProducerAuthentication, ProducerConfig,
    RasterConfiguration, RequestMetadata, SceneNode, Session, SurfaceDefinition, SurfaceDescriptor,
    SurfaceRole, TrackConfiguration, TrackMode,
};
use zeroize::Zeroizing;

struct TcpPresenterListener {
    listener: TcpListener,
    endpoint: String,
}

impl TcpPresenterListener {
    fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("tcp:{}", listener.local_addr()?);
        Ok(Self { listener, endpoint })
    }
}

impl PresenterListener for TcpPresenterListener {
    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn accept(&self) -> io::Result<Transport> {
        let (stream, _) = self.listener.accept()?;
        stream.set_nodelay(true)?;
        let reader = stream.try_clone()?;
        let timeout_stream = stream.try_clone()?;
        let cancel_stream = stream.try_clone()?;
        Ok(Transport::new(
            Box::new(reader),
            Box::new(stream),
            ConnectionCancel::new(move || {
                let _ = cancel_stream.shutdown(Shutdown::Both);
            }),
            Arc::new(move |timeout| timeout_stream.set_read_timeout(timeout)),
        ))
    }
}

fn desktop_target(width: u32, height: u32) -> DesktopTarget {
    DesktopTarget {
        origin_x: 0,
        origin_y: 0,
        width,
        height,
        outputs: vec![OutputDescriptor {
            output_id: 1,
            origin_x: 0,
            origin_y: 0,
            width,
            height,
            scale_numerator: 1,
            scale_denominator: 1,
            rotation: Rotation::None,
            primary: true,
        }],
        settled: true,
        topology_revision: 1,
    }
}

fn connect_test_presenter(presenter: &TestPresenter, name: &str) -> io::Result<Session> {
    let mut producer = ProducerConfig::desktop();
    producer.endpoint_control = Some(presenter.endpoint().into());
    producer.authentication = ProducerAuthentication::Root {
        root_secret: Secret32::from_hex(ROOT_SECRET_HEX).map_err(io::Error::other)?,
    };
    producer.producer_name = name.into();
    Session::connect(producer)
}

/// The B10 neutrality scenario. The exact same producer-side operations are used for a direct
/// presenter and the inner side of the terminating gateway.
fn present_desktop_scene(session: &mut Session, title: &str) -> io::Result<()> {
    let context = session.info().root_context_id;
    let surface = session.create_surface(
        SurfaceDefinition {
            context_id: context,
            surface_id: 1,
            semantic_profile: vivid_sdk::GENERIC_CONTENT.into(),
            coordinate_model: CoordinateModel::DesktopLogicalPixels,
            logical_width: 1280,
            logical_height: 720,
            scale_numerator: 1,
            scale_denominator: 1,
            rotation: 0,
            descriptor: SurfaceDescriptor {
                role: SurfaceRole::Desktop,
                title: title.into(),
                semantic_content_revision: 1,
                semantic_availability: 0,
                locator_hint: String::new(),
            },
            policy: 0,
            profile_parameters: vec![],
        },
        &RequestMetadata::default(),
    )?;
    session.create_node(
        &SceneNode {
            owning_context_id: context,
            node_id: 1,
            surface_context_id: context,
            surface_id: surface.id(),
            geometry: vec![
                (0, Value::Unsigned(1)),
                (1, Value::Unsigned(0)),
                (2, Value::Unsigned(0)),
                (3, Value::Unsigned(1280)),
                (4, Value::Unsigned(720)),
            ],
            fit: Fit::Contain,
            linear_sampling: true,
            z_index: 0,
            visible: true,
            opacity: u16::MAX,
            clip: Some(vec![
                (0, Value::Unsigned(0)),
                (1, Value::Unsigned(0)),
                (2, Value::Unsigned(1280)),
                (3, Value::Unsigned(720)),
            ]),
        },
        &RequestMetadata::default(),
    )?;
    let maximum_record_body =
        media::rgba8_raw_frame_body_len(1280, 720).map_err(io::Error::other)?;
    session.create_track(
        TrackConfiguration {
            context_id: context,
            surface_id: surface.id(),
            track_id: 1,
            slot: 3,
            mode: TrackMode::Live,
            lane: LaneClass::Bulk,
            maximum_record_body,
            maximum_rate_millihertz: 30_000,
            maximum_encoded_bits_per_second: 1_000_000_000,
            maximum_records_per_second: 30,
            maximum_inflight_body_bytes: u64::from(maximum_record_body) * 2,
            kind: KindConfiguration::Raster(RasterConfiguration {
                width: 1280,
                height: 720,
                alpha_mode: 1,
                delta_enabled: false,
                maximum_delta_operations: 1,
                zstd_enabled: false,
            }),
            target_latency_us: 16_000,
            maximum_latency_us: 100_000,
            retained_pixel_charge: 1280 * 720,
        },
        &RequestMetadata::default(),
    )?;
    Ok(())
}

fn normalized_scene_records(
    records: &[vivid_sdk::testing::Observed],
) -> Vec<(u16, vivid_protocol::messages::PayloadMap)> {
    records
        .iter()
        .filter(|record| {
            matches!(
                record.record_type,
                messages::CREATE_SURFACE
                    | messages::BEGIN_TXN
                    | messages::CREATE_NODE
                    | messages::COMMIT_TXN
            )
        })
        .map(|record| {
            let identity_keys: &[u64] = match record.record_type {
                messages::CREATE_SURFACE => &[0, 1],
                messages::BEGIN_TXN => &[0, 1],
                messages::CREATE_NODE => &[0, 1, 2, 3],
                _ => &[],
            };
            (
                record.record_type,
                record
                    .payload
                    .iter()
                    .filter(|(key, _)| !identity_keys.contains(key))
                    .cloned()
                    .collect(),
            )
        })
        .collect()
}

#[test]
fn identical_desktop_script_is_record_neutral_across_the_terminating_gateway() -> io::Result<()> {
    let direct_presenter = TestPresenter::start_desktop(1280, 720)?;
    let mut direct = connect_test_presenter(&direct_presenter, "neutrality-direct")?;
    present_desktop_scene(&mut direct, "neutral desktop")?;
    let direct_records = normalized_scene_records(&direct_presenter.observed());

    let listener = TcpPresenterListener::bind()?;
    let inner = VirtualVivid::start_configured(
        listener,
        PresenterConfig::desktop(MediaConfig::default(), desktop_target(1280, 720)),
        None,
    )?;
    let principal = 71;
    let root = inner.issue_pane_capability(principal)?;
    let mut producer = ProducerConfig::desktop();
    producer.endpoint_control = Some(inner.endpoint());
    producer.authentication = ProducerAuthentication::Root {
        root_secret: Secret32::from_hex(&root).map_err(io::Error::other)?,
    };
    producer.producer_name = "neutrality-gateway".into();
    let mut through_gateway = Session::connect(producer)?;
    present_desktop_scene(&mut through_gateway, "neutral desktop")?;

    let projection = inner
        .projection_snapshot(&[principal].into_iter().collect())
        .bridge_projection();
    let browser_presenter = TestPresenter::start_desktop(1280, 720)?;
    let mut bridge = OuterBridge::connect_native_for_target(
        browser_presenter.endpoint().into(),
        None,
        None,
        Zeroizing::new(ROOT_SECRET_HEX.to_owned()),
        vivid_sdk::DESKTOP_SURFACE,
        DisplayMetrics::default(),
    )?;
    bridge.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    let browser_records = normalized_scene_records(&browser_presenter.observed());

    assert_eq!(browser_records, direct_records);
    assert_eq!(
        direct_records
            .iter()
            .map(|record| record.0)
            .collect::<Vec<_>>(),
        vec![
            messages::CREATE_SURFACE,
            messages::BEGIN_TXN,
            messages::CREATE_NODE,
            messages::COMMIT_TXN,
        ]
    );
    through_gateway.close()?;
    direct.close()?;
    Ok(())
}

#[test]
fn terminating_hops_derive_independent_channel_authenticators() -> io::Result<()> {
    let listener = TcpPresenterListener::bind()?;
    let inner = VirtualVivid::start_configured(
        listener,
        PresenterConfig::desktop(MediaConfig::default(), desktop_target(1280, 720)),
        None,
    )?;
    let inner_root =
        Secret32::from_hex(&inner.issue_pane_capability(72)?).map_err(io::Error::other)?;
    let outer_root = Secret32::from_hex(ROOT_SECRET_HEX).map_err(io::Error::other)?;
    assert_ne!(inner_root.expose(), outer_root.expose());

    let derive = |root: &Secret32| {
        let prk = extract_handshake_prk(root, &[1; 32], &[2; 32], &[0; 32]);
        derive_session_keys(&prk, 1, 1, &[3; 16]).0
    };
    let inner_keys = derive(&inner_root);
    let outer_keys = derive(&outer_root);
    let inner_tag = channel_tag(inner_keys.channel_key(), 1, 1, 1, 1, 1, 1, 4, &[4; 16]);
    let outer_tag = channel_tag(outer_keys.channel_key(), 1, 1, 1, 1, 1, 1, 4, &[4; 16]);
    assert!(verify_tag(&inner_tag, &inner_tag));
    assert!(verify_tag(&outer_tag, &outer_tag));
    assert!(!verify_tag(&inner_tag, &outer_tag));
    assert!(!verify_tag(&outer_tag, &inner_tag));
    Ok(())
}

#[test]
fn desktop_surface_crosses_both_terminating_hops() -> io::Result<()> {
    let listener = TcpPresenterListener::bind()?;
    let inner = VirtualVivid::start_configured(
        listener,
        PresenterConfig::desktop(MediaConfig::default(), desktop_target(1280, 720)),
        None,
    )?;
    let principal = 7;
    let root = inner.issue_pane_capability(principal)?;

    let mut producer = ProducerConfig::desktop();
    producer.endpoint_control = Some(inner.endpoint());
    producer.authentication = ProducerAuthentication::Root {
        root_secret: Secret32::from_hex(&root).map_err(io::Error::other)?,
    };
    producer.producer_name = "gateway-desktop-smoke-inner".into();
    let mut session = vivid_sdk::Session::connect(producer)?;
    assert_eq!(session.info().target_profile, vivid_sdk::DESKTOP_SURFACE);

    let context = session.info().root_context_id;
    let surface = session.create_surface(
        SurfaceDefinition {
            context_id: context,
            surface_id: 1,
            semantic_profile: vivid_sdk::GENERIC_CONTENT.into(),
            coordinate_model: CoordinateModel::DesktopLogicalPixels,
            logical_width: 1280,
            logical_height: 720,
            scale_numerator: 1,
            scale_denominator: 1,
            rotation: 0,
            descriptor: SurfaceDescriptor {
                role: SurfaceRole::Desktop,
                title: "sanitized desktop".into(),
                semantic_content_revision: 1,
                semantic_availability: 0,
                locator_hint: String::new(),
            },
            policy: 0,
            profile_parameters: vec![],
        },
        &RequestMetadata::default(),
    )?;

    let projection = inner.projection_snapshot(&[principal].into_iter().collect());
    assert_eq!(projection.surfaces.len(), 1);
    let projected = &projection.surfaces[0];
    assert_eq!(projected.surface, surface.id());

    let outer_presenter = TestPresenter::start_desktop(1280, 720)?;
    let mut bridge = OuterBridge::connect_native_for_target(
        outer_presenter.endpoint().into(),
        None,
        None,
        Zeroizing::new(ROOT_SECRET_HEX.to_owned()),
        vivid_sdk::DESKTOP_SURFACE,
        DisplayMetrics::default(),
    )?;
    assert_eq!(bridge.target_profile(), vivid_sdk::DESKTOP_SURFACE);
    bridge.set_enforced_surface_policy(POLICY_DENY_CAPTURE)?;

    let bridge_surface = BridgeSurfaceKey {
        producer: projected.producer,
        context: projected.context,
        surface: projected.surface,
    };
    bridge.rebuild(
        &[BridgeSurface {
            key: bridge_surface,
            logical_width: projected.logical_width,
            logical_height: projected.logical_height,
            capture_policy: projected.capture_policy,
            descriptor: BridgeSourceDescriptor {
                role: projected.semantic_descriptor.role,
                title: projected.semantic_descriptor.title.clone(),
                content_revision: projected.semantic_descriptor.content_revision,
                semantic_availability: projected.semantic_descriptor.semantic_availability,
                locator: projected.semantic_descriptor.locator.clone(),
            },
        }],
        &[],
        &[BridgeNode {
            producer: projected.producer,
            node: 1,
            fragment: 0,
            surface: bridge_surface,
            x: 0,
            y: 0,
            width: 1280,
            height: 720,
            z_index: 0,
            visible: true,
            clip: BridgeClipRect {
                x: 0,
                y: 0,
                width: 1280,
                height: 720,
            },
        }],
    )?;

    assert!(bridge.outer_surface_id(bridge_surface).is_some());
    let observed = outer_presenter.observed();
    let create = observed
        .iter()
        .find(|record| record.record_type == messages::CREATE_SURFACE)
        .expect("outer presenter must observe the re-originated surface");
    assert_eq!(
        create
            .payload
            .iter()
            .find(|entry| entry.0 == 10)
            .and_then(|entry| entry.1.as_u64()),
        Some(POLICY_DENY_CAPTURE)
    );
    let create_node = observed
        .iter()
        .find(|record| record.record_type == messages::CREATE_NODE)
        .expect("outer presenter must observe the re-originated desktop node");
    let geometry_value = create_node
        .payload
        .iter()
        .find(|entry| entry.0 == 4)
        .map(|entry| &entry.1)
        .expect("desktop node has geometry");
    let vivid_protocol::cbor::Value::Map(geometry) = geometry_value else {
        panic!("desktop node geometry must be a map");
    };
    assert_eq!(
        geometry.len(),
        5,
        "desktop geometry must not fabricate terminal layer key 5"
    );
    session.close()?;
    Ok(())
}

#[test]
fn identical_inner_local_ids_map_to_distinct_outer_objects() -> io::Result<()> {
    let listener = TcpPresenterListener::bind()?;
    let inner = VirtualVivid::start_configured(
        listener,
        PresenterConfig::desktop(MediaConfig::default(), desktop_target(1280, 720)),
        None,
    )?;
    let mut sessions = Vec::new();
    for principal in [7_u64, 8] {
        let root = inner.issue_pane_capability(principal)?;
        let mut producer = ProducerConfig::desktop();
        producer.endpoint_control = Some(inner.endpoint());
        producer.authentication = ProducerAuthentication::Root {
            root_secret: Secret32::from_hex(&root).map_err(io::Error::other)?,
        };
        producer.producer_name = format!("gateway-owner-{principal}");
        let mut session = vivid_sdk::Session::connect(producer)?;
        let context = session.info().root_context_id;
        session.create_surface(
            SurfaceDefinition {
                context_id: context,
                surface_id: 1,
                semantic_profile: vivid_sdk::GENERIC_CONTENT.into(),
                coordinate_model: CoordinateModel::DesktopLogicalPixels,
                logical_width: 1280,
                logical_height: 720,
                scale_numerator: 1,
                scale_denominator: 1,
                rotation: 0,
                descriptor: SurfaceDescriptor {
                    role: SurfaceRole::Desktop,
                    title: format!("owner-{principal}"),
                    semantic_content_revision: 1,
                    semantic_availability: 0,
                    locator_hint: String::new(),
                },
                policy: 0,
                profile_parameters: vec![],
            },
            &RequestMetadata::default(),
        )?;
        sessions.push(session);
    }

    let snapshot = inner.projection_snapshot(&[7_u64, 8].into_iter().collect());
    let projection = snapshot.bridge_projection();
    assert_eq!(projection.surfaces.len(), 2);
    assert!(
        projection
            .surfaces
            .iter()
            .all(|surface| surface.key.surface == 1)
    );
    assert_ne!(
        projection.surfaces[0].key.producer,
        projection.surfaces[1].key.producer
    );

    let outer_presenter = TestPresenter::start_desktop(1280, 720)?;
    let mut bridge = OuterBridge::connect_native_for_target(
        outer_presenter.endpoint().into(),
        None,
        None,
        Zeroizing::new(ROOT_SECRET_HEX.to_owned()),
        vivid_sdk::DESKTOP_SURFACE,
        DisplayMetrics::default(),
    )?;
    bridge.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    let first_key = projection.surfaces[0].key;
    let second_key = projection.surfaces[1].key;
    let first_outer = bridge.outer_surface_id(first_key).unwrap();
    let second_outer = bridge.outer_surface_id(second_key).unwrap();
    assert_ne!(first_outer, second_outer);

    bridge.rebuild(
        &[projection.surfaces[1].clone()],
        &projection.sources,
        &projection.nodes,
    )?;
    assert!(bridge.outer_surface_id(first_key).is_none());
    assert_eq!(bridge.outer_surface_id(second_key), Some(second_outer));
    for session in sessions {
        session.close()?;
    }
    Ok(())
}
