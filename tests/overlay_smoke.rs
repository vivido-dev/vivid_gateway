//! An overlay window crossing the terminating gateway, over real sockets, end to end.
//!
//! This is the arrangement a `vivid_ui` program gets inside a `vvmux` pane. A real producer speaks
//! the overlay bundle to an inner presenter that hosts windows without rasterizing them; the
//! projection that presenter publishes is re-originated by [`OuterBridge`] onto a second, equally
//! real overlay-hosting presenter, standing in for the terminal that finally composites. Nothing
//! here is a state snapshot passed between structures in one process: the window handshake, the
//! vector-scene channel and its credit, and the surface lifecycle all cross two authenticated
//! sessions on two sockets, because that is where ordering and liveness can actually go wrong.

use std::collections::HashSet;
use std::io;
use std::time::{Duration, Instant};

use vivid_gateway::{
    BridgeSurface, DisplayMetrics, MediaConfig, OuterBridge, PresenterConfig, PresenterListener,
    ProjectionSnapshot, VirtualVivid,
};
use vivid_protocol::cbor::Value;
use vivid_sdk::overlay::{Brush, Canvas, Color, Path, Rect, WindowMode};
use vivid_sdk::{
    CoordinateModel, Fit, KindConfiguration, LaneClass, OverlaySession, OverlayWindow,
    OverlayWindowOptions, ProducerAuthentication, ProducerConfig, RasterConfiguration,
    RequestMetadata, SceneNode, Session, SurfaceDefinition, SurfaceDescriptor, SurfaceRole,
    TrackConfiguration, TrackMode,
};
use zeroize::Zeroizing;

mod common;

use common::{TcpPresenterListener, pin_endpoints};

const CELL: (u16, u16) = (8, 16);
const COLUMNS: u16 = 80;
const ROWS: u16 = 24;

/// A presenter that hosts overlay windows without rasterizing them — an inner pane presenter, and
/// equally the outer one, since the point of the relay is that both ends speak the same protocol.
fn overlay_presenter() -> io::Result<(VirtualVivid, String)> {
    let listener = TcpPresenterListener::bind()?;
    let endpoint = listener.endpoint();
    let presenter = VirtualVivid::start_configured_eventless(
        listener,
        PresenterConfig::terminal_with_overlay(MediaConfig::default()),
    )?;
    Ok((presenter, endpoint))
}

fn pane(presenter: &VirtualVivid, pane: u64) -> io::Result<String> {
    presenter.update_metrics(pane, COLUMNS, ROWS, CELL);
    presenter.issue_pane_capability(pane)
}

fn overlay_producer(endpoint: &str, secret: &str, name: &str) -> io::Result<OverlaySession> {
    let mut config = ProducerConfig {
        authentication: ProducerAuthentication::root_hex(secret).map_err(io::Error::other)?,
        producer_name: name.into(),
        ..ProducerConfig::default()
    };
    pin_endpoints(&mut config, endpoint.to_owned());
    OverlaySession::connect(config)
}

fn scene(colour: u32) -> Canvas {
    let mut canvas = Canvas::new();
    canvas
        .fill(
            Path::rectangle(Rect::new(0., 0., 120., 60.).unwrap()).unwrap(),
            Brush::Solid(Color(colour)),
        )
        .unwrap();
    canvas
}

#[test]
fn overlay_display_lists_cross_both_sockets_and_return_credit_after_delivery() -> io::Result<()> {
    fn relaying_presenter() -> io::Result<(VirtualVivid, String)> {
        let listener = TcpPresenterListener::bind()?;
        let endpoint = listener.endpoint();
        Ok((
            VirtualVivid::start_configured(
                listener,
                PresenterConfig::terminal_with_overlay(MediaConfig::default()),
                None,
            )?,
            endpoint,
        ))
    }
    let (inner, endpoint) = relaying_presenter()?;
    let secret = pane(&inner, 1)?;
    let producer = overlay_producer(&endpoint, &secret, "drawing-relay")?;
    let window = window_of(&producer, 0., 0.)?;
    let (outer, endpoint) = relaying_presenter()?;
    let secret = pane(&outer, 9)?;
    let mut relay = bridge(&endpoint, &secret)?;
    for colour in [0xff0000ff, 0x00ff00ff, 0x0000ffff, 0xffffffff] {
        let mut canvas = scene(colour);
        canvas
            .push(vivid_sdk::overlay::Command::Hit {
                id: 7,
                path: Path::rectangle(Rect::new(0., 0., 120., 60.).unwrap()).unwrap(),
                role: vivid_sdk::overlay::HitRole::Input,
                cursor: None,
            })
            .unwrap();
        window.present(canvas.clone())?;
        let event = inner
            .wait_media_event(Duration::from_secs(5))?
            .expect("accepted scene must become an actual bridge delivery");
        assert!(inner.bridge_delivery_is_pending(event.delivery_id, event.source));
        let projection = snapshot(&inner, &[1]).bridge_projection();
        relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
        relay.media_chunk(
            event.delivery_id,
            event.source,
            event.record_type,
            0,
            event.body.len() as u32,
            true,
            event.body,
        )?;
        let painted = outer
            .wait_media_event(Duration::from_secs(5))?
            .expect("outer presenter must receive drawing commands");
        let frame = vivid_protocol::vector::Frame::decode(&painted.body).unwrap();
        assert_eq!(frame.canvas, canvas);
        outer.complete_bridge_delivery(painted.delivery_id, true);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let completions = relay.take_media_completions();
            if let Some((id, delivered, _, _)) = completions.first() {
                assert_eq!(*id, event.delivery_id);
                assert!(*delivered);
                inner.complete_bridge_delivery(*id, *delivered);
                break;
            }
            assert!(Instant::now() < deadline, "outer write never completed");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    assert!(outer.overlay_pointer(9, 20., 20., Some((1, true)), 0)?);
    assert!(outer.overlay_key(9, 0x2b, true, 0));
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut pointer = false;
    let mut keyboard = false;
    while !pointer || !keyboard {
        for (surface, body) in relay.take_overlay_input()? {
            assert!(inner.relay_overlay_input(surface, &body)?);
        }
        while let Some(event) = producer.wait_event(Duration::ZERO)? {
            if let vivid_sdk::OverlayLaneEvent::Input(event) = event {
                match event.event {
                    vivid_sdk::overlay::Event::Pointer {
                        button: Some((1, true)),
                        ..
                    } => pointer = true,
                    vivid_sdk::overlay::Event::Key {
                        physical: 0x2b,
                        down: true,
                        ..
                    } => keyboard = true,
                    _ => {}
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "native overlay input never reached the inner producer"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

#[test]
fn measured_layouts_and_image_assets_survive_a_replaced_outer_session() -> io::Result<()> {
    use vivid_sdk::overlay::{Point, StyledText, TextStyle};
    let listener = TcpPresenterListener::bind()?;
    let endpoint = listener.endpoint();
    let inner = VirtualVivid::start_configured(
        listener,
        PresenterConfig::terminal_with_overlay(MediaConfig::default()),
        None,
    )?;
    inner.enable_overlay_host_relay();
    let secret = pane(&inner, 1)?;
    let producer = overlay_producer(&endpoint, &secret, "retained-overlay")?;
    let window = window_of(&producer, 0., 0.)?;
    let (outer, endpoint) = overlay_presenter()?;
    let secret = pane(&outer, 9)?;
    // Occupy one outer layout ID so accidental identity passthrough cannot pass this test.
    let decoy = overlay_producer(&endpoint, &secret, "different-owner")?;
    let decoy_window = window_of(&decoy, 300., 0.)?;
    let _decoy_layout =
        decoy_window.layout_text(&StyledText::new("other", TextStyle::default()))?;
    let mut relay = bridge(&endpoint, &secret)?;
    let layout = std::thread::scope(|scope| -> io::Result<_> {
        let job = scope.spawn(|| {
            window.layout_text(&StyledText::new(
                "host-shaped paragraph",
                TextStyle::default(),
            ))
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while !job.is_finished() {
            for request in inner.take_overlay_host_requests() {
                let projection = snapshot(&inner, &[1]).bridge_projection();
                relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
                inner.complete_overlay_host_request(
                    request.id,
                    relay
                        .overlay_host_request(&request)
                        .map_err(|error| error.to_string()),
                );
            }
            assert!(Instant::now() < deadline, "text request did not complete");
            std::thread::sleep(Duration::from_millis(5));
        }
        job.join().unwrap()
    })?;
    let image = window.upload_rgba(1, 1, &[255, 0, 0, 255])?;
    let asset = inner
        .wait_media_event(Duration::from_secs(5))?
        .expect("asset delivery");
    let projection = snapshot(&inner, &[1]).bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    relay.media_chunk(
        asset.delivery_id,
        asset.source,
        asset.record_type,
        0,
        asset.body.len() as u32,
        true,
        asset.body,
    )?;
    finish_delivery(&inner, &mut relay, asset.delivery_id)?;
    let mut canvas = Canvas::new();
    window.draw_text_layout(&mut canvas, &layout, Point::new(5., 5.).unwrap())?;
    window.draw_image(
        &mut canvas,
        &image,
        Rect::new(0., 0., 10., 10.).unwrap(),
        u16::MAX,
    )?;
    window.present(canvas)?;
    let frame = inner
        .wait_media_event(Duration::from_secs(5))?
        .expect("scene delivery");
    let projection = snapshot(&inner, &[1]).bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    relay.media_chunk(
        frame.delivery_id,
        frame.source,
        frame.record_type,
        0,
        frame.body.len() as u32,
        true,
        frame.body,
    )?;
    finish_delivery(&inner, &mut relay, frame.delivery_id)?;
    let drawn = snapshot(&outer, &[9])
        .sources
        .into_iter()
        .find_map(|source| source.retained)
        .expect("outer scene");
    let first = vivid_protocol::vector::Frame::decode(&drawn).unwrap();
    let outer_layout = match first.canvas.commands()[0] {
        vivid_sdk::overlay::Command::TextLayout { layout, .. } => layout,
        _ => panic!("missing text layout"),
    };
    assert_ne!(
        outer_layout,
        layout.id(),
        "layout namespace must be re-originated"
    );
    relay.replace_session(&projection.surfaces, &projection.sources, &projection.nodes)?;
    let retained = snapshot(&inner, &[1]).sources.remove(0);
    for (kind, body) in retained.retained_vector {
        relay.media_chunk(
            0,
            retained.key,
            kind,
            0,
            body.len() as u32,
            true,
            body.to_vec(),
        )?;
        finish_delivery(&inner, &mut relay, 0)?;
    }
    let drawn = snapshot(&outer, &[9])
        .sources
        .into_iter()
        .find_map(|source| source.retained)
        .expect("restored outer scene");
    let restored = vivid_protocol::vector::Frame::decode(&drawn).unwrap();
    assert_eq!(restored.canvas.commands().len(), 2);
    assert_ne!(
        restored.canvas.commands()[0],
        first.canvas.commands()[0],
        "replacement must shape and remap the retained layout again"
    );
    Ok(())
}

fn finish_delivery(inner: &VirtualVivid, relay: &mut OuterBridge, expected: u64) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some((id, delivered, _, _)) = relay.take_media_completions().into_iter().next() {
            assert_eq!(id, expected);
            assert!(delivered, "outer delivery failed");
            inner.complete_bridge_delivery(id, delivered);
            return Ok(());
        }
        assert!(Instant::now() < deadline, "outer delivery did not complete");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn vector_credit_blocks_only_its_owner_until_delivery_is_acknowledged() -> io::Result<()> {
    let listener = TcpPresenterListener::bind()?;
    let endpoint = listener.endpoint();
    let inner = VirtualVivid::start_configured(
        listener,
        PresenterConfig::terminal_with_overlay(MediaConfig::default()),
        None,
    )?;
    let first = overlay_producer(&endpoint, &pane(&inner, 1)?, "credit-one")?;
    let second = overlay_producer(&endpoint, &pane(&inner, 2)?, "credit-two")?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 0., 0.)?;
    a.present(scene(1))?;
    let held = inner.wait_media_event(Duration::from_secs(5))?.unwrap();
    std::thread::scope(|scope| -> io::Result<()> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let a = &a;
        let job = scope.spawn(move || {
            let result = a.present(scene(2));
            tx.send(()).unwrap();
            result
        });
        let blocked = rx.recv_timeout(Duration::from_millis(100)).is_err();
        // Clean up the grant even if the assertion fails, so a broken test cannot hang joining.
        if !blocked {
            inner.complete_bridge_delivery(held.delivery_id, true);
        }
        assert!(
            blocked,
            "second frame escaped before the outer-write acknowledgement"
        );
        b.present(scene(3))?;
        let unrelated = inner.wait_media_event(Duration::from_secs(5))?.unwrap();
        assert_eq!(held.source.track, unrelated.source.track);
        assert_eq!(held.source.surface, unrelated.source.surface);
        assert_ne!(held.source.producer, unrelated.source.producer);
        inner.complete_bridge_delivery(unrelated.delivery_id, true);
        inner.complete_bridge_delivery(held.delivery_id, true);
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        job.join().unwrap()?;
        let resumed = inner.wait_media_event(Duration::from_secs(5))?.unwrap();
        assert_eq!(resumed.source, held.source);
        inner.complete_bridge_delivery(resumed.delivery_id, true);
        Ok(())
    })
}

#[test]
fn child_modal_and_pointer_capture_cross_the_gateway() -> io::Result<()> {
    let (inner, endpoint) = overlay_presenter()?;
    inner.enable_overlay_host_relay();
    let producer = overlay_producer(&endpoint, &pane(&inner, 1)?, "modal-capture")?;
    let parent = window_of(&producer, 0., 0.)?;
    parent.present(scene(1))?;
    let child = producer.create_child(
        &parent,
        OverlayWindowOptions::new(Rect::new(30., 30., 100., 80.).unwrap(), WindowMode::Modal),
    )?;
    child.present(scene(2))?;
    let (outer, endpoint) = overlay_presenter()?;
    let mut relay = bridge(&endpoint, &pane(&outer, 9)?)?;
    let projection = snapshot(&inner, &[1]).bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    for source in snapshot(&inner, &[1]).sources {
        for (kind, body) in source.retained_vector {
            relay.media_chunk(
                0,
                source.key,
                kind,
                0,
                body.len() as u32,
                true,
                body.to_vec(),
            )?;
            finish_delivery(&inner, &mut relay, 0)?;
        }
    }
    let windows = overlay_windows(&snapshot(&outer, &[9]));
    assert_eq!(windows.len(), 2);
    let modal = windows
        .iter()
        .find(|(_, window)| window.parent.is_some())
        .unwrap();
    assert!(
        snapshot(&outer, &[9])
            .bridge_projection()
            .surfaces
            .iter()
            .any(|surface| Some(surface.key) == modal.1.parent)
    );
    assert!(outer.overlay_pointer(9, 45., 45., Some((1, true)), 0)?);
    for (surface, body) in relay.take_overlay_input()? {
        inner.relay_overlay_input(surface, &body)?;
    }
    std::thread::scope(|scope| -> io::Result<()> {
        let capture = scope.spawn(|| producer.capture_pointer(&child, true));
        let deadline = Instant::now() + Duration::from_secs(5);
        while !capture.is_finished() {
            for request in inner.take_overlay_host_requests() {
                inner.complete_overlay_host_request(
                    request.id,
                    relay
                        .overlay_host_request(&request)
                        .map_err(|error| error.to_string()),
                );
            }
            assert!(Instant::now() < deadline, "capture never completed");
            std::thread::sleep(Duration::from_millis(5));
        }
        capture.join().unwrap()
    })?;
    // Outside both windows: only a capture reaching the physical host can receive this release.
    assert!(outer.overlay_pointer(9, 600., 300., Some((1, false)), 0)?);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for (surface, body) in relay.take_overlay_input()? {
            inner.relay_overlay_input(surface, &body)?;
        }
        if let Some(vivid_sdk::OverlayLaneEvent::Input(event)) =
            producer.wait_event(Duration::from_millis(10))?
            && matches!(
                event.event,
                vivid_sdk::overlay::Event::Pointer {
                    button: Some((1, false)),
                    ..
                }
            )
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "captured release never reached producer"
        );
    }
    Ok(())
}

/// An ordinary raster producer, so a pane running one can be shown to keep going while a pane
/// running an overlay is withdrawn.
fn raster_producer(endpoint: &str, secret: &str, name: &str) -> io::Result<Session> {
    let mut config = ProducerConfig {
        authentication: ProducerAuthentication::root_hex(secret).map_err(io::Error::other)?,
        producer_name: name.into(),
        ..ProducerConfig::default()
    };
    pin_endpoints(&mut config, endpoint.to_owned());
    let mut session = Session::connect(config)?;
    let context = session.info().root_context_id;
    let surface = session.create_surface(
        SurfaceDefinition {
            context_id: context,
            surface_id: 1,
            semantic_profile: vivid_sdk::GENERIC_CONTENT.into(),
            coordinate_model: CoordinateModel::CanvasLogicalUnits,
            logical_width: 64,
            logical_height: 64,
            scale_numerator: 1,
            scale_denominator: 1,
            rotation: 0,
            descriptor: SurfaceDescriptor {
                role: SurfaceRole::Figure,
                title: name.into(),
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
            // Cell coordinates, and the terminal layer a node is required to name.
            geometry: vec![
                (0, Value::Unsigned(1)),
                (1, Value::Unsigned(0)),
                (2, Value::Unsigned(0)),
                (3, Value::Unsigned(8)),
                (4, Value::Unsigned(4)),
                (5, Value::Unsigned(0)),
            ],
            fit: Fit::Contain,
            linear_sampling: true,
            z_index: 0,
            visible: true,
            opacity: u16::MAX,
            clip: None,
        },
        &RequestMetadata::default(),
    )?;
    let maximum_record_body =
        vivid_protocol::media::rgba8_raw_frame_body_len(64, 64).map_err(io::Error::other)?;
    session.create_track(
        TrackConfiguration {
            direction: Default::default(),
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
                width: 64,
                height: 64,
                alpha_mode: 1,
                delta_enabled: false,
                maximum_delta_operations: 1,
                zstd_enabled: false,
            }),
            target_latency_us: 16_000,
            maximum_latency_us: 100_000,
            retained_pixel_charge: 64 * 64,
        },
        &RequestMetadata::default(),
    )?;
    Ok(session)
}

fn snapshot(presenter: &VirtualVivid, panes: &[u64]) -> ProjectionSnapshot {
    presenter.projection_snapshot(&panes.iter().copied().collect::<HashSet<_>>())
}

/// Every surface in a projection that carries an overlay window, by its owner-qualified key.
fn overlay_windows(
    snapshot: &ProjectionSnapshot,
) -> Vec<(u64, vivid_gateway::BridgeOverlayWindow)> {
    snapshot
        .bridge_projection()
        .surfaces
        .iter()
        .filter_map(|surface| Some((surface.key.producer, surface.overlay_window?)))
        .collect()
}

/// An outer bridge speaking to an overlay-hosting presenter, as the foreground client does.
fn bridge(endpoint: &str, secret: &str) -> io::Result<OuterBridge> {
    OuterBridge::connect_native_for_target(
        endpoint.to_owned(),
        None,
        None,
        Zeroizing::new(secret.to_owned()),
        vivid_sdk::TERMINAL_SURFACE,
        DisplayMetrics::default(),
    )
}

/// Move each relayed window by a pane origin, the way a multiplexer places a pane-local window in
/// the terminal it is re-originating into.
fn translated(surfaces: &[BridgeSurface], dx: i64, dy: i64) -> Vec<BridgeSurface> {
    surfaces
        .iter()
        .cloned()
        .map(|mut surface| {
            if let Some(window) = surface.overlay_window.as_mut() {
                window.x += dx;
                window.y += dy;
            }
            surface
        })
        .collect()
}

/// Wait for a projection to settle on a window count. A producer's disconnect is noticed by the
/// presenter's own reader thread, so the snapshot that reflects it is not the next one by
/// construction.
fn await_windows(presenter: &VirtualVivid, panes: &[u64], count: usize) -> ProjectionSnapshot {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let current = snapshot(presenter, panes);
        if overlay_windows(&current).len() == count {
            return current;
        }
        assert!(
            Instant::now() < deadline,
            "projection never settled on {count} overlay window(s)"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The geometry one overlay-hosting presenter is holding for its single window.
fn hosted_window(presenter: &VirtualVivid, panes: &[u64]) -> vivid_gateway::BridgeOverlayWindow {
    let windows = overlay_windows(&snapshot(presenter, panes));
    assert_eq!(windows.len(), 1, "expected exactly one hosted window");
    windows[0].1
}

fn window_of(overlays: &OverlaySession, x: f64, y: f64) -> io::Result<OverlayWindow> {
    overlays.create_window(OverlayWindowOptions::new(
        Rect::new(x, y, 240., 120.).map_err(io::Error::other)?,
        WindowMode::Floating,
    ))
}

/// The whole path, in the order it happens, with the producer still live at the end.
///
/// Ordering is the point: the outer surface has to exist before its window can be placed, the
/// window before the scene that fills it, and a later move has to name the revision the outer
/// presenter actually holds rather than the one the inner producer counted. Getting the last of
/// those wrong is invisible in a snapshot comparison and fatal on a socket — the outer presenter
/// refuses the update as stale and the window silently stops tracking.
#[test]
fn an_overlay_window_crosses_the_terminating_gateway_and_keeps_tracking() -> io::Result<()> {
    const INNER_PANE: u64 = 3;
    const OUTER_PANE: u64 = 9;

    let (inner, inner_endpoint) = overlay_presenter()?;
    let inner_secret = pane(&inner, INNER_PANE)?;
    let overlays = overlay_producer(&inner_endpoint, &inner_secret, "overlay-relay")?;
    let window = window_of(&overlays, 12., 20.)?;
    window.present(scene(0x203050ff))?;

    let (outer, outer_endpoint) = overlay_presenter()?;
    let outer_secret = pane(&outer, OUTER_PANE)?;
    let mut relay = bridge(&outer_endpoint, &outer_secret)?;

    let projection = snapshot(&inner, &[INNER_PANE]).bridge_projection();
    assert_eq!(overlay_windows(&snapshot(&inner, &[INNER_PANE])).len(), 1);
    assert!(
        projection.nodes.is_empty(),
        "an overlay window is placed by its own bounds, never by a scene node"
    );
    // A pane at column 10, row 2 of an 8x16 cell grid.
    relay.rebuild(
        &translated(&projection.surfaces, 80, 32),
        &projection.sources,
        &projection.nodes,
    )?;

    let placed = hosted_window(&outer, &[OUTER_PANE]);
    assert_eq!(
        (placed.x, placed.y, placed.width, placed.height),
        (92, 52, 240, 120),
        "the outer presenter holds the window where the relay placed it"
    );
    assert!(placed.visible);
    let first_revision = placed.revision;
    assert!(first_revision > 0, "the outer window was established");

    // Liveness: the producer keeps drawing into a window that has already been relayed. A vector
    // channel that never returns its grant accepts the first display list and then blocks forever,
    // which looks exactly like a hung program.
    for frame in 0..6 {
        window.present(scene(0x203050ff + frame))?;
    }

    // The producer moves its window twice before the relay reconciles again, which is the normal
    // case rather than a corner: a producer moves a window when it likes, and a multiplexer
    // reconciles on its own render cadence. The two revisions diverge here — the inner window is
    // now at 3 while the outer presenter still holds 1 — and every later update has to name the
    // revision the *outer* presenter holds. Reusing the inner one is refused as stale, and the
    // window silently stops tracking from then on.
    let move_to = |x: f64, y: f64| -> io::Result<()> {
        window.set_bounds(Rect::new(x, y, 240., 120.).map_err(io::Error::other)?)
    };
    move_to(24., 40.)?;
    move_to(40., 60.)?;
    let moved = snapshot(&inner, &[INNER_PANE]).bridge_projection();
    relay.rebuild(
        &translated(&moved.surfaces, 80, 32),
        &moved.sources,
        &moved.nodes,
    )?;
    let tracked = hosted_window(&outer, &[OUTER_PANE]);
    assert_eq!(
        (tracked.x, tracked.y),
        (120, 92),
        "the move reached the outer presenter"
    );
    assert!(
        tracked.revision > first_revision,
        "the outer window advanced its own revision"
    );

    // A third move, now that the two revision counters have diverged.
    move_to(8., 16.)?;
    let again = snapshot(&inner, &[INNER_PANE]).bridge_projection();
    relay.rebuild(
        &translated(&again.surfaces, 80, 32),
        &again.sources,
        &again.nodes,
    )?;
    let still_tracking = hosted_window(&outer, &[OUTER_PANE]);
    assert_eq!(
        (still_tracking.x, still_tracking.y),
        (88, 48),
        "a window whose producer outran the relay keeps tracking rather than sticking"
    );
    assert!(still_tracking.revision > tracked.revision);

    // Teardown: withdrawing the surface takes the window with it.
    relay.rebuild(&[], &[], &[])?;
    assert!(
        overlay_windows(&snapshot(&outer, &[OUTER_PANE])).is_empty(),
        "the outer window is gone once its surface is withdrawn"
    );
    Ok(())
}

/// Visibility, not focus, decides what is projected. A pane that stops being visible has its
/// overlay window withdrawn exactly like a hidden raster source, while a pane that is still visible
/// keeps its media — and the withdrawn window comes back intact when its pane does.
#[test]
fn a_hidden_pane_withdraws_its_overlay_window_while_the_visible_pane_keeps_going() -> io::Result<()>
{
    const OVERLAY_PANE: u64 = 1;
    const RASTER_PANE: u64 = 2;
    const OUTER_PANE: u64 = 9;

    let (inner, inner_endpoint) = overlay_presenter()?;
    let overlay_secret = pane(&inner, OVERLAY_PANE)?;
    let raster_secret = pane(&inner, RASTER_PANE)?;
    let overlays = overlay_producer(&inner_endpoint, &overlay_secret, "overlay-pane")?;
    let raster = raster_producer(&inner_endpoint, &raster_secret, "raster-pane")?;
    let window = window_of(&overlays, 12., 20.)?;
    window.present(scene(0x203050ff))?;

    let (outer, outer_endpoint) = overlay_presenter()?;
    let outer_secret = pane(&outer, OUTER_PANE)?;
    let mut relay = bridge(&outer_endpoint, &outer_secret)?;

    let both = snapshot(&inner, &[OVERLAY_PANE, RASTER_PANE]).bridge_projection();
    assert_eq!(both.surfaces.len(), 2, "both panes are visible");
    relay.rebuild(&both.surfaces, &both.sources, &both.nodes)?;
    assert_eq!(overlay_windows(&snapshot(&outer, &[OUTER_PANE])).len(), 1);
    let outer_raster = both
        .sources
        .iter()
        .find(|source| {
            !matches!(
                source.kind,
                vivid_gateway::BridgeSourceKind::VectorScene { .. }
            )
        })
        .map(|source| source.key)
        .expect("the raster pane has a source");
    let raster_track = relay
        .outer_track_id(outer_raster)
        .expect("the raster source reached the outer presenter");

    // The overlay pane stops being visible; the raster pane does not.
    let hidden = snapshot(&inner, &[RASTER_PANE]).bridge_projection();
    assert!(
        hidden
            .surfaces
            .iter()
            .all(|surface| surface.overlay_window.is_none()),
        "a hidden pane contributes no overlay window to the projection"
    );
    relay.rebuild(&hidden.surfaces, &hidden.sources, &hidden.nodes)?;
    assert!(
        overlay_windows(&snapshot(&outer, &[OUTER_PANE])).is_empty(),
        "the hidden pane's window was withdrawn from the outer presenter"
    );
    assert_eq!(
        relay.outer_track_id(outer_raster),
        Some(raster_track),
        "hiding one pane must not disturb the pane that is still visible"
    );

    // Both visible again: the window returns, and its producer never stopped being able to draw.
    window.present(scene(0x00ff00ff))?;
    let restored = snapshot(&inner, &[OVERLAY_PANE, RASTER_PANE]).bridge_projection();
    relay.rebuild(&restored.surfaces, &restored.sources, &restored.nodes)?;
    let returned = hosted_window(&outer, &[OUTER_PANE]);
    assert_eq!((returned.x, returned.y), (12, 20));
    assert!(returned.visible);
    drop(raster);
    Ok(())
}

/// Two overlay producers that name their surfaces, tracks and windows identically, because each
/// numbers objects in its own identity space and nothing stops them from colliding.
///
/// Losing one of them must leave the other's window, scene and outer object exactly where they
/// were, and must not cost it the ability to carry on: a teardown keyed on a local object id alone
/// would take the survivor's window down with the casualty's.
#[test]
fn one_overlay_producer_closing_leaves_the_other_reusing_the_same_ids_intact() -> io::Result<()> {
    const FIRST_PANE: u64 = 1;
    const SECOND_PANE: u64 = 2;
    const OUTER_PANE: u64 = 9;

    let (inner, inner_endpoint) = overlay_presenter()?;
    let first_secret = pane(&inner, FIRST_PANE)?;
    let second_secret = pane(&inner, SECOND_PANE)?;
    let first = overlay_producer(&inner_endpoint, &first_secret, "owner-one")?;
    let second = overlay_producer(&inner_endpoint, &second_secret, "owner-two")?;
    let first_window = window_of(&first, 12., 20.)?;
    let second_window = window_of(&second, 12., 20.)?;
    let first_image = first_window.upload_rgba(1, 1, &[255, 0, 0, 255])?;
    let second_image = second_window.upload_rgba(1, 1, &[0, 0, 255, 255])?;
    let mut first_scene = scene(0xff0000ff);
    first_window.draw_image(
        &mut first_scene,
        &first_image,
        Rect::new(0., 0., 10., 10.).unwrap(),
        u16::MAX,
    )?;
    let mut second_scene = scene(0x0000ffff);
    second_window.draw_image(
        &mut second_scene,
        &second_image,
        Rect::new(0., 0., 10., 10.).unwrap(),
        u16::MAX,
    )?;
    first_window.present(first_scene)?;
    second_window.present(second_scene.clone())?;

    let (outer, outer_endpoint) = overlay_presenter()?;
    let outer_secret = pane(&outer, OUTER_PANE)?;
    let mut relay = bridge(&outer_endpoint, &outer_secret)?;

    let panes = [FIRST_PANE, SECOND_PANE];
    let established = snapshot(&inner, &panes);
    // Each pane's own surface, so the producer that is about to be lost is named rather than
    // guessed from an ordering the presenter never promised.
    let producer_of = |pane: u64| {
        established
            .surfaces
            .iter()
            .find(|surface| surface.pane == pane)
            .map(|surface| surface.producer)
            .expect("each pane has a surface")
    };
    let projection = established.bridge_projection();
    assert_eq!(projection.surfaces.len(), 2);
    assert_eq!(
        projection.surfaces[0].key.surface, projection.surfaces[1].key.surface,
        "the two producers really did reuse the same local surface id"
    );
    assert_ne!(
        projection.surfaces[0].key.producer,
        projection.surfaces[1].key.producer
    );
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;

    let key_of = |producer| {
        projection
            .surfaces
            .iter()
            .find(|surface| surface.key.producer == producer)
            .map(|surface| surface.key)
            .expect("each producer has a projected surface")
    };
    let casualty_key = key_of(producer_of(FIRST_PANE));
    let mut asset_ids = Vec::new();
    for source in snapshot(&inner, &panes).sources {
        asset_ids.push(
            vivid_protocol::vector::ImageAsset::decode(&source.retained_vector[0].1)
                .unwrap()
                .id,
        );
        for (kind, body) in source.retained_vector {
            relay.media_chunk(
                0,
                source.key,
                kind,
                0,
                body.len() as u32,
                true,
                body.to_vec(),
            )?;
            finish_delivery(&inner, &mut relay, 0)?;
        }
    }
    assert_eq!(asset_ids.len(), 2);
    assert_eq!(
        asset_ids[0], asset_ids[1],
        "both producers reuse the same local asset ID"
    );
    let survivor_key = key_of(producer_of(SECOND_PANE));
    let survivor_outer = relay
        .outer_surface_id(survivor_key)
        .expect("survivor has an outer surface");
    let survivor_source = projection
        .sources
        .iter()
        .find(|source| source.key.producer == survivor_key.producer)
        .map(|source| source.key)
        .expect("survivor has a vector-scene source");
    let survivor_track = relay
        .outer_track_id(survivor_source)
        .expect("survivor's vector track reached the outer presenter");
    assert_ne!(
        survivor_outer,
        relay
            .outer_surface_id(casualty_key)
            .expect("casualty has an outer surface"),
        "identical local ids became distinct outer objects"
    );

    // One producer is lost.
    drop(first_window);
    drop(first);
    let survivor_window = second_window;
    let remaining = await_windows(&inner, &panes, 1).bridge_projection();
    assert_eq!(remaining.surfaces.len(), 1);
    assert_eq!(
        remaining.surfaces[0].key, survivor_key,
        "the surface that survived is the one belonging to the producer that did"
    );

    relay.rebuild(&remaining.surfaces, &remaining.sources, &remaining.nodes)?;
    assert!(
        relay.outer_surface_id(casualty_key).is_none(),
        "the lost producer's outer surface was destroyed"
    );
    assert_eq!(
        relay.outer_surface_id(survivor_key),
        Some(survivor_outer),
        "the unrelated producer kept the very same outer surface"
    );
    assert_eq!(
        relay.outer_track_id(survivor_source),
        Some(survivor_track),
        "and kept its vector-scene track"
    );
    let held = hosted_window(&outer, &[OUTER_PANE]);
    assert_eq!(
        (held.x, held.y, held.width, held.height),
        (12, 20, 240, 120),
        "the survivor's window is still where it was"
    );

    // And it can still commit further work: a new display list and a window move both land.
    survivor_window.present(second_scene.clone())?;
    survivor_window.set_bounds(Rect::new(30., 40., 240., 120.).map_err(io::Error::other)?)?;
    let after = snapshot(&inner, &panes).bridge_projection();
    relay.rebuild(&after.surfaces, &after.sources, &after.nodes)?;
    let retained = snapshot(&inner, &panes).sources.remove(0);
    let frame = retained.retained.unwrap();
    relay.media_chunk(
        0,
        survivor_source,
        vivid_protocol::messages::VECTOR_FRAME,
        0,
        frame.len() as u32,
        true,
        frame.to_vec(),
    )?;
    finish_delivery(&inner, &mut relay, 0)?;
    let painted = snapshot(&outer, &[OUTER_PANE])
        .sources
        .remove(0)
        .retained
        .unwrap();
    assert_eq!(
        vivid_protocol::vector::Frame::decode(&painted)
            .unwrap()
            .canvas,
        second_scene
    );
    let moved = hosted_window(&outer, &[OUTER_PANE]);
    assert_eq!((moved.x, moved.y), (30, 40));
    assert!(moved.revision > held.revision);
    drop(second);
    Ok(())
}
