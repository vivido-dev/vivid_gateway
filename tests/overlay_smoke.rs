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
    let presenter = VirtualVivid::start_configured(
        listener,
        PresenterConfig::terminal_with_overlay(MediaConfig::default()),
        None,
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
    first_window.present(scene(0xff0000ff))?;
    second_window.present(scene(0x0000ffff))?;

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
    survivor_window.present(scene(0x00ff00ff))?;
    survivor_window.set_bounds(Rect::new(30., 40., 240., 120.).map_err(io::Error::other)?)?;
    let after = snapshot(&inner, &panes).bridge_projection();
    relay.rebuild(&after.surfaces, &after.sources, &after.nodes)?;
    let moved = hosted_window(&outer, &[OUTER_PANE]);
    assert_eq!((moved.x, moved.y), (30, 40));
    assert!(moved.revision > held.revision);
    drop(second);
    Ok(())
}
