//! Outer producer, surface, track and scene-node configuration derived from inner state.

use std::io;
use vivid_protocol::auth::Secret32;
use vivid_protocol::cbor::Value;
use vivid_protocol::media;
use vivid_protocol::messages::LaneClass;
use vivid_protocol::registry;
use vivid_protocol::surface::POLICY_KNOWN_MASK;
use vivid_protocol::track::{
    AudioConfiguration, ImageConfiguration, KindConfiguration, RasterConfiguration,
    TrackConfiguration, TrackDirection, TrackMode, VectorConfiguration, VideoConfiguration,
};
use vivid_sdk::presenter::{
    BridgeNode, BridgeSource, BridgeSourceKind, BridgeSurface, DisplayMetrics,
};
use vivid_sdk::{
    CoordinateModel, Fit, ProducerAuthentication, ProducerConfig, SceneNode, Surface,
    SurfaceDefinition, SurfaceDescriptor, SurfaceRole,
};

use super::{invalid_data, payload_u64};

/// The `primary-video` surface slot (media specification, slot table).
pub(super) const SLOT_VIDEO: u64 = 1;
/// The `audio` surface slot, which is the surface clock whenever it is active.
pub(super) const SLOT_AUDIO: u64 = 2;
/// The `raster` surface slot.
pub(super) const SLOT_RASTER: u64 = 3;
/// The `poster` surface slot, which relayed encoded images occupy.
const SLOT_POSTER: u64 = 4;
/// The `vector-scene` surface slot (overlay specification).
const SLOT_VECTOR: u64 = 5;

/// Name the outer presenter sees for every re-originated producer session.
const PRODUCER_NAME: &str = "vivid-gateway";

/// Terminal target descriptor keys (terminal-surface specification, target descriptor table).
const TERMINAL_COLUMNS: u64 = 2;
const TERMINAL_ROWS: u64 = 3;
const TERMINAL_CELL_WIDTH: u64 = 4;
const TERMINAL_CELL_HEIGHT: u64 = 5;

/// Node geometry map keys shared by the terminal and desktop target profiles.
const GEOMETRY_SPACE: u64 = 0;
const GEOMETRY_X: u64 = 1;
const GEOMETRY_Y: u64 = 2;
const GEOMETRY_WIDTH: u64 = 3;
const GEOMETRY_HEIGHT: u64 = 4;
/// Terminal-only geometry key naming the text layer the node composes into.
const GEOMETRY_TEXT_LAYER: u64 = 5;
/// Node clip map keys; the clip follows the node's coordinate space.
const CLIP_X: u64 = 0;
const CLIP_Y: u64 = 1;
const CLIP_WIDTH: u64 = 2;
const CLIP_HEIGHT: u64 = 3;
/// Grid-cell coordinates on a terminal target, logical pixels on a desktop target.
const SPACE_TARGET: u64 = 1;
/// The terminal text layer between the cell background and the glyphs.
const TEXT_LAYER_UNDER_GLYPHS: u64 = 1;

/// Bytes a `VECTOR_FRAME` body carries ahead of its canvas: a `u32` epoch and a `u64` revision.
const VECTOR_FRAME_HEADER_BYTES: u32 = 12;
/// Bytes a `VECTOR_ASSET` body carries ahead of its pixels: a `u64` ID and `u32` dimensions.
const VECTOR_ASSET_HEADER_BYTES: usize = 16;

/// Records per second a relayed raster track may send; each record is one frame.
const RASTER_RECORDS_PER_SECOND: u64 = 120;
/// Raster frames the outer flow window may hold in flight at once.
const RASTER_INFLIGHT_RECORDS: u64 = 2;
/// A relayed encoded image is a single record.
const IMAGE_RECORDS_PER_SECOND: u64 = 1;
/// Access units per second a relayed video track may send.
const VIDEO_RECORDS_PER_SECOND: u64 = 240;
/// Video access units the outer flow window may hold in flight at once.
const VIDEO_INFLIGHT_RECORDS: u64 = 16;
/// Reorder depth advertised for every relayed video track.
///
/// The specification caps it at 64. [`super::OUTER_TIMED_PREROLL_RECORDS`] must stay above it, or
/// pre-roll stops before a reordering decoder can emit its first picture.
const VIDEO_REORDER_DEPTH: u8 = 16;
/// Audio packets per second a relayed audio track may send.
const AUDIO_RECORDS_PER_SECOND: u64 = 1_000;
/// Audio packets the outer flow window may hold in flight at once.
const AUDIO_INFLIGHT_RECORDS: u64 = 64;
/// Display-list revisions per second a relayed vector track may send.
const VECTOR_RECORDS_PER_SECOND: u64 = 60;
/// Vector frames the outer flow window may hold in flight at once.
const VECTOR_INFLIGHT_RECORDS: u64 = 2;
/// Presentation latency a timed outer track aims for.
const TIMED_TARGET_LATENCY_US: u64 = 20_000;
/// Latency past which the outer presenter treats a timed record as late.
const TIMED_MAXIMUM_LATENCY_US: u64 = 1_000_000;
/// Latency past which the outer presenter treats a live record as late.
const LIVE_MAXIMUM_LATENCY_US: u64 = 100_000;

/// Admission ceilings one relayed track requests from the outer presenter.
struct Admission {
    records_per_second: u64,
    bits_per_second: u64,
    inflight_body_bytes: u64,
}

pub(super) fn producer_config(
    endpoint_control: Option<String>,
    endpoint_realtime: Option<String>,
    endpoint_bulk: Option<String>,
    root_secret: &Secret32,
    target_profile: &str,
) -> io::Result<ProducerConfig> {
    let mut config = match target_profile {
        registry::TERMINAL_SURFACE => ProducerConfig {
            target_profile: vivid_sdk::TERMINAL_SURFACE.into(),
            required_profiles: vec![
                vivid_sdk::AUDIO_GAIN.into(),
                vivid_sdk::LIVE_MEDIA.into(),
                vivid_sdk::OBSERVABILITY.into(),
                vivid_sdk::TERMINAL_SURFACE.into(),
                vivid_sdk::TIMED_MEDIA.into(),
                vivid_sdk::CORE_CONTROL.into(),
            ],
            // Optional: an outer presenter that cannot host overlay windows must keep working
            // exactly as it does today, so none of these are required. Requested only under
            // terminal-surface-v1 — `terminal-overlay-v1`'s prerequisite excludes desktop-surface-v1.
            optional_profiles: vec![
                registry::AUDIO_INPUT.into(),
                registry::TERMINAL_OVERLAY.into(),
                registry::VECTOR_SCENE.into(),
                registry::OVERLAY_INPUT.into(),
                registry::OVERLAY_PAINT.into(),
                registry::OVERLAY_POINTER.into(),
                registry::OVERLAY_TEXT.into(),
                registry::OVERLAY_TEXT_LAYOUT.into(),
                registry::OVERLAY_ENV.into(),
                registry::OVERLAY_CLIPBOARD.into(),
                registry::OVERLAY_A11Y.into(),
                registry::OVERLAY_TYPOGRAPHY.into(),
            ],
            ..ProducerConfig::default()
        },
        registry::DESKTOP_SURFACE => ProducerConfig::desktop(),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gateway outer target must be terminal-surface-v1 or desktop-surface-v1",
            ));
        }
    };
    config.optional_profiles.extend([
        registry::TIMED_MEDIA.into(),
        registry::TIMED_MEDIA_SYNC.into(),
    ]);
    config
        .optional_profiles
        .retain(|profile| !config.required_profiles.contains(profile));
    config.optional_profiles.sort();
    config.optional_profiles.dedup();
    // These endpoints have already been resolved by the gateway's caller. Pin every native lane
    // so an ambient producer-discovery environment cannot redirect one part of this independently
    // authenticated outer session to another presenter. The protocol fallbacks are interactive to
    // control, bulk to control, and realtime to bulk.
    let endpoint_bulk = endpoint_bulk.or_else(|| endpoint_control.clone());
    let endpoint_realtime = endpoint_realtime.or_else(|| endpoint_bulk.clone());
    config.endpoint_interactive = endpoint_control.clone();
    config.endpoint_control = endpoint_control;
    config.endpoint_realtime = endpoint_realtime;
    config.endpoint_bulk = endpoint_bulk;
    config.authentication = ProducerAuthentication::Root {
        root_secret: root_secret.clone(),
    };
    config.producer_name = PRODUCER_NAME.into();
    config.producer_version = env!("CARGO_PKG_VERSION").into();
    Ok(config)
}

pub(super) fn display_from_target(
    session: &vivid_sdk::Session,
    fallback: DisplayMetrics,
) -> io::Result<DisplayMetrics> {
    if session.info().target_profile == registry::DESKTOP_SURFACE {
        return Ok(fallback);
    }
    let payload = &session.info().target_descriptor;
    let read = |key| payload_u64(payload, key);
    let columns = read(TERMINAL_COLUMNS).and_then(|value| u16::try_from(value).ok());
    let rows = read(TERMINAL_ROWS).and_then(|value| u16::try_from(value).ok());
    let cell_width = read(TERMINAL_CELL_WIDTH).and_then(|value| u16::try_from(value).ok());
    let cell_height = read(TERMINAL_CELL_HEIGHT).and_then(|value| u16::try_from(value).ok());
    match (columns, rows, cell_width, cell_height) {
        (Some(columns), Some(rows), Some(cell_width), Some(cell_height))
            if columns > 0 && rows > 0 && cell_width > 0 && cell_height > 0 =>
        {
            Ok(DisplayMetrics {
                columns,
                rows,
                cell_width,
                cell_height,
            })
        }
        _ if fallback.columns > 0
            && fallback.rows > 0
            && fallback.cell_width > 0
            && fallback.cell_height > 0 =>
        {
            Ok(fallback)
        }
        _ => Err(invalid_data(
            "outer WELCOME has an invalid terminal target descriptor",
        )),
    }
}

pub(super) fn surface_definition(
    session: &vivid_sdk::Session,
    surface: &BridgeSurface,
    existing: Option<&Surface>,
    enforced_policy: u64,
) -> io::Result<SurfaceDefinition> {
    let descriptor = &surface.descriptor;
    Ok(SurfaceDefinition {
        context_id: session.info().root_context_id,
        surface_id: match existing {
            Some(existing) => existing.id(),
            None => session.allocate_id()?,
        },
        semantic_profile: vivid_sdk::GENERIC_CONTENT.into(),
        coordinate_model: CoordinateModel::DesktopLogicalPixels,
        logical_width: surface.logical_width,
        logical_height: surface.logical_height,
        scale_numerator: 1,
        scale_denominator: 1,
        rotation: 0,
        descriptor: SurfaceDescriptor {
            role: SurfaceRole::try_from(descriptor.role)
                .ok()
                .unwrap_or(SurfaceRole::Figure),
            title: if descriptor.title.is_empty() {
                {
                    format!(
                        "nested surface {}:{}:{}",
                        surface.key.producer, surface.key.context, surface.key.surface
                    )
                }
            } else {
                descriptor.title.clone()
            },
            semantic_content_revision: descriptor.content_revision,
            semantic_availability: descriptor.semantic_availability,
            locator_hint: descriptor.locator.clone(),
        },
        policy: intersect_surface_policy(surface.capture_policy, enforced_policy)?,
        profile_parameters: vec![],
    })
}

/// Combines an inner surface's capture policy with route-level restrictions.
///
/// Capture policy bits are restrictions, so combining them is a union that can only tighten what the
/// inner producer asked for.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] when either policy has a bit the registry has not
/// assigned.
pub(crate) fn intersect_surface_policy(inner_policy: u64, enforced_policy: u64) -> io::Result<u64> {
    let combined = inner_policy | enforced_policy;
    if combined & !POLICY_KNOWN_MASK != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "gateway surface policy contains unassigned bits",
        ));
    }
    Ok(combined)
}

pub(super) fn track_configuration(
    session: &vivid_sdk::Session,
    surface: &Surface,
    source: &BridgeSource,
) -> io::Result<TrackConfiguration> {
    let mode = if source.live {
        TrackMode::Live
    } else {
        TrackMode::Timed
    };
    let (kind, mode, lane, maximum_record_body, admission, pixels) = match &source.kind {
        BridgeSourceKind::Raster {
            width,
            height,
            alpha_mode,
            compression_mode,
            delta_operation_limit,
        } => {
            let body =
                media::rgba8_raw_frame_body_len(*width, *height).map_err(io::Error::other)?;
            (
                KindConfiguration::Raster(RasterConfiguration {
                    width: *width,
                    height: *height,
                    alpha_mode: *alpha_mode,
                    delta_enabled: delta_operation_limit.is_some(),
                    maximum_delta_operations: delta_operation_limit
                        .and_then(|value| u8::try_from(value).ok())
                        .unwrap_or(1),
                    zstd_enabled: *compression_mode != 0,
                }),
                TrackMode::Live,
                LaneClass::Bulk,
                body,
                Admission {
                    records_per_second: RASTER_RECORDS_PER_SECOND,
                    bits_per_second: u64::from(body)
                        .saturating_mul(8)
                        .saturating_mul(RASTER_RECORDS_PER_SECOND),
                    inflight_body_bytes: u64::from(body).saturating_mul(RASTER_INFLIGHT_RECORDS),
                },
                u64::from(*width).saturating_mul(u64::from(*height)),
            )
        }
        BridgeSourceKind::Image {
            encoding,
            width,
            height,
            encoded_length,
            sha256,
        } => (
            KindConfiguration::EncodedImage(ImageConfiguration {
                encoding: *encoding,
                width: *width,
                height: *height,
                encoded_length: *encoded_length,
                sha256: *sha256,
                cache_lookup: false,
            }),
            TrackMode::Live,
            LaneClass::Bulk,
            *encoded_length,
            Admission {
                records_per_second: IMAGE_RECORDS_PER_SECOND,
                bits_per_second: u64::from(*encoded_length).saturating_mul(8),
                inflight_body_bytes: u64::from(*encoded_length),
            },
            u64::from(*width).saturating_mul(u64::from(*height)),
        ),
        BridgeSourceKind::Video {
            codec,
            packetization,
            extradata,
            width,
            height,
            profile,
            level,
            bitrate,
            color_primaries,
            transfer,
            matrix,
            range,
            sar_num,
            sar_den,
            max_access_unit_bytes,
            codec_string,
            decoder_config,
        } => {
            let body = media::video_body_len(*max_access_unit_bytes).map_err(io::Error::other)?;
            (
                KindConfiguration::Video(VideoConfiguration {
                    codec: codec.clone(),
                    packetization: packetization.clone(),
                    extradata: extradata.clone(),
                    coded_width: *width,
                    coded_height: *height,
                    profile: *profile,
                    level: *level,
                    maximum_reorder_depth: VIDEO_REORDER_DEPTH,
                    color_primaries: *color_primaries,
                    transfer: *transfer,
                    matrix: *matrix,
                    signal_range: *range,
                    aspect_numerator: u64::from(*sar_num),
                    aspect_denominator: u64::from(*sar_den),
                    maximum_access_unit_bytes: *max_access_unit_bytes,
                    codec_string: codec_string.clone(),
                    decoder_configuration: decoder_config.clone(),
                }),
                mode,
                LaneClass::Realtime,
                body,
                Admission {
                    records_per_second: VIDEO_RECORDS_PER_SECOND,
                    bits_per_second: (*bitrate).max(u64::from(body).saturating_mul(8)),
                    inflight_body_bytes: u64::from(body).saturating_mul(VIDEO_INFLIGHT_RECORDS),
                },
                u64::from(*width).saturating_mul(u64::from(*height)),
            )
        }
        BridgeSourceKind::Audio {
            codec,
            packetization,
            extradata,
            sample_rate,
            channels,
            channel_mask,
            bitrate,
            max_access_unit_bytes,
            codec_string,
            ..
        } => {
            let body = media::audio_body_len(*max_access_unit_bytes).map_err(io::Error::other)?;
            (
                KindConfiguration::Audio(AudioConfiguration {
                    codec: codec.clone(),
                    packetization: packetization.clone(),
                    extradata: extradata.clone(),
                    sample_rate: *sample_rate,
                    channels: u8::try_from(*channels).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("audio channel count exceeds u8: {error}"),
                        )
                    })?,
                    channel_mask: *channel_mask,
                    maximum_access_unit_bytes: *max_access_unit_bytes,
                    codec_string: codec_string.clone(),
                }),
                mode,
                LaneClass::Realtime,
                body,
                Admission {
                    records_per_second: AUDIO_RECORDS_PER_SECOND,
                    bits_per_second: (*bitrate).max(u64::from(body).saturating_mul(8)),
                    inflight_body_bytes: u64::from(body).saturating_mul(AUDIO_INFLIGHT_RECORDS),
                },
                0,
            )
        }
        BridgeSourceKind::VectorScene {
            width,
            height,
            maximum_scene_bytes,
        } => {
            // The record body limit must admit a whole frame, not only its canvas, and still admit
            // the largest asset record the same track carries.
            let body = maximum_scene_bytes
                .checked_add(VECTOR_FRAME_HEADER_BYTES)
                .ok_or_else(|| invalid_data("vector scene record body overflow"))?
                .max(
                    u32::try_from(
                        vivid_protocol::vector::MAX_ASSET_BYTES + VECTOR_ASSET_HEADER_BYTES,
                    )
                    .map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("vector asset record body overflow: {error}"),
                        )
                    })?,
                );
            (
                KindConfiguration::VectorScene(VectorConfiguration {
                    width: *width,
                    height: *height,
                    maximum_scene_bytes: *maximum_scene_bytes,
                }),
                // A display list is neither live-paced nor media-timed: it has no PTS and no
                // preplay/PLAY handshake, only a monotonic revision. Timed mode's pacing
                // fields (target/maximum latency) are meaningless for it, matched below like
                // Raster/Image.
                TrackMode::Live,
                LaneClass::Bulk,
                body,
                Admission {
                    records_per_second: VECTOR_RECORDS_PER_SECOND,
                    bits_per_second: u64::from(body)
                        .saturating_mul(8)
                        .saturating_mul(VECTOR_RECORDS_PER_SECOND),
                    inflight_body_bytes: u64::from(body).saturating_mul(VECTOR_INFLIGHT_RECORDS),
                },
                u64::from(*width).saturating_mul(u64::from(*height)),
            )
        }
    };
    let timed = mode == TrackMode::Timed;
    Ok(TrackConfiguration {
        direction: TrackDirection::default(),
        context_id: surface.context_id(),
        surface_id: surface.id(),
        track_id: session.allocate_id()?,
        slot: match &kind {
            KindConfiguration::Video(_) => SLOT_VIDEO,
            KindConfiguration::Audio(_) => SLOT_AUDIO,
            KindConfiguration::Raster(_) => SLOT_RASTER,
            KindConfiguration::EncodedImage(_) => SLOT_POSTER,
            KindConfiguration::VectorScene(_) => SLOT_VECTOR,
        },
        mode,
        lane,
        maximum_record_body,
        maximum_rate_millihertz: admission.records_per_second.saturating_mul(1_000),
        maximum_encoded_bits_per_second: admission.bits_per_second.max(1),
        maximum_records_per_second: admission.records_per_second,
        maximum_inflight_body_bytes: admission
            .inflight_body_bytes
            .max(u64::from(maximum_record_body)),
        kind,
        target_latency_us: if timed { TIMED_TARGET_LATENCY_US } else { 0 },
        maximum_latency_us: if timed {
            TIMED_MAXIMUM_LATENCY_US
        } else {
            LIVE_MAXIMUM_LATENCY_US
        },
        retained_pixel_charge: pixels,
    })
}

pub(super) fn slot_for_kind(kind: &BridgeSourceKind) -> u64 {
    match kind {
        BridgeSourceKind::Video { .. } => SLOT_VIDEO,
        BridgeSourceKind::Audio { .. } => SLOT_AUDIO,
        BridgeSourceKind::Raster { .. } => SLOT_RASTER,
        BridgeSourceKind::Image { .. } => SLOT_POSTER,
        BridgeSourceKind::VectorScene { .. } => SLOT_VECTOR,
    }
}

pub(super) fn scene_node(
    root_context: u64,
    node_id: u64,
    surface: &Surface,
    source: &BridgeNode,
    target_profile: &str,
) -> SceneNode {
    let mut geometry = vec![
        (GEOMETRY_SPACE, Value::Unsigned(SPACE_TARGET)),
        (GEOMETRY_X, signed(source.x)),
        (GEOMETRY_Y, signed(source.y)),
        (GEOMETRY_WIDTH, signed(source.width)),
        (GEOMETRY_HEIGHT, signed(source.height)),
    ];
    if target_profile == registry::TERMINAL_SURFACE {
        geometry.push((
            GEOMETRY_TEXT_LAYER,
            Value::Unsigned(TEXT_LAYER_UNDER_GLYPHS),
        ));
    }
    SceneNode {
        owning_context_id: root_context,
        node_id,
        surface_context_id: surface.context_id(),
        surface_id: surface.id(),
        geometry,
        fit: Fit::Contain,
        linear_sampling: true,
        z_index: source.z_index,
        visible: source.visible,
        opacity: u16::MAX,
        clip: Some(vec![
            (CLIP_X, signed(source.clip.x)),
            (CLIP_Y, signed(source.clip.y)),
            (CLIP_WIDTH, signed(source.clip.width)),
            (CLIP_HEIGHT, signed(source.clip.height)),
        ]),
    }
}

fn signed(value: i64) -> Value {
    u64::try_from(value).map_or(Value::Negative(value), Value::Unsigned)
}
