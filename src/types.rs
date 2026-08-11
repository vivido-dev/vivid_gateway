use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use vivid_protocol::geometry::{NodeGeometry, TargetExtent, decode_clip};
use vivid_protocol::messages::PayloadMap;
use vivid_protocol::registry;
use vivid_protocol::resource::ResourceContract;
use vivid_protocol::scene::SceneNode;
use vivid_protocol::surface::{DesktopSurfaceParameters, SurfaceDefinition};
use vivid_protocol::target::DesktopTarget;

pub type PaneId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MediaConfig {
    pub aggregate_retained_bytes: u64,
    pub max_sources: usize,
    pub max_nodes: usize,
    pub max_anchors: usize,
    pub ipc_queue_bytes: usize,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            aggregate_retained_bytes: 256 * 1024 * 1024,
            max_sources: 64,
            max_nodes: 256,
            max_anchors: 256,
            ipc_queue_bytes: 32 * 1024 * 1024,
        }
    }
}

/// The presentation target terminated by the inner presenter.
///
/// Terminal descriptors are supplied per principal with [`crate::VirtualVivid::update_metrics`], while a
/// desktop descriptor is part of the gateway's route configuration and can later be advanced with
/// [`crate::VirtualVivid::update_desktop_target`].
pub trait PresentationTarget: fmt::Debug + Send + Sync + 'static {
    fn profile_name(&self) -> &'static str;
    fn initial_descriptor(&self) -> Option<PayloadMap>;
    fn accepts_anchors(&self) -> bool;
    fn extent(&self) -> Option<TargetExtent>;
    fn validate_configuration(&self) -> Result<(), &'static str>;
    fn validate_surface(&self, definition: &SurfaceDefinition) -> Result<(), &'static str>;
    fn validate_node(&self, node: &SceneNode) -> Result<(), &'static str>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TerminalTarget;

impl PresentationTarget for TerminalTarget {
    fn profile_name(&self) -> &'static str {
        registry::TERMINAL_SURFACE
    }

    fn initial_descriptor(&self) -> Option<PayloadMap> {
        None
    }

    fn accepts_anchors(&self) -> bool {
        true
    }

    fn extent(&self) -> Option<TargetExtent> {
        None
    }

    fn validate_configuration(&self) -> Result<(), &'static str> {
        Ok(())
    }

    fn validate_surface(&self, definition: &SurfaceDefinition) -> Result<(), &'static str> {
        if definition.semantic_profile == registry::DESKTOP_CONTENT {
            return Err("desktop content cannot be presented on a terminal target");
        }
        Ok(())
    }

    fn validate_node(&self, node: &SceneNode) -> Result<(), &'static str> {
        let value = |key| {
            node.geometry
                .iter()
                .find(|entry| entry.0 == key)
                .map(|entry| &entry.1)
        };
        let coordinate_space = value(0).and_then(|value| value.as_u64());
        let valid = value(1).and_then(|value| value.as_i64()).is_some()
            && value(2).and_then(|value| value.as_i64()).is_some()
            && value(3)
                .and_then(|value| value.as_i64())
                .is_some_and(|value| value > 0)
            && value(4)
                .and_then(|value| value.as_i64())
                .is_some_and(|value| value > 0)
            && value(5)
                .and_then(|value| value.as_u64())
                .is_some_and(|value| value <= 2)
            && matches!(coordinate_space, Some(1 | 2))
            && match coordinate_space {
                Some(1) => node.geometry.len() == 6,
                Some(2) => {
                    node.geometry.len() == 8
                        && value(6)
                            .and_then(|value| value.as_u64())
                            .is_some_and(|value| value != 0)
                        && value(7)
                            .and_then(|value| value.as_u64())
                            .is_some_and(|value| value != 0)
                }
                _ => false,
            };
        if !valid {
            return Err("invalid terminal node geometry");
        }
        if node
            .clip
            .as_ref()
            .is_some_and(|clip| decode_clip(clip).is_err())
        {
            return Err("invalid scene node clip");
        }
        Ok(())
    }
}

impl PresentationTarget for DesktopTarget {
    fn profile_name(&self) -> &'static str {
        registry::DESKTOP_SURFACE
    }

    fn initial_descriptor(&self) -> Option<PayloadMap> {
        Some(self.encode())
    }

    fn accepts_anchors(&self) -> bool {
        false
    }

    fn extent(&self) -> Option<TargetExtent> {
        Some(self.extent())
    }

    fn validate_configuration(&self) -> Result<(), &'static str> {
        self.validate().map_err(|_| "invalid desktop target")
    }

    fn validate_surface(&self, definition: &SurfaceDefinition) -> Result<(), &'static str> {
        if definition.semantic_profile == registry::TERMINAL_CONTENT {
            return Err("terminal content cannot be presented on a desktop target");
        }
        if definition.semantic_profile == registry::DESKTOP_CONTENT {
            DesktopSurfaceParameters::decode(&definition.profile_parameters)
                .map_err(|_| "invalid desktop surface parameters")?;
        }
        Ok(())
    }

    fn validate_node(&self, node: &SceneNode) -> Result<(), &'static str> {
        NodeGeometry::decode(&node.geometry).map_err(|_| "invalid desktop node geometry")?;
        if node
            .clip
            .as_ref()
            .is_some_and(|clip| decode_clip(clip).is_err())
        {
            return Err("invalid scene node clip");
        }
        Ok(())
    }
}

/// Negotiation and resource policy for an inner-presenter route.
#[derive(Debug, Clone)]
pub struct PresenterConfig {
    pub media: MediaConfig,
    pub target: Arc<dyn PresentationTarget>,
    /// The complete prerequisite-closed set this presenter can honor.
    pub supported_profiles: Vec<String>,
    /// Optional explicit contract. When absent, a bounded contract is derived from `media`.
    pub resource_contract: Option<ResourceContract>,
}

impl PresenterConfig {
    pub fn terminal(media: MediaConfig) -> Self {
        Self {
            media,
            target: Arc::new(TerminalTarget),
            supported_profiles: vec![
                registry::AUDIO_GAIN.into(),
                registry::CORE_CONTROL.into(),
                registry::LIVE_MEDIA.into(),
                registry::OBSERVABILITY.into(),
                registry::TERMINAL_SURFACE.into(),
                registry::TIMED_MEDIA.into(),
            ],
            resource_contract: None,
        }
    }

    pub fn desktop(media: MediaConfig, target: DesktopTarget) -> Self {
        Self {
            media,
            target: Arc::new(target),
            supported_profiles: vec![
                registry::CORE_CONTROL.into(),
                registry::DESKTOP_SURFACE.into(),
                registry::LIVE_MEDIA.into(),
                registry::OBSERVABILITY.into(),
            ],
            resource_contract: None,
        }
    }

    pub fn with_resource_contract(mut self, contract: ResourceContract) -> Self {
        self.resource_contract = Some(contract);
        self
    }

    pub fn with_supported_profiles(mut self, profiles: Vec<String>) -> Self {
        self.supported_profiles = profiles;
        self
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IpcMetrics {
    pub records_written: u64,
    pub wire_bytes_written: u64,
    pub records_read: u64,
    pub wire_bytes_read: u64,
    pub media_payload_bytes: u64,
    pub media_records: u64,
    pub render_payload_bytes: u64,
    pub write_blocked_us: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryMetrics {
    pub created: u64,
    pub delivered: u64,
    pub failed: u64,
    pub dropped_actor_queue_full: u64,
    pub dropped_queue_budget: u64,
    pub released_hidden: u64,
    pub keyframe_requests: u64,
    pub keyframe_requests_damped: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeMetrics {
    pub outer_media_records: u64,
    pub outer_media_bytes: u64,
    pub outer_raster_full_frames: u64,
    pub outer_raster_delta_frames: u64,
    pub inner_raster_bytes: u64,
    pub client_queue_drops: u64,
    pub control_wait_us: u64,
    pub control_wait_timeouts: u64,
    pub session_replacements: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelayMetrics {
    pub ipc: IpcMetrics,
    pub delivery: DeliveryMetrics,
    pub bridge: BridgeMetrics,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DisplayMetrics {
    pub columns: u16,
    pub rows: u16,
    pub cell_width: u16,
    pub cell_height: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaneMediaStatus {
    pub virtual_projection_revision: u64,
    pub virtual_scene_revision: u64,
    pub outer_projection_revision: u64,
    pub outer_apply_sequence: u64,
    pub bridge_instance_id: Option<u64>,
    pub bridge_local_revision: u64,
    pub surfaces: Vec<PaneMediaSurfaceStatus>,
    pub tracks: Vec<PaneMediaTrackStatus>,
    pub nodes: Vec<PaneMediaNodeStatus>,
    pub relay: RelayMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaneMediaSurfaceStatus {
    pub producer_id: u64,
    pub context_id: u64,
    pub surface_id: u64,
    pub lifecycle: String,
    pub surface_revision: u64,
    pub surface_generation: u64,
    pub visible: bool,
    pub capture_policy: u64,
    pub descriptor: Option<PaneMediaSurfaceDescriptor>,
    pub active_slots: Vec<(u64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaneMediaTrackStatus {
    pub producer_id: u64,
    pub context_id: u64,
    pub surface_id: u64,
    pub track_id: u64,
    pub kind: String,
    pub lifecycle: String,
    pub track_revision: u64,
    pub epoch: u32,
    pub channel_state: u64,
    pub inner_channel_generation: u64,
    pub outer_channel_generation: Option<u64>,
    pub outer_mapping_fresh: bool,
    pub visible: bool,
    pub retained_static: bool,
    pub keyframe_needed: bool,
    pub milestones: u64,
    pub queued_packets: u64,
    pub queued_bytes: u64,
    pub available_packet_credit: u64,
    pub available_byte_credit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaneMediaSurfaceDescriptor {
    pub role: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_availability: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaneMediaNodeStatus {
    pub producer_id: u64,
    pub context_id: u64,
    pub node_id: u64,
    pub surface_context_id: u64,
    pub surface_id: u64,
    pub visible: bool,
    pub x: i64,
    pub y: i64,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BridgeSurfaceKey {
    pub producer: u64,
    pub context: u64,
    pub surface: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeSurface {
    pub key: BridgeSurfaceKey,
    pub logical_width: u64,
    pub logical_height: u64,
    pub capture_policy: u64,
    pub descriptor: BridgeSourceDescriptor,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct BridgeSourceKey {
    pub producer: u64,
    pub context: u64,
    pub surface: u64,
    pub track: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeKeyframeRequest {
    pub source: BridgeSourceKey,
    pub minimum_epoch: Option<u32>,
    pub reason: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BridgeSourceKind {
    Raster {
        width: u32,
        height: u32,
        alpha_mode: u64,
        compression_mode: u64,
        delta_operation_limit: Option<u32>,
    },
    Image {
        encoding: u64,
        width: u32,
        height: u32,
        encoded_length: u32,
        sha256: Option<[u8; 32]>,
    },
    Video {
        codec: String,
        packetization: String,
        extradata: Vec<u8>,
        width: u32,
        height: u32,
        profile: i32,
        level: i32,
        bitrate: u64,
        color_primaries: u64,
        transfer: u64,
        matrix: u64,
        range: u64,
        sar_num: u32,
        sar_den: u32,
        max_access_unit_bytes: u32,
        codec_string: Option<String>,
        decoder_config: Option<Vec<u8>>,
    },
    Audio {
        linked_video: Option<BridgeSourceKey>,
        codec: String,
        packetization: String,
        extradata: Vec<u8>,
        sample_rate: u32,
        channels: u16,
        channel_mask: u64,
        bitrate: u64,
        max_access_unit_bytes: u32,
        codec_string: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeSource {
    pub key: BridgeSourceKey,
    pub kind: BridgeSourceKind,
    /// The inner track uses live rather than media-timed delivery semantics.
    ///
    /// This is independent of `playing`: live audio begins when its surface slot is activated and
    /// never receives PLAY. Relays must preserve that distinction or the first audio packet can
    /// remain parked as timed pre-roll forever.
    #[serde(default)]
    pub live: bool,
    /// Whether the inner surface's authoritative slot map currently names this track.
    #[serde(default)]
    pub active: bool,
    /// Unsigned Q32.32 linear audio gain when the inner producer negotiated `audio-gain-v1`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio_gain: Option<u64>,
    #[serde(default)]
    pub capture_policy: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor: Option<BridgeSourceDescriptor>,
    pub playing: bool,
    pub play_request: BridgePlayRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eos_epoch: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<[u8; 16]>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeSourceDescriptor {
    pub role: u64,
    pub title: String,
    pub content_revision: u64,
    pub semantic_availability: u64,
    pub locator: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgePlayRequest {
    pub start_pts_us: i64,
    pub minimum_buffer_us: u64,
    pub maximum_latency_us: u64,
    pub rate_32_32: i64,
    pub late_policy: u64,
    pub loop_count: u64,
    pub start_policy: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeClipRect {
    pub x: i64,
    pub y: i64,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BridgeNode {
    pub producer: u64,
    pub node: u64,
    pub fragment: u8,
    pub surface: BridgeSurfaceKey,
    pub x: i64,
    pub y: i64,
    pub width: i64,
    pub height: i64,
    pub z_index: i64,
    pub visible: bool,
    pub clip: BridgeClipRect,
}
