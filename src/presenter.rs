//! Vivid 1.5 virtual presenter used by panes.
//!
//! This module terminates the inner session. Nothing secret-bearing or authoritative is relayed:
//! projection snapshots contain validated semantic state and portable media bodies only.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use vivid_protocol::anchor::{self, AnchorKey};
use vivid_protocol::auth::{self, Secret32};
use vivid_protocol::cbor::Value;
use vivid_protocol::geometry::{NodeGeometry, decode_clip};
use vivid_protocol::identity::{PresenterInstanceId, SessionIdentity};
use vivid_protocol::lease::{
    AttemptDecision, CleanupPolicy, LeaseMachine, LeaseState, SessionLeaseDefinition,
};
use vivid_protocol::media;
use vivid_protocol::messages::{
    self, ChannelOpen, Envelope, ErrorDetail, ErrorReply, Hello, HelloAuthentication, StrictMap,
    Welcome, WelcomeAuthentication,
};
use vivid_protocol::registry;
use vivid_protocol::resource::{Resource, ResourceContract};
use vivid_protocol::revision::{
    ChannelGeneration, ResumeGeneration, SceneRevision, SurfaceGeneration, SurfaceRevision,
    TargetGeneration,
};
use vivid_protocol::scene::SceneNode as ProtocolSceneNode;
use vivid_protocol::surface::{SurfaceDefinition, SurfaceDescriptor, SurfaceState};
use vivid_protocol::target::DesktopTarget;
use vivid_protocol::track::{
    AudioGain, ImageConfiguration, KindConfiguration, MILESTONE_BUFFERED_ENDED,
    MILESTONE_CHANNEL_ACCEPTED, MILESTONE_CHANNEL_DETACHED, MILESTONE_CLOCK_STARTED,
    MILESTONE_DECODER_INITIALIZED, MILESTONE_EOS_ACCEPTED, MILESTONE_OUTPUT_READY,
    MILESTONE_PRESENTED, RasterConfiguration, TrackConfiguration, TrackMode, TrackState,
    VideoConfiguration,
};
use vivid_protocol::wire::{ConnectionKind, RECORD_OPTIONAL, Record};

use crate::transport::{Reader, Writer};
use crate::types::{
    BridgeClipRect, BridgeNode, BridgePlayRequest, BridgeSource, BridgeSourceDescriptor,
    BridgeSourceKey, BridgeSourceKind, BridgeSurface, BridgeSurfaceKey, PaneMediaNodeStatus,
    PaneMediaStatus, PaneMediaSurfaceStatus, PaneMediaTrackStatus,
};
use crate::types::{
    DeliveryMetrics, MediaConfig, PaneId, PaneMediaSurfaceDescriptor, PresenterConfig, RelayMetrics,
};
use crate::{PresenterListener, Transport};

const MAX_CONNECTIONS: usize = 64;
const MAX_SESSIONS: usize = 16;
const MAX_LEASES: usize = 64;
const MAX_WAITS: usize = 64;
const CHANNEL_OPEN_DEADLINE_US: u64 = 30_000_000;
const MAX_WAIT_US: u64 = 24 * 60 * 60 * 1_000_000;
const INITIAL_FLOW_RECORDS: u64 = 1;
// Start with one record so a newly projected video observes recovery promptly, then expand after
// the first completed delivery. Keeping the startup grant forever turns linked audio into a
// stop-and-wait stream across both bridge hops; one acknowledgement round trip per access unit is
// enough to underrun 21 ms AAC packets while video is active.
const ROLLING_FLOW_RECORDS: u64 = 8;
const MAX_ACTIVE_ANCHORS: usize = 256;
const MAX_DISCONNECT_GRACE_US: u64 = 10_000_000;

pub type ProducerId = u64;
pub type SourceKey = BridgeSourceKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyframeRequestOutcome {
    Forwarded,
    Damped,
    Ignored,
}

pub struct OuterMediaProjection<'a> {
    pub compatibility_revision: u64,
    pub apply_sequence: u64,
    pub bridge_instance_id: Option<u64>,
    pub bridge_local_revision: u64,
    pub attachment_generations: &'a HashMap<BridgeSourceKey, u64>,
}

#[derive(Debug, Clone)]
pub struct AudioSourceConfig {
    pub linked_video_source_id: Option<u64>,
    pub codec: String,
    pub packetization: String,
    pub extradata: Vec<u8>,
    pub sample_rate: u32,
    pub channels: u16,
    pub channel_mask: u64,
    pub bitrate: u64,
    pub max_access_unit_bytes: u32,
    pub codec_string: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SourceDescriptor {
    Raster(RasterConfiguration),
    Image(ImageConfiguration),
    Video(VideoConfiguration),
    Audio(AudioSourceConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlayRequest {
    pub start_pts_us: i64,
    pub minimum_buffer_us: u64,
    pub maximum_latency_us: u64,
    pub rate_32_32: i64,
    pub late_policy: u64,
    pub loop_count: u64,
    pub start_policy: u64,
}

impl PlayRequest {
    fn baseline() -> Self {
        Self {
            start_pts_us: 0,
            minimum_buffer_us: 1,
            maximum_latency_us: 1_000_000,
            rate_32_32: 1_i64 << 32,
            late_policy: 1,
            loop_count: 0,
            start_policy: 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SemanticDescriptor {
    pub role: u64,
    pub title: String,
    pub content_revision: u64,
    pub semantic_availability: u64,
    pub locator: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeConfig {
    pub node_id: u64,
    pub track: SourceKey,
    pub x: i64,
    pub y: i64,
    pub width: i64,
    pub height: i64,
    pub z_index: i64,
    pub visible: bool,
    pub anchor_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClipRect {
    pub x: i64,
    pub y: i64,
    pub width: i64,
    pub height: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SceneNodeConfig {
    pub node: NodeConfig,
    pub clip: Option<ClipRect>,
}

#[derive(Debug, Clone)]
pub struct SceneNode {
    pub producer: ProducerId,
    pub pane: PaneId,
    pub config: SceneNodeConfig,
}

#[derive(Debug, Clone)]
pub struct ProjectionSnapshot {
    pub revision: u64,
    pub surfaces: Vec<SnapshotSurface>,
    pub sources: Vec<SnapshotSource>,
    pub nodes: Vec<SceneNode>,
    pub live_nodes: Vec<(ProducerId, u64)>,
    pub videos_needing_keyframes: Vec<SourceKey>,
}

#[derive(Debug, Clone)]
pub struct BridgeProjection {
    pub surfaces: Vec<BridgeSurface>,
    pub sources: Vec<BridgeSource>,
    pub nodes: Vec<BridgeNode>,
}

impl ProjectionSnapshot {
    /// Converts validated inner objects into the owner-qualified outer bridge model without
    /// carrying any hop-local protocol identity, revision, generation, epoch, or key material.
    pub fn bridge_projection(&self) -> BridgeProjection {
        let surfaces = self
            .surfaces
            .iter()
            .map(|surface| BridgeSurface {
                key: BridgeSurfaceKey {
                    producer: surface.producer,
                    context: surface.context,
                    surface: surface.surface,
                },
                logical_width: surface.logical_width,
                logical_height: surface.logical_height,
                capture_policy: surface.capture_policy,
                descriptor: bridge_semantic_descriptor(&surface.semantic_descriptor),
            })
            .collect();
        let sources = self
            .sources
            .iter()
            .map(|source| BridgeSource {
                key: source.key,
                kind: bridge_source_kind(source),
                live: source.live,
                active: source.active,
                audio_gain: source.audio_gain.map(AudioGain::raw),
                capture_policy: source.capture_policy,
                descriptor: source
                    .semantic_descriptor
                    .as_ref()
                    .map(bridge_semantic_descriptor),
                playing: source.playing,
                play_request: BridgePlayRequest {
                    start_pts_us: source.play_request.start_pts_us,
                    minimum_buffer_us: source.play_request.minimum_buffer_us,
                    maximum_latency_us: source.play_request.maximum_latency_us,
                    rate_32_32: source.play_request.rate_32_32,
                    late_policy: source.play_request.late_policy,
                    loop_count: source.play_request.loop_count,
                    start_policy: source.play_request.start_policy,
                },
                eos_epoch: source.eos_epoch,
                causation_id: source.causation_id,
            })
            .collect();
        let nodes = self
            .nodes
            .iter()
            .map(|node| {
                let placement = node.config.node;
                let clip = node.config.clip.unwrap_or(ClipRect {
                    x: placement.x,
                    y: placement.y,
                    width: placement.width,
                    height: placement.height,
                });
                BridgeNode {
                    producer: node.producer,
                    node: placement.node_id,
                    fragment: 0,
                    surface: BridgeSurfaceKey {
                        producer: placement.track.producer,
                        context: placement.track.context,
                        surface: placement.track.surface,
                    },
                    x: placement.x,
                    y: placement.y,
                    width: placement.width,
                    height: placement.height,
                    z_index: placement.z_index,
                    visible: placement.visible,
                    clip: BridgeClipRect {
                        x: clip.x,
                        y: clip.y,
                        width: clip.width,
                        height: clip.height,
                    },
                }
            })
            .collect();
        BridgeProjection {
            surfaces,
            sources,
            nodes,
        }
    }
}

fn bridge_semantic_descriptor(descriptor: &SemanticDescriptor) -> BridgeSourceDescriptor {
    BridgeSourceDescriptor {
        role: descriptor.role,
        title: descriptor.title.clone(),
        content_revision: descriptor.content_revision,
        semantic_availability: descriptor.semantic_availability,
        locator: descriptor.locator.clone(),
    }
}

fn bridge_source_kind(source: &SnapshotSource) -> BridgeSourceKind {
    match &source.descriptor {
        SourceDescriptor::Raster(config) => BridgeSourceKind::Raster {
            width: config.width,
            height: config.height,
            alpha_mode: config.alpha_mode,
            compression_mode: u64::from(config.zstd_enabled),
            delta_operation_limit: source.raster_delta_operation_limit,
        },
        SourceDescriptor::Image(config) => BridgeSourceKind::Image {
            encoding: config.encoding,
            width: config.width,
            height: config.height,
            encoded_length: config.encoded_length,
            sha256: config.sha256,
        },
        SourceDescriptor::Video(config) => BridgeSourceKind::Video {
            codec: config.codec.clone(),
            packetization: config.packetization.clone(),
            extradata: config.extradata.clone(),
            width: config.coded_width,
            height: config.coded_height,
            profile: config.profile,
            level: config.level,
            bitrate: u64::from(config.maximum_access_unit_bytes)
                .saturating_mul(8)
                .saturating_mul(240),
            color_primaries: config.color_primaries,
            transfer: config.transfer,
            matrix: config.matrix,
            range: config.signal_range,
            sar_num: u32::try_from(config.aspect_numerator).unwrap_or(u32::MAX),
            sar_den: u32::try_from(config.aspect_denominator).unwrap_or(u32::MAX),
            max_access_unit_bytes: config.maximum_access_unit_bytes,
            codec_string: config.codec_string.clone(),
            decoder_config: config.decoder_configuration.clone(),
        },
        SourceDescriptor::Audio(config) => BridgeSourceKind::Audio {
            linked_video: config.linked_video_source_id.map(|track| BridgeSourceKey {
                producer: source.key.producer,
                context: source.key.context,
                surface: source.key.surface,
                track,
            }),
            codec: config.codec.clone(),
            packetization: config.packetization.clone(),
            extradata: config.extradata.clone(),
            sample_rate: config.sample_rate,
            channels: config.channels,
            channel_mask: config.channel_mask,
            bitrate: config.bitrate,
            max_access_unit_bytes: config.max_access_unit_bytes,
            codec_string: config.codec_string.clone(),
        },
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotSurface {
    pub producer: ProducerId,
    pub context: u64,
    pub surface: u64,
    pub logical_width: u64,
    pub logical_height: u64,
    pub capture_policy: u64,
    pub semantic_descriptor: SemanticDescriptor,
}

#[derive(Debug, Clone)]
pub struct SnapshotSource {
    pub key: SourceKey,
    pub descriptor: SourceDescriptor,
    pub live: bool,
    pub active: bool,
    pub audio_gain: Option<AudioGain>,
    /// Retained immutable media body, currently used by encoded-image tracks.
    pub retained: Option<Arc<[u8]>>,
    /// The fully composed latest raster, independent of the producer's delta chain.
    pub retained_raster: Option<RetainedRaster>,
    pub first_visible_presented: bool,
    pub playing: bool,
    pub play_request: PlayRequest,
    pub eos_epoch: Option<u32>,
    #[allow(dead_code)]
    pub last_inner_record_sequence: u64,
    pub causation_id: Option<[u8; messages::CAUSATION_ID_BYTES]>,
    pub capture_policy: u64,
    pub semantic_descriptor: Option<SemanticDescriptor>,
    pub raster_delta_operation_limit: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct RetainedRaster {
    pub epoch: u32,
    pub frame_id: u64,
    pub width: u32,
    pub height: u32,
    pub pixels: Arc<[u8]>,
}

#[derive(Debug)]
pub struct MediaEvent {
    pub delivery_id: u64,
    pub source: SourceKey,
    pub record_type: u16,
    pub recovered_keyframe: Option<(u32, i64)>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct GatewayLeaseReady {
    pub context_id: u64,
    pub lease_id: u64,
    pub permitted_profiles: Vec<String>,
    pub contract: ResourceContract,
    pub activation_timeout_us: u64,
    pub disconnect_grace_us: u64,
    pub cleanup_policy: CleanupPolicy,
    pub revision: u64,
}

impl GatewayLeaseReady {
    fn from_entry(entry: &LeaseEntry) -> Self {
        Self {
            context_id: entry.definition.context_id,
            lease_id: entry.definition.lease_id,
            permitted_profiles: entry.definition.permitted_profiles.clone(),
            contract: entry.contract.clone(),
            activation_timeout_us: entry.definition.activation_timeout_us,
            disconnect_grace_us: entry.definition.requested_disconnect_grace_us,
            cleanup_policy: entry.definition.cleanup_policy,
            revision: entry.revision,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SurfaceKey {
    session: u64,
    context: u64,
    surface: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TrackKey {
    surface: SurfaceKey,
    track: u64,
}

fn bridge_track_key(key: TrackKey) -> SourceKey {
    SourceKey {
        producer: key.surface.session,
        context: key.surface.context,
        surface: key.surface.surface,
        track: key.track,
    }
}

fn inner_track_key(key: SourceKey) -> TrackKey {
    TrackKey {
        surface: SurfaceKey {
            session: key.producer,
            context: key.context,
            surface: key.surface,
        },
        track: key.track,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeKey {
    session: u64,
    context: u64,
    node: u64,
}

struct SessionRuntime {
    pane: PaneId,
    closed: bool,
    accepted_profiles: HashSet<String>,
    session_tag: [u8; messages::SESSION_TAG_BYTES],
    channel_key: Secret32,
    anchor_key: AnchorKey,
    writer: Arc<Writer>,
    root_context: u64,
    scene_revision: SceneRevision,
    target_generation: TargetGeneration,
    anchors: HashMap<(u64, u64), (i32, usize)>,
    seen_anchors: HashSet<(u64, u64)>,
    cancelled_waits: HashSet<u64>,
    pending_waits: usize,
    lease: Option<(u64, u64)>,
    resume_key: Secret32,
    resume_generation: u64,
}

struct LeaseEntry {
    pane: PaneId,
    definition: SessionLeaseDefinition,
    contract: ResourceContract,
    revision: u64,
    issued_at: Instant,
    active_session: Option<u64>,
    machine: LeaseMachine,
    resume_key: Option<Secret32>,
    grace_deadline: Option<Instant>,
}

struct SurfaceEntry {
    state: SurfaceState,
    active_slots: HashMap<u64, u64>,
}

struct TrackEntry {
    configuration: TrackConfiguration,
    state: TrackState,
    audio_gain: AudioGain,
    channel_writer: Option<Arc<Writer>>,
    retained: Option<Arc<[u8]>>,
    retained_raster: Option<RetainedRaster>,
    playing: bool,
    play_request: PlayRequest,
    eos_epoch: Option<u32>,
    last_record_sequence: u64,
    last_pts_us: i64,
    outer_presented: bool,
    recovery_pending: bool,
    recovery_requested: bool,
    recovery_minimum_epoch: u32,
    /// The track worker has already read one record and is holding it behind an unapplied
    /// projection. A recovery request issued in that state must discard that record even when it
    /// happens to be random-access: the producer cannot observe the request until the blocked
    /// send returns, so accepting it would let the request overtake its own recovery keyframe.
    projection_blocked: bool,
    discard_blocked_for_recovery: bool,
    gate_linked_audio_for_recovery: bool,
    /// Linked audio below the replacement video's accepted recovery PTS is stale. Consume it
    /// locally so the physical audio clock restarts beside the recovered video rather than
    /// several seconds behind it.
    resume_after_pts_us: Option<i64>,
    causation_id: Option<[u8; messages::CAUSATION_ID_BYTES]>,
}

#[derive(Clone)]
enum NodeMutation {
    Create(ProtocolSceneNode),
    Update(ProtocolSceneNode),
    Delete(NodeKey),
}

struct NodeEntry {
    pane: PaneId,
    node: ProtocolSceneNode,
}

struct PendingDelivery {
    track: TrackKey,
    bytes: u64,
    random_access: bool,
}

#[derive(Clone)]
struct CachedMutation {
    fingerprint: [u8; 32],
    record_type: u16,
    object_id: u64,
    body: Vec<u8>,
}

#[derive(Clone, Copy)]
struct Metrics {
    generation: u64,
    viewport_width: u32,
    viewport_height: u32,
    columns: u32,
    rows: u32,
    cell_width: u32,
    cell_height: u32,
}

#[derive(Clone)]
struct TargetState {
    generation: u64,
    descriptor: Vec<(u64, Value)>,
}

struct State {
    config: PresenterConfig,
    presenter: PresenterInstanceId,
    capabilities: HashMap<PaneId, Secret32>,
    leases: HashMap<(u64, u64), LeaseEntry>,
    metrics: HashMap<PaneId, Metrics>,
    targets: HashMap<PaneId, TargetState>,
    sessions: HashMap<u64, SessionRuntime>,
    surfaces: HashMap<SurfaceKey, SurfaceEntry>,
    tracks: HashMap<TrackKey, TrackEntry>,
    nodes: HashMap<NodeKey, NodeEntry>,
    transactions: HashMap<(u64, u64, u64), Vec<NodeMutation>>,
    next_session: u64,
    projection_revision: u64,
    projected_sources: HashSet<SourceKey>,
    deliveries: HashMap<u64, PendingDelivery>,
    idempotency: HashMap<(u64, [u8; messages::IDEMPOTENCY_KEY_BYTES]), CachedMutation>,
    idempotency_order: std::collections::VecDeque<(u64, [u8; messages::IDEMPOTENCY_KEY_BYTES])>,
    next_delivery: u64,
    events: Option<mpsc::SyncSender<MediaEvent>>,
    media_wakeup: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(any(test, feature = "testing"))]
    play_commands: Vec<BridgeSourceKey>,
    connections: usize,
    delivery_metrics: DeliveryMetrics,
}

pub struct VirtualVivid {
    endpoint: String,
    state: Arc<Mutex<State>>,
    delivery_changed: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
}

impl VirtualVivid {
    #[allow(dead_code)]
    pub fn start<L: PresenterListener>(listener: L, config: MediaConfig) -> io::Result<Self> {
        Self::start_configured(listener, PresenterConfig::terminal(config), None)
    }

    pub fn start_with_events<L: PresenterListener>(
        listener: L,
        config: MediaConfig,
        events: Option<mpsc::SyncSender<MediaEvent>>,
    ) -> io::Result<Self> {
        Self::start_configured(listener, PresenterConfig::terminal(config), events)
    }

    pub fn start_configured<L: PresenterListener>(
        listener: L,
        mut config: PresenterConfig,
        events: Option<mpsc::SyncSender<MediaEvent>>,
    ) -> io::Result<Self> {
        config.supported_profiles.sort();
        config.supported_profiles.dedup();
        if !config
            .supported_profiles
            .iter()
            .any(|profile| profile == registry::CORE_CONTROL)
            || !config
                .supported_profiles
                .iter()
                .any(|profile| profile == config.target.profile_name())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "supported profiles must contain core and the selected target profile",
            ));
        }
        registry::validate_profile_set(config.supported_profiles.iter().map(String::as_str))
            .map_err(io::Error::other)?;
        config
            .target
            .validate_configuration()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let advertised_endpoint = listener.endpoint();
        let mut presenter = [0_u8; 16];
        getrandom::fill(&mut presenter).map_err(io::Error::other)?;
        let state = Arc::new(Mutex::new(State {
            config,
            presenter: PresenterInstanceId(presenter),
            capabilities: HashMap::new(),
            leases: HashMap::new(),
            metrics: HashMap::new(),
            targets: HashMap::new(),
            sessions: HashMap::new(),
            surfaces: HashMap::new(),
            tracks: HashMap::new(),
            nodes: HashMap::new(),
            transactions: HashMap::new(),
            next_session: 0,
            projection_revision: 0,
            projected_sources: HashSet::new(),
            deliveries: HashMap::new(),
            idempotency: HashMap::new(),
            idempotency_order: std::collections::VecDeque::new(),
            next_delivery: 0,
            events,
            media_wakeup: None,
            #[cfg(any(test, feature = "testing"))]
            play_commands: Vec::new(),
            connections: 0,
            delivery_metrics: DeliveryMetrics::default(),
        }));
        let delivery_changed = Arc::new(Condvar::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let service = Self {
            endpoint: advertised_endpoint,
            state: state.clone(),
            delivery_changed: delivery_changed.clone(),
            shutdown: shutdown.clone(),
        };
        thread::Builder::new()
            .name("vvmux-vivid-1.5-listener".into())
            .spawn(move || accept_loop(listener, state, delivery_changed, shutdown))?;
        Ok(service)
    }

    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub fn set_media_wakeup(&self, wakeup: Arc<dyn Fn() + Send + Sync>) {
        lock(&self.state).media_wakeup = Some(wakeup);
    }

    pub fn issue_pane_capability(&self, pane: PaneId) -> io::Result<String> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).map_err(io::Error::other)?;
        let mut state = lock(&self.state);
        state.capabilities.insert(pane, Secret32::new(bytes));
        if let Some(descriptor) = state.config.target.initial_descriptor() {
            state.targets.insert(
                pane,
                TargetState {
                    generation: 1,
                    descriptor,
                },
            );
        }
        Ok(hex(&bytes))
    }

    pub fn revoke_pane(&self, pane: PaneId) {
        let mut state = lock(&self.state);
        state.capabilities.remove(&pane);
        let sessions = state
            .sessions
            .iter()
            .filter_map(|(id, session)| (session.pane == pane).then_some(*id))
            .collect::<Vec<_>>();
        for session in sessions {
            cleanup_session(&mut state, session);
        }
        state.metrics.remove(&pane);
        state.targets.remove(&pane);
        advance_projection(&mut state);
    }

    /// Registers a controller-minted activation verifier for one delegated inner session.
    ///
    /// The activation secret never crosses this API: the controller retains it and the presenter
    /// stores only the protocol verifier carried by `SessionLeaseDefinition`.
    pub fn issue_lease(
        &self,
        pane: PaneId,
        mut definition: SessionLeaseDefinition,
    ) -> io::Result<GatewayLeaseReady> {
        definition.validate().map_err(io::Error::other)?;
        definition.requested_disconnect_grace_us = match definition.cleanup_policy {
            CleanupPolicy::Immediate => 0,
            CleanupPolicy::SuspendOnUncleanLoss => definition
                .requested_disconnect_grace_us
                .min(MAX_DISCONNECT_GRACE_US),
        };
        let mut state = lock(&self.state);
        if !definition
            .permitted_profiles
            .iter()
            .all(|profile| state.config.supported_profiles.contains(profile))
            || !definition
                .permitted_profiles
                .iter()
                .any(|profile| profile == state.config.target.profile_name())
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "lease profile set is not supported by this presentation target",
            ));
        }
        let ceiling = state.config.resource_contract.clone().unwrap_or_else(|| {
            presenter_contract(&state.config.media, state.config.target.as_ref())
        });
        let contract = definition.requested_contract.component_min(&ceiling);
        let key = (definition.context_id, definition.lease_id);
        if let Some(existing) = state.leases.get(&key) {
            if existing.pane != pane || existing.definition != definition {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "lease identity is already bound to different terms",
                ));
            }
            return Ok(GatewayLeaseReady::from_entry(existing));
        }
        if state.leases.len() >= MAX_LEASES {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "inner lease capacity is exhausted",
            ));
        }
        if let Some(descriptor) = state.config.target.initial_descriptor() {
            state.targets.entry(pane).or_insert(TargetState {
                generation: 1,
                descriptor,
            });
        }
        let entry = LeaseEntry {
            pane,
            machine: LeaseMachine::new(
                definition.cleanup_policy,
                definition.requested_disconnect_grace_us,
            ),
            definition,
            contract,
            revision: 1,
            issued_at: Instant::now(),
            active_session: None,
            resume_key: None,
            grace_deadline: None,
        };
        let ready = GatewayLeaseReady::from_entry(&entry);
        state.leases.insert(key, entry);
        Ok(ready)
    }

    /// Revokes one exact lease and removes only the session and objects owned by it.
    pub fn revoke_lease(&self, context_id: u64, lease_id: u64) -> io::Result<()> {
        let mut state = lock(&self.state);
        let key = (context_id, lease_id);
        let Some(lease) = state.leases.remove(&key) else {
            return Ok(());
        };
        if let Some(session) = lease.active_session {
            cleanup_session(&mut state, session);
        }
        if !state
            .leases
            .values()
            .any(|candidate| candidate.pane == lease.pane)
        {
            state.targets.remove(&lease.pane);
        }
        advance_projection(&mut state);
        Ok(())
    }

    pub fn update_metrics(&self, pane: PaneId, columns: u16, rows: u16, cell: (u16, u16)) {
        if columns == 0 || rows == 0 || cell.0 == 0 || cell.1 == 0 {
            return;
        }
        let mut state = lock(&self.state);
        if !state.config.target.accepts_anchors() {
            return;
        }
        let generation = state
            .metrics
            .get(&pane)
            .and_then(|metrics| metrics.generation.checked_add(1))
            .unwrap_or(1);
        let metrics = Metrics {
            generation,
            viewport_width: u32::from(columns) * u32::from(cell.0),
            viewport_height: u32::from(rows) * u32::from(cell.1),
            columns: u32::from(columns),
            rows: u32::from(rows),
            cell_width: u32::from(cell.0),
            cell_height: u32::from(cell.1),
        };
        state.metrics.insert(pane, metrics);
        let target = TargetState {
            generation,
            descriptor: target_descriptor(metrics),
        };
        state.targets.insert(pane, target.clone());
        announce_target_change(&mut state, pane, &target, 0x1f);
    }

    /// Advances one desktop principal's target and announces the new descriptor to its live
    /// session. The configured desktop is used as generation one when the capability is issued.
    pub fn update_desktop_target(
        &self,
        pane: PaneId,
        target: DesktopTarget,
        reason_mask: u64,
    ) -> io::Result<u64> {
        target.validate().map_err(io::Error::other)?;
        let mut state = lock(&self.state);
        if state.config.target.profile_name() != registry::DESKTOP_SURFACE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "desktop target updates require a desktop presenter",
            ));
        }
        if !state.capabilities.contains_key(&pane) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "presentation principal is unknown",
            ));
        }
        let generation = state
            .targets
            .get(&pane)
            .and_then(|current| current.generation.checked_add(1))
            .ok_or_else(|| io::Error::other("target generation exhausted"))?;
        let target = TargetState {
            generation,
            descriptor: target.encode(),
        };
        state.targets.insert(pane, target.clone());
        announce_target_change(&mut state, pane, &target, reason_mask);
        Ok(generation)
    }
}

fn announce_target_change(state: &mut State, pane: PaneId, target: &TargetState, reason_mask: u64) {
    for session in state
        .sessions
        .values_mut()
        .filter(|session| session.pane == pane && !session.closed)
    {
        session.target_generation = TargetGeneration::new(target.generation);
        // The descriptor is inline: keys 0..=8 are the target itself, 9 its generation, and
        // 10 the reason mask. A nested producer validates that shape exactly, so a wrapped
        // descriptor leaves it stuck on the generation it read at WELCOME and every scene
        // commit it makes afterwards is rejected as stale.
        let body = Envelope::new(0, target_changed_payload(target, reason_mask)).encode();
        if let Ok(body) = body {
            let _ = session
                .writer
                .write_record(messages::TARGET_CHANGED, 0, &body);
        }
    }
}
impl VirtualVivid {
    pub fn notify_capabilities_changed(&self, reason_mask: u64) -> io::Result<u64> {
        let state = lock(&self.state);
        let body = Envelope::new(
            0,
            vec![(0, Value::Unsigned(1)), (1, Value::Unsigned(reason_mask))],
        )
        .encode()?;
        for session in state.sessions.values().filter(|session| !session.closed) {
            let _ = session
                .writer
                .write_record(messages::CAPS_CHANGED, 0, &body);
        }
        Ok(1)
    }

    pub fn observe_marker(&self, pane: PaneId, value: &str, row: i32, column: usize) {
        let marker = anchor::parse_marker(value).or_else(|_| anchor::parse_conpty_marker(value));
        let Ok(marker) = marker else {
            return;
        };
        let mut state = lock(&self.state);
        if !state.config.target.accepts_anchors() {
            return;
        }
        let Some(session) = state.sessions.values_mut().find(|session| {
            !session.closed
                && session.pane == pane
                && session.session_tag == marker.session_tag
                && anchor::verify_marker(&session.anchor_key, &marker)
        }) else {
            return;
        };
        let key = (marker.context_id, marker.anchor_id);
        if session.seen_anchors.len() >= MAX_ACTIVE_ANCHORS && !session.seen_anchors.contains(&key)
        {
            return;
        }
        session.seen_anchors.insert(key);
        session.anchors.insert(key, (row, column));
        let body = Envelope::new(
            0,
            vec![
                (0, Value::Unsigned(marker.context_id)),
                (1, Value::Unsigned(marker.anchor_id)),
                (
                    2,
                    Value::Unsigned(u64::try_from(column).unwrap_or(u64::MAX)),
                ),
                (3, nonnegative(row)),
                (4, Value::Bool(true)),
                (5, Value::Unsigned(session.target_generation.get())),
            ],
        )
        .encode();
        if let Ok(body) = body {
            let _ = session
                .writer
                .write_record(messages::ANCHOR_READY, marker.anchor_id, &body);
        }
        advance_projection(&mut state);
    }

    pub fn scroll_anchors(&self, pane: PaneId, lines: i32) {
        let mut state = lock(&self.state);
        if !state.config.target.accepts_anchors() {
            return;
        }
        for session in state
            .sessions
            .values_mut()
            .filter(|session| session.pane == pane)
        {
            for (line, _) in session.anchors.values_mut() {
                *line = line.saturating_sub(lines);
            }
        }
        advance_projection(&mut state);
    }

    pub fn clear_anchors(&self, pane: PaneId) {
        let mut state = lock(&self.state);
        if !state.config.target.accepts_anchors() {
            return;
        }
        for session in state
            .sessions
            .values_mut()
            .filter(|session| session.pane == pane)
        {
            let anchors = std::mem::take(&mut session.anchors);
            for ((context, anchor), _) in anchors {
                if let Ok(body) = Envelope::new(
                    0,
                    vec![
                        (0, Value::Unsigned(context)),
                        (1, Value::Unsigned(anchor)),
                        (2, Value::Unsigned(1)),
                    ],
                )
                .encode()
                {
                    let _ = session
                        .writer
                        .write_record(messages::ANCHOR_GONE, anchor, &body);
                }
            }
        }
        advance_projection(&mut state);
    }

    pub fn pane_for_source(&self, source: SourceKey) -> Option<PaneId> {
        let state = lock(&self.state);
        state
            .sessions
            .get(&source.producer)
            .map(|session| session.pane)
    }

    pub fn revision(&self) -> u64 {
        lock(&self.state).projection_revision
    }

    /// Drain the exact track identities named by accepted PLAY commands.
    ///
    /// This test-support hook intentionally records the command target before linked surface state
    /// is propagated. Snapshot state alone cannot distinguish PLAY(video) from PLAY(audio), while
    /// a real presenter starts the physical audio device only for the latter.
    #[cfg(any(test, feature = "testing"))]
    pub fn take_play_commands(&self) -> Vec<BridgeSourceKey> {
        std::mem::take(&mut lock(&self.state).play_commands)
    }

    #[allow(dead_code)]
    pub fn projection_snapshot(&self, panes: &HashSet<PaneId>) -> ProjectionSnapshot {
        self.projection_snapshot_inner(panes, &HashMap::new(), true)
    }

    pub fn projection_snapshot_with_viewports(
        &self,
        panes: &HashSet<PaneId>,
        viewport_offsets: &HashMap<PaneId, usize>,
    ) -> ProjectionSnapshot {
        self.projection_snapshot_inner(panes, viewport_offsets, true)
    }

    /// Build a projection without exposing newly visible timed sources to their workers.
    ///
    /// A relay uses this form while it submits the snapshot to a physical presenter. It must call
    /// [`Self::activate_bridge_projection`] only after that presenter acknowledges the exact
    /// snapshot. Sources removed by the snapshot are parked immediately.
    pub fn prepare_projection_snapshot_with_viewports(
        &self,
        panes: &HashSet<PaneId>,
        viewport_offsets: &HashMap<PaneId, usize>,
    ) -> ProjectionSnapshot {
        self.projection_snapshot_inner(panes, viewport_offsets, false)
    }

    fn projection_snapshot_inner(
        &self,
        panes: &HashSet<PaneId>,
        viewport_offsets: &HashMap<PaneId, usize>,
        activate_immediately: bool,
    ) -> ProjectionSnapshot {
        let mut state = lock(&self.state);
        let sessions = state
            .sessions
            .iter()
            .filter_map(|(id, session)| panes.contains(&session.pane).then_some(*id))
            .collect::<HashSet<_>>();
        let surfaces = state
            .surfaces
            .iter()
            .filter(|(key, _)| sessions.contains(&key.session))
            .map(|(key, surface)| SnapshotSurface {
                producer: key.session,
                context: key.context,
                surface: key.surface,
                logical_width: surface.state.definition.logical_width,
                logical_height: surface.state.definition.logical_height,
                capture_policy: surface.state.definition.policy,
                semantic_descriptor: semantic_descriptor(&surface.state.definition.descriptor),
            })
            .collect::<Vec<_>>();
        let sources = state
            .tracks
            .iter()
            .filter(|(key, _)| sessions.contains(&key.surface.session))
            .map(|(key, track)| SnapshotSource {
                key: bridge_track_key(*key),
                descriptor: source_descriptor(&state.tracks, *key, track),
                live: track.configuration.mode == TrackMode::Live,
                active: state.surfaces.get(&key.surface).is_some_and(|surface| {
                    surface
                        .active_slots
                        .values()
                        .any(|track_id| *track_id == key.track)
                }),
                audio_gain: (matches!(track.configuration.kind, KindConfiguration::Audio(_))
                    && state
                        .sessions
                        .get(&key.surface.session)
                        .is_some_and(|session| {
                            session.accepted_profiles.contains(registry::AUDIO_GAIN)
                        }))
                .then_some(track.audio_gain),
                retained: track.retained.clone(),
                retained_raster: track.retained_raster.clone(),
                first_visible_presented: track.outer_presented,
                playing: track.playing,
                play_request: track.play_request,
                eos_epoch: track.eos_epoch,
                last_inner_record_sequence: track.last_record_sequence,
                causation_id: track.causation_id,
                capture_policy: state
                    .surfaces
                    .get(&key.surface)
                    .map_or(0, |surface| surface.state.definition.policy),
                semantic_descriptor: state
                    .surfaces
                    .get(&key.surface)
                    .map(|surface| semantic_descriptor(&surface.state.definition.descriptor)),
                raster_delta_operation_limit: match &track.configuration.kind {
                    KindConfiguration::Raster(config) if config.delta_enabled => {
                        Some(u32::from(config.maximum_delta_operations))
                    }
                    _ => None,
                },
            })
            .collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for (key, entry) in &state.nodes {
            if !sessions.contains(&key.session) {
                continue;
            }
            let surface = SurfaceKey {
                session: key.session,
                context: entry.node.surface_context_id,
                surface: entry.node.surface_id,
            };
            let Some(track_key) = selected_visual_track(&state, surface) else {
                continue;
            };
            let Some(session) = state.sessions.get(&key.session) else {
                continue;
            };
            let Some(config) = projected_node_config(
                &entry.node,
                bridge_track_key(track_key),
                session,
                *viewport_offsets.get(&entry.pane).unwrap_or(&0),
                state.config.target.profile_name(),
                state.config.target.extent(),
            ) else {
                continue;
            };
            nodes.push(SceneNode {
                producer: key.session,
                pane: entry.pane,
                config,
            });
        }
        let live_nodes = state
            .nodes
            .keys()
            .map(|key| (key.session, key.node))
            .collect::<Vec<_>>();
        let projected_sources = sources
            .iter()
            .map(|source| source.key)
            .collect::<HashSet<_>>();
        let hidden_sources = state
            .projected_sources
            .difference(&projected_sources)
            .copied()
            .collect::<Vec<_>>();
        let hidden_tracks = hidden_sources
            .iter()
            .copied()
            .map(inner_track_key)
            .collect::<HashSet<_>>();
        for source in &hidden_sources {
            let Some(track) = state.tracks.get_mut(&inner_track_key(*source)) else {
                continue;
            };
            if track.playing && matches!(track.configuration.kind, KindConfiguration::Video(_)) {
                // A hidden tab removes its outer decoder just like a detached foreground client.
                // Re-arm recovery before allowing the source into a later projection so no
                // inter-frame packet can become the first input to the replacement decoder.
                track.recovery_pending = true;
                track.recovery_requested = false;
                track.recovery_minimum_epoch = track.state.media_epoch;
                track.discard_blocked_for_recovery = false;
                track.gate_linked_audio_for_recovery = true;
            }
        }
        // Media events already admitted for a source can still be sitting in the session actor's
        // dedicated queue when the tab falls out of projection. Retire their delivery identities
        // now; otherwise their event shells can be forwarded after re-apply and splice old-epoch
        // packets behind the replacement keyframe.
        let stale_deliveries = state
            .deliveries
            .iter()
            .filter_map(|(delivery_id, delivery)| {
                hidden_tracks
                    .contains(&delivery.track)
                    .then_some(*delivery_id)
            })
            .collect::<Vec<_>>();
        for delivery_id in &stale_deliveries {
            if let Some(delivery) = state.deliveries.remove(delivery_id) {
                release_delivery_allowance(&mut state, &delivery);
            }
        }
        if activate_immediately {
            state.projected_sources = projected_sources;
        } else {
            // Falling edges are immediate: once a tab is hidden, no later timed packet may enter
            // the outer bridge being torn down. Rising edges wait for the exact applied ack.
            state
                .projected_sources
                .retain(|source| projected_sources.contains(source));
        }
        let videos_needing_keyframes = state
            .tracks
            .iter()
            .filter_map(|(key, track)| {
                (sessions.contains(&key.surface.session)
                    && track.recovery_pending
                    && (track.recovery_requested
                        || track.gate_linked_audio_for_recovery
                        || track.outer_presented)
                    && matches!(track.configuration.kind, KindConfiguration::Video(_)))
                .then_some(bridge_track_key(*key))
            })
            .collect::<Vec<_>>();
        let snapshot = ProjectionSnapshot {
            revision: state.projection_revision,
            surfaces,
            sources,
            nodes,
            live_nodes,
            videos_needing_keyframes,
        };
        drop(state);
        if activate_immediately || !stale_deliveries.is_empty() {
            self.delivery_changed.notify_all();
        }
        snapshot
    }

    /// Publish the exact source set acknowledged by the foreground physical presenter.
    pub fn activate_bridge_projection(&self, sources: &HashSet<SourceKey>) {
        lock(&self.state).projected_sources = sources.clone();
        self.delivery_changed.notify_all();
    }

    pub fn deactivate_bridge(&self) {
        let mut state = lock(&self.state);
        state.projected_sources.clear();
        for track in state.tracks.values_mut() {
            if matches!(track.configuration.kind, KindConfiguration::Video(_)) && track.playing {
                track.recovery_pending = true;
                // A later foreground presenter owns a fresh decoder. Re-arm producer recovery for
                // that handoff instead of damping its first NEED_KEYFRAME against the request
                // state owned by the presenter that just detached.
                track.recovery_requested = false;
                track.recovery_minimum_epoch = track.state.media_epoch;
                track.discard_blocked_for_recovery = false;
                track.gate_linked_audio_for_recovery = true;
            }
        }
        let deliveries = std::mem::take(&mut state.deliveries);
        for delivery in deliveries.values() {
            release_delivery_allowance(&mut state, delivery);
        }
        drop(state);
        // Wake both ordinary delivery waiters and timed channel workers observing the projection.
        // The latter will remain parked until a new foreground projection includes their source.
        self.delivery_changed.notify_all();
    }

    pub fn complete_bridge_delivery(&self, delivery_id: u64, delivered: bool) -> bool {
        let mut state = lock(&self.state);
        let Some(delivery) = state.deliveries.remove(&delivery_id) else {
            return false;
        };
        let mut resync = !delivered;
        let mut recovery_completed = false;
        release_delivery_allowance(&mut state, &delivery);
        if let Some(track) = state.tracks.get_mut(&delivery.track) {
            if delivered {
                track.outer_presented = true;
                track.state.milestones |= MILESTONE_PRESENTED;
                if delivery.random_access && track.recovery_pending {
                    // A keyframe has recovered the nested path only after the foreground bridge
                    // confirms that it reached the outer track. Clearing this at inner ingest
                    // lets a delayed recovery request overtake the delivery acknowledgement and
                    // incorrectly send Vivi back into recovery after its good keyframe.
                    track.recovery_pending = false;
                    track.recovery_requested = false;
                    track.recovery_minimum_epoch = track.state.media_epoch;
                    track.discard_blocked_for_recovery = false;
                    track.gate_linked_audio_for_recovery = false;
                    recovery_completed = true;
                }
            } else {
                if !track.recovery_pending || delivery.random_access {
                    // The failed packet starts a new recovery episode, while a failed random-access
                    // packet means the previously requested recovery was not usable.
                    track.recovery_requested = false;
                    track.discard_blocked_for_recovery = false;
                }
                track.recovery_pending = true;
                resync = true;
                if matches!(track.configuration.kind, KindConfiguration::Video(_)) {
                    track.gate_linked_audio_for_recovery = true;
                }
            }
        }
        let wakeup = recovery_completed
            .then(|| {
                advance_projection(&mut state);
                state.media_wakeup.clone()
            })
            .flatten();
        drop(state);
        self.delivery_changed.notify_all();
        if let Some(wakeup) = wakeup {
            // Publish the falling edge of videos_needing_keyframes so the bridge can arm a later,
            // genuinely new recovery episode.
            wakeup();
        }
        resync
    }

    /// Release a delivery superseded by a newer outer attachment generation.
    ///
    /// This returns its bounded ingress allowance without claiming that the old packet was
    /// presented and without turning an intentional recovery discard into a new source loss.
    pub fn release_bridge_delivery(&self, delivery_id: u64) -> bool {
        let mut state = lock(&self.state);
        let Some(delivery) = state.deliveries.remove(&delivery_id) else {
            return false;
        };
        release_delivery_allowance(&mut state, &delivery);
        drop(state);
        self.delivery_changed.notify_all();
        true
    }

    /// Whether an actor-queued event still owns the exact live bridge delivery it names.
    pub fn bridge_delivery_is_pending(&self, delivery_id: u64, source: SourceKey) -> bool {
        lock(&self.state)
            .deliveries
            .get(&delivery_id)
            .is_some_and(|delivery| delivery.track == inner_track_key(source))
    }

    pub fn complete_retained_hydration(&self, source: SourceKey) {
        let mut state = lock(&self.state);
        if let Some(track) = state.tracks.get_mut(&inner_track_key(source)) {
            track.outer_presented = true;
            track.state.milestones |= MILESTONE_PRESENTED;
        }
    }

    pub fn request_keyframe(
        &self,
        source: SourceKey,
        minimum_epoch: Option<u32>,
        reason: u64,
    ) -> KeyframeRequestOutcome {
        let mut state = lock(&self.state);
        let key = inner_track_key(source);
        let recovery_inflight = state
            .deliveries
            .values()
            .any(|delivery| delivery.track == key && delivery.random_access);
        let Some(track) = state.tracks.get_mut(&key) else {
            return KeyframeRequestOutcome::Ignored;
        };
        if !matches!(track.configuration.kind, KindConfiguration::Video(_)) {
            return KeyframeRequestOutcome::Ignored;
        }
        if track.recovery_pending && (track.recovery_requested || recovery_inflight) {
            return KeyframeRequestOutcome::Damped;
        }
        track.recovery_pending = true;
        let replacement_handoff =
            track.gate_linked_audio_for_recovery || track.playing || track.outer_presented;
        let requested_epoch = if reason == crate::outer::KEYFRAME_REASON_TRANSPORT_LOSS {
            minimum_epoch.unwrap_or(0).max(if replacement_handoff {
                track.state.media_epoch.saturating_add(1)
            } else {
                track.state.media_epoch
            })
        } else {
            minimum_epoch
                .unwrap_or(0)
                .max(track.state.media_epoch.saturating_add(1))
        };
        if !send_need_keyframe(key, track, requested_epoch, reason) {
            return KeyframeRequestOutcome::Ignored;
        }
        track.recovery_requested = true;
        track.recovery_minimum_epoch = requested_epoch;
        // The channel event is observed only after an already-blocked media send returns. Do not
        // let that pre-request record masquerade as the requested recovery keyframe.
        track.discard_blocked_for_recovery = track.projection_blocked;
        if replacement_handoff {
            track.gate_linked_audio_for_recovery = true;
        }
        KeyframeRequestOutcome::Forwarded
    }

    pub fn request_full_frames(&self, sources: &[SourceKey], _reason: u64) {
        let mut state = lock(&self.state);
        for source in sources {
            let key = inner_track_key(*source);
            let Some(track) = state.tracks.get_mut(&key) else {
                continue;
            };
            if matches!(track.configuration.kind, KindConfiguration::Raster(_)) {
                track.recovery_pending = true;
                send_need_full_frame(key, track);
            }
        }
    }

    pub fn apply_outer_playback(&self, source: SourceKey, state_value: u64, eos_state: u64) {
        let mut state = lock(&self.state);
        let key = inner_track_key(source);
        if let Some(track) = state.tracks.get_mut(&key) {
            if state_value >= 2 {
                track.state.milestones |= MILESTONE_CLOCK_STARTED;
            }
            if eos_state >= 1 {
                track.state.milestones |= MILESTONE_EOS_ACCEPTED;
            }
            if eos_state >= 2 {
                track.state.milestones |= MILESTONE_BUFFERED_ENDED;
            }
        }
    }

    pub fn pane_status(
        &self,
        pane: PaneId,
        outer: OuterMediaProjection<'_>,
        relay: RelayMetrics,
    ) -> PaneMediaStatus {
        let state = lock(&self.state);
        let sessions = state
            .sessions
            .iter()
            .filter_map(|(id, session)| (session.pane == pane).then_some(*id))
            .collect::<HashSet<_>>();
        let mut surfaces = state
            .surfaces
            .iter()
            .filter(|(key, _)| sessions.contains(&key.session))
            .map(|(key, surface)| {
                let descriptor = &surface.state.definition.descriptor;
                let mut active_slots = surface
                    .active_slots
                    .iter()
                    .map(|(slot, track)| (*slot, *track))
                    .collect::<Vec<_>>();
                active_slots.sort_unstable();
                PaneMediaSurfaceStatus {
                    producer_id: key.session,
                    context_id: key.context,
                    surface_id: key.surface,
                    lifecycle: "live".into(),
                    surface_revision: surface.state.revision.get(),
                    surface_generation: surface.state.generation.get(),
                    visible: state.projected_sources.iter().any(|track| {
                        track.producer == key.session
                            && track.context == key.context
                            && track.surface == key.surface
                    }),
                    capture_policy: surface.state.definition.policy,
                    descriptor: Some(PaneMediaSurfaceDescriptor {
                        role: descriptor.role as u64,
                        title: Some(descriptor.title.clone()),
                        content_revision: Some(descriptor.semantic_content_revision),
                        semantic_availability: Some(descriptor.semantic_availability),
                        locator: Some(descriptor.locator_hint.clone()),
                    }),
                    active_slots,
                }
            })
            .collect::<Vec<_>>();
        surfaces
            .sort_by_key(|surface| (surface.producer_id, surface.context_id, surface.surface_id));
        let mut tracks = state
            .tracks
            .iter()
            .filter(|(key, _)| sessions.contains(&key.surface.session))
            .map(|(key, track)| {
                let source = bridge_track_key(*key);
                PaneMediaTrackStatus {
                    producer_id: source.producer,
                    context_id: source.context,
                    surface_id: source.surface,
                    track_id: source.track,
                    kind: kind_name(&track.configuration.kind).into(),
                    lifecycle: if track.state.lost {
                        "lost"
                    } else if track.eos_epoch.is_some() {
                        "ended"
                    } else if track.playing {
                        "playing"
                    } else {
                        "live"
                    }
                    .into(),
                    track_revision: track.state.revision.get(),
                    epoch: track.state.media_epoch,
                    channel_state: if track.channel_writer.is_some() { 1 } else { 0 },
                    inner_channel_generation: track.state.channel_generation.get(),
                    outer_channel_generation: outer.attachment_generations.get(&source).copied(),
                    outer_mapping_fresh: outer.bridge_instance_id.is_some(),
                    visible: state.projected_sources.contains(&source),
                    retained_static: has_retained_media(track),
                    keyframe_needed: track.recovery_pending,
                    milestones: track.state.milestones,
                    queued_packets: state
                        .deliveries
                        .values()
                        .filter(|delivery| delivery.track == *key)
                        .count() as u64,
                    queued_bytes: state
                        .deliveries
                        .values()
                        .filter(|delivery| delivery.track == *key)
                        .map(|delivery| delivery.bytes)
                        .sum(),
                    available_packet_credit: track
                        .state
                        .flow
                        .maximum_media_records
                        .saturating_sub(track.state.flow.sent_media_records),
                    available_byte_credit: track
                        .state
                        .flow
                        .maximum_body_bytes
                        .saturating_sub(track.state.flow.sent_body_bytes),
                }
            })
            .collect::<Vec<_>>();
        tracks.sort_by_key(|track| {
            (
                track.producer_id,
                track.context_id,
                track.surface_id,
                track.track_id,
            )
        });
        let mut nodes = state
            .nodes
            .iter()
            .filter(|(key, entry)| sessions.contains(&key.session) && entry.pane == pane)
            .filter_map(|(key, entry)| {
                let surface = SurfaceKey {
                    session: key.session,
                    context: entry.node.surface_context_id,
                    surface: entry.node.surface_id,
                };
                let track = selected_visual_track(&state, surface)?;
                let config = projected_node_config(
                    &entry.node,
                    bridge_track_key(track),
                    state.sessions.get(&key.session)?,
                    0,
                    state.config.target.profile_name(),
                    state.config.target.extent(),
                )?;
                Some(PaneMediaNodeStatus {
                    producer_id: key.session,
                    context_id: entry.node.owning_context_id,
                    node_id: key.node,
                    surface_context_id: entry.node.surface_context_id,
                    surface_id: entry.node.surface_id,
                    visible: config.node.visible,
                    x: config.node.x,
                    y: config.node.y,
                    width: config.node.width,
                    height: config.node.height,
                })
            })
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| (node.producer_id, node.node_id));
        let virtual_scene_revision = state
            .sessions
            .iter()
            .filter(|(_, session)| session.pane == pane)
            .map(|(_, session)| session.scene_revision.get())
            .max()
            .unwrap_or(0);
        PaneMediaStatus {
            virtual_projection_revision: state.projection_revision,
            virtual_scene_revision,
            outer_projection_revision: outer.compatibility_revision,
            outer_apply_sequence: outer.apply_sequence,
            bridge_instance_id: outer.bridge_instance_id,
            bridge_local_revision: outer.bridge_local_revision,
            surfaces,
            tracks,
            nodes,
            relay: RelayMetrics {
                delivery: state.delivery_metrics,
                ..relay
            },
        }
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn wait_for_retained_media(&self, pane: PaneId, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = lock(&self.state);
        loop {
            if state.tracks.iter().any(|(key, track)| {
                has_retained_media(track)
                    && state
                        .sessions
                        .get(&key.surface.session)
                        .is_some_and(|session| session.pane == pane)
            }) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timed) = self
                .delivery_changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if timed.timed_out() {
                return false;
            }
        }
    }
}

impl Drop for VirtualVivid {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.delivery_changed.notify_all();
    }
}

fn accept_loop<L: PresenterListener>(
    listener: L,
    state: Arc<Mutex<State>>,
    changed: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
) {
    while !shutdown.load(Ordering::Acquire) {
        match listener.accept() {
            Ok(stream) => {
                {
                    let mut state = lock(&state);
                    if state.connections >= MAX_CONNECTIONS {
                        continue;
                    }
                    state.connections += 1;
                }
                let state_clone = state.clone();
                let changed_clone = changed.clone();
                let _ = thread::Builder::new()
                    .name("vvmux-vivid-1.5-connection".into())
                    .spawn(move || {
                        if let Err(error) = handle_connection(stream, &state_clone, &changed_clone)
                        {
                            log::debug!("inner Vivid connection closed: {error}");
                        }
                        let mut state = lock(&state_clone);
                        state.connections = state.connections.saturating_sub(1);
                    });
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}

fn handle_connection(
    stream: Transport,
    state: &Arc<Mutex<State>>,
    changed: &Arc<Condvar>,
) -> io::Result<()> {
    stream
        .set_read_deadline(Duration::from_secs(3))
        .map_err(|error| with_context(error, "setting handshake deadline"))?;
    let (mut reader, preface, preface_bytes) =
        Reader::new(stream).map_err(|error| with_context(error, "reading Vivid preface"))?;
    match preface.kind {
        ConnectionKind::Control => handle_control(&mut reader, &preface_bytes, state, changed),
        ConnectionKind::Track => handle_track(&mut reader, state, changed),
        ConnectionKind::Lane => {
            let writer = reader.writer();
            let first = reader.read_record(ConnectionKind::Lane)?;
            let request = messages::decode_control(&first.body)
                .map(|envelope| envelope.request_id)
                .unwrap_or(0);
            writer.write_record(
                messages::ERROR,
                first.object_id,
                &protocol_error(
                    request,
                    messages::ERROR_UNSUPPORTED_PROFILE,
                    true,
                    "vvmux does not support Vivid lane connections",
                )?,
            )?;
            Ok(())
        }
        ConnectionKind::FileTransfer => {
            let writer = reader.writer();
            let first = reader.read_record(ConnectionKind::FileTransfer)?;
            writer.write_record(
                messages::ERROR,
                first.object_id,
                &protocol_error(
                    0,
                    messages::ERROR_UNSUPPORTED_PROFILE,
                    true,
                    "file-drop-v1 is not implemented by this presenter",
                )?,
            )?;
            Ok(())
        }
    }
}

fn handle_control(
    reader: &mut Reader,
    preface: &[u8; 16],
    shared: &Arc<Mutex<State>>,
    changed: &Arc<Condvar>,
) -> io::Result<()> {
    let writer = reader.writer();
    let first = reader
        .read_record(ConnectionKind::Control)
        .map_err(|error| with_context(error, "reading HELLO"))?;
    let (request_id, hello) = Hello::decode(&first.body)
        .map_err(|error| io::Error::other(format!("decoding HELLO: {error}")))?;
    let (session_id, maximum) =
        establish_session(shared, writer.clone(), preface, &hello, request_id)
            .map_err(|error| with_context(error, "establishing root session"))?;
    reader
        .set_maximum(maximum)
        .map_err(|error| with_context(error, "setting control receive maximum"))?;
    writer
        .set_maximum(maximum)
        .map_err(|error| with_context(error, "setting control send maximum"))?;
    reader
        .clear_read_deadline()
        .map_err(|error| with_context(error, "clearing handshake deadline"))?;
    let mut clean = false;
    let mut terminal_error = None;
    loop {
        let record = match reader.read_record(ConnectionKind::Control) {
            Ok(record) => record,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => {
                terminal_error = Some(error);
                break;
            }
        };
        match dispatch_control(shared, session_id, &record) {
            Ok(Some((record_type, object_id, body))) => {
                writer.write_record(record_type, object_id, &body)?;
            }
            Ok(None) => {}
            Err(error) => {
                let request = messages::decode_control(&record.body)
                    .map(|envelope| envelope.request_id)
                    .unwrap_or(0);
                let fatal = request == 0;
                writer.write_record(
                    messages::ERROR,
                    record.object_id,
                    &protocol_error(request, error.code, fatal, error.message)?,
                )?;
                if fatal {
                    break;
                }
            }
        }
        if record.record_type == messages::GOODBYE {
            clean = true;
            break;
        }
    }
    let expiry = {
        let mut state = lock(shared);
        let lease = state
            .sessions
            .get(&session_id)
            .and_then(|session| session.lease);
        let expiry = if let Some(lease_key) = lease {
            if !clean {
                suspend_session(&mut state, session_id, lease_key)
            } else {
                cleanup_session(&mut state, session_id);
                if let Some(mut lease) = state.leases.remove(&lease_key) {
                    let _ = lease.machine.confirm_transport_lost(true);
                }
                None
            }
        } else {
            if clean {
                detach_session(&mut state, session_id);
            } else {
                cleanup_session(&mut state, session_id);
            }
            None
        };
        advance_projection(&mut state);
        expiry
    };
    if let Some((lease_key, generation, deadline)) = expiry {
        spawn_lease_expiry(shared.clone(), lease_key, session_id, generation, deadline);
    }
    // A timed channel can be parked behind projection backpressure when its control session is
    // torn down. Wake it so it observes that its owner-scoped track disappeared and exits.
    changed.notify_all();
    terminal_error.map_or(Ok(()), Err)
}

fn establish_session(
    shared: &Arc<Mutex<State>>,
    writer: Arc<Writer>,
    preface: &[u8; 16],
    hello: &Hello,
    request_id: u64,
) -> io::Result<(u64, u32)> {
    enum Principal {
        Root {
            pane: PaneId,
            secret: Secret32,
        },
        Activation {
            key: (u64, u64),
            secret: Secret32,
            attempt_id: [u8; 16],
        },
        Resume {
            key: (u64, u64),
            secret: Secret32,
            attempt_id: [u8; 16],
            session_id: u64,
            generation: u64,
        },
    }

    let mut state = lock(shared);
    let root_contract =
        state.config.resource_contract.clone().unwrap_or_else(|| {
            presenter_contract(&state.config.media, state.config.target.as_ref())
        });
    let principal = match &hello.authentication {
        HelloAuthentication::Root { proof } => {
            let authless = hello.authless_payload()?;
            let matches = state
                .capabilities
                .iter()
                .filter_map(|(pane, secret)| {
                    auth::verify_root_hello_proof(secret, preface, &authless, proof)
                        .then_some(*pane)
                })
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return Err(send_fatal(
                    &writer,
                    request_id,
                    messages::ERROR_AUTH_FAILED,
                    "root authentication failed",
                ));
            }
            let pane = matches[0];
            let secret = state
                .capabilities
                .get(&pane)
                .map(|secret| Secret32::new(*secret.expose()))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::PermissionDenied, "principal was revoked")
                })?;
            Principal::Root { pane, secret }
        }
        HelloAuthentication::LeaseActivation {
            context_id,
            lease_id,
            activation_secret,
            attempt_id,
            ..
        } => {
            let key = (*context_id, *lease_id);
            let Some(lease) = state.leases.get(&key) else {
                return Err(send_fatal(
                    &writer,
                    request_id,
                    messages::ERROR_AUTH_FAILED,
                    "lease activation failed",
                ));
            };
            let expired = lease.issued_at.elapsed().as_micros()
                >= u128::from(lease.definition.activation_timeout_us);
            if expired
                || lease.active_session.is_some()
                || lease.machine.state() != LeaseState::Issued
                || !auth::verify_activation_secret(
                    *lease_id,
                    activation_secret,
                    &lease.definition.activation_verifier,
                )
            {
                return Err(send_fatal(
                    &writer,
                    request_id,
                    messages::ERROR_AUTH_FAILED,
                    "lease activation failed",
                ));
            }
            Principal::Activation {
                key,
                secret: Secret32::new(*activation_secret.expose()),
                attempt_id: *attempt_id,
            }
        }
        HelloAuthentication::Resume {
            context_id,
            lease_id,
            session_id,
            resume_generation,
            attempt_id,
            proof,
        } => {
            let key = (*context_id, *lease_id);
            let Some(lease) = state.leases.get(&key) else {
                return Err(send_fatal(
                    &writer,
                    request_id,
                    messages::ERROR_AUTH_FAILED,
                    "lease resume failed",
                ));
            };
            let valid_deadline = lease
                .grace_deadline
                .is_some_and(|deadline| deadline > Instant::now());
            let valid_identity = lease.active_session == Some(*session_id)
                && lease.machine.state() == LeaseState::Suspended
                && lease.machine.resume_generation().get() == *resume_generation;
            let Some(prior) = lease
                .resume_key
                .as_ref()
                .filter(|_| valid_deadline && valid_identity)
                .map(|key| Secret32::new(*key.expose()))
            else {
                return Err(send_fatal(
                    &writer,
                    request_id,
                    messages::ERROR_AUTH_FAILED,
                    "lease resume failed",
                ));
            };
            let expected = auth::resume_hello_proof(
                prior.expose(),
                preface,
                *lease_id,
                *session_id,
                *resume_generation,
                attempt_id,
                &hello.authless_payload()?,
            );
            if !auth::verify_proof(&expected, proof) {
                return Err(send_fatal(
                    &writer,
                    request_id,
                    messages::ERROR_AUTH_FAILED,
                    "lease resume failed",
                ));
            }
            Principal::Resume {
                key,
                secret: prior,
                attempt_id: *attempt_id,
                session_id: *session_id,
                generation: *resume_generation,
            }
        }
    };
    let (
        pane,
        lease_key,
        permitted_profiles,
        session_contract,
        authentication_kind,
        leased_context,
    ) = match &principal {
        Principal::Root { pane, .. } => (
            *pane,
            None,
            None,
            root_contract,
            messages::AUTHENTICATION_ROOT,
            None,
        ),
        Principal::Activation { key, .. } => {
            let lease = state.leases.get(key).expect("authenticated lease exists");
            (
                lease.pane,
                Some(*key),
                Some(lease.definition.permitted_profiles.clone()),
                lease.contract.clone(),
                messages::AUTHENTICATION_LEASE_ACTIVATION,
                Some(key.0),
            )
        }
        Principal::Resume { key, .. } => {
            let lease = state.leases.get(key).expect("authenticated lease exists");
            (
                lease.pane,
                Some(*key),
                Some(lease.definition.permitted_profiles.clone()),
                lease.contract.clone(),
                messages::AUTHENTICATION_RESUME,
                Some(key.0),
            )
        }
    };
    let target_profile = state.config.target.profile_name();
    if hello.target_profile != target_profile {
        return Err(send_fatal(
            &writer,
            request_id,
            messages::ERROR_UNSUPPORTED_PROFILE,
            "inner presenter target profile does not match the route",
        ));
    }
    let supported = &state.config.supported_profiles;
    if hello.required_profiles.iter().any(|profile| {
        !supported.contains(profile)
            || permitted_profiles
                .as_ref()
                .is_some_and(|permitted| !permitted.contains(profile))
    }) {
        return Err(send_fatal(
            &writer,
            request_id,
            messages::ERROR_UNSUPPORTED_PROFILE,
            "required Vivid profile is unsupported",
        ));
    }
    let Some(target) = state.targets.get(&pane).cloned() else {
        return Err(send_fatal(
            &writer,
            request_id,
            messages::ERROR_BAD_STATE,
            "presentation target is not ready",
        ));
    };
    if state
        .sessions
        .values()
        .filter(|session| !session.closed)
        .count()
        >= MAX_SESSIONS
    {
        return Err(send_fatal(
            &writer,
            request_id,
            messages::ERROR_LIMIT_EXCEEDED,
            "inner session capacity is exhausted",
        ));
    }
    let mut accepted = hello.required_profiles.clone();
    accepted.extend(
        hello
            .optional_profiles
            .iter()
            .filter(|profile| {
                supported.contains(profile)
                    && permitted_profiles
                        .as_ref()
                        .is_none_or(|permitted| permitted.contains(profile))
            })
            .cloned(),
    );
    accepted.sort();
    accepted.dedup();
    registry::validate_profile_set(accepted.iter().map(String::as_str))
        .map_err(io::Error::other)?;
    let session_id = match &principal {
        Principal::Resume { session_id, .. } => *session_id,
        _ => {
            state.next_session = state
                .next_session
                .checked_add(1)
                .ok_or_else(|| io::Error::other("inner session ID exhausted"))?;
            state.next_session
        }
    };
    let identity = SessionIdentity::new(state.presenter, session_id).map_err(io::Error::other)?;
    let root_context = match leased_context {
        Some(context) => context,
        None => identity.context(1).map_err(io::Error::other)?.context_id,
    };
    let mut server_nonce = [0_u8; auth::NONCE_BYTES];
    let mut session_tag = [0_u8; messages::SESSION_TAG_BYTES];
    getrandom::fill(&mut server_nonce).map_err(io::Error::other)?;
    getrandom::fill(&mut session_tag).map_err(io::Error::other)?;
    let secret = match &principal {
        Principal::Root { secret, .. }
        | Principal::Activation { secret, .. }
        | Principal::Resume { secret, .. } => secret,
    };
    let resume_generation = match &principal {
        Principal::Resume { generation, .. } => generation.saturating_add(1),
        _ => 0,
    };
    let prk = auth::extract_handshake_prk(secret, &hello.client_nonce, &server_nonce, &[0; 32]);
    let maximum = hello
        .maximum_control_body
        .min(vivid_protocol::CONTROL_MAX_RECORD_BODY);
    let mut welcome = Welcome {
        session_id,
        session_tag,
        root_context_id: root_context,
        target_generation: target.generation,
        target_profile: target_profile.into(),
        target_descriptor: target.descriptor,
        accepted_profiles: accepted,
        maximum_control_body: maximum,
        server_nonce,
        authentication: WelcomeAuthentication {
            kind: authentication_kind,
            confirmation: [0; 32],
            lease_state: if lease_key.is_some() {
                LeaseState::Active as u64
            } else {
                0
            },
            activation_attempt_status: 0,
        },
        session_revision: 1,
        scene_revision: 0,
        resource_contract: session_contract,
        establishment_state: u64::from(matches!(principal, Principal::Resume { .. })),
        resume_generation,
        extensions: vec![],
    };
    welcome.confirm(&prk)?;
    let candidate_welcome = welcome.encode(request_id)?;
    let fingerprint = profile_fingerprint(target_profile, &welcome.accepted_profiles);
    let (decided_nonce, decided_welcome) = match &principal {
        Principal::Root { .. } => (server_nonce, candidate_welcome),
        Principal::Activation {
            key, attempt_id, ..
        } => {
            let lease = state
                .leases
                .get_mut(key)
                .ok_or_else(|| io::Error::other("lease disappeared during activation"))?;
            let decision = lease
                .machine
                .begin_activation(
                    *attempt_id,
                    hello.client_nonce,
                    &hello.authless_payload()?,
                    fingerprint,
                    session_id,
                    server_nonce,
                    candidate_welcome,
                )
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::PermissionDenied, "lease activation failed")
                })?;
            lease.machine.commit_welcome().map_err(|error| {
                io::Error::other(format!("lease activation commit failed: {error:?}"))
            })?;
            match decision {
                AttemptDecision::Fresh {
                    server_nonce,
                    welcome,
                    ..
                }
                | AttemptDecision::ExactReplay {
                    server_nonce,
                    welcome,
                    ..
                } => (server_nonce, welcome),
            }
        }
        Principal::Resume {
            key,
            attempt_id,
            generation,
            ..
        } => {
            let lease = state
                .leases
                .get_mut(key)
                .ok_or_else(|| io::Error::other("lease disappeared during resume"))?;
            let decision = lease
                .machine
                .begin_resume(
                    ResumeGeneration::new(*generation),
                    *attempt_id,
                    hello.client_nonce,
                    &hello.authless_payload()?,
                    fingerprint,
                    session_id,
                    server_nonce,
                    candidate_welcome,
                )
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::PermissionDenied, "lease resume failed")
                })?;
            lease.machine.commit_welcome().map_err(|error| {
                io::Error::other(format!("lease resume commit failed: {error:?}"))
            })?;
            lease.resume_key = None;
            lease.grace_deadline = None;
            match decision {
                AttemptDecision::Fresh {
                    server_nonce,
                    welcome,
                    ..
                }
                | AttemptDecision::ExactReplay {
                    server_nonce,
                    welcome,
                    ..
                } => (server_nonce, welcome),
            }
        }
    };
    let (_, decided) = Welcome::decode(&decided_welcome).map_err(io::Error::other)?;
    let session_tag = decided.session_tag;
    let accepted_profiles = decided.accepted_profiles.iter().cloned().collect();
    let prk = auth::extract_handshake_prk(secret, &hello.client_nonce, &decided_nonce, &[0; 32]);
    let (keys, anchor_key) =
        auth::derive_session_keys(&prk, session_id, resume_generation, &session_tag);
    writer.write_record(messages::WELCOME, 0, &decided_welcome)?;
    let (scene_revision, target_generation) = state
        .sessions
        .get(&session_id)
        .map(|session| (session.scene_revision, session.target_generation))
        .unwrap_or((
            SceneRevision::ZERO,
            TargetGeneration::new(target.generation),
        ));
    state.sessions.insert(
        session_id,
        SessionRuntime {
            pane,
            closed: false,
            accepted_profiles,
            session_tag,
            channel_key: Secret32::new(*keys.channel_key()),
            anchor_key,
            writer,
            root_context,
            scene_revision,
            target_generation,
            anchors: HashMap::new(),
            seen_anchors: HashSet::new(),
            cancelled_waits: HashSet::new(),
            pending_waits: 0,
            lease: lease_key,
            resume_key: Secret32::new(*keys.resume_key()),
            resume_generation,
        },
    );
    if let Some(key) = lease_key
        && let Some(lease) = state.leases.get_mut(&key)
    {
        lease.active_session = Some(session_id);
        let _ = lease.machine.admit_post_hello();
        lease.revision = lease.revision.saturating_add(1);
    }
    Ok((session_id, maximum))
}

#[derive(Debug)]
struct ControlError {
    code: u64,
    message: &'static str,
}

impl ControlError {
    fn bad(message: &'static str) -> Self {
        Self {
            code: messages::ERROR_BAD_MESSAGE,
            message,
        }
    }
    fn state(message: &'static str) -> Self {
        Self {
            code: messages::ERROR_BAD_STATE,
            message,
        }
    }
    fn missing(message: &'static str) -> Self {
        Self {
            code: messages::ERROR_NOT_FOUND,
            message,
        }
    }
}

type ControlReply = Option<(u16, u64, Vec<u8>)>;

fn is_idempotent_mutation(record_type: u16) -> bool {
    matches!(
        record_type,
        messages::SET_OBSERVATION
            | messages::CREATE_SURFACE
            | messages::UPDATE_SURFACE
            | messages::DESTROY_SURFACE
            | messages::CREATE_TRACK
            | messages::DESTROY_TRACK
            | messages::ADVANCE_CHANNEL
            | messages::SET_AUDIO_GAIN
            | messages::ACTIVATE_TRACK
            | messages::BEGIN_TXN
            | messages::CREATE_NODE
            | messages::UPDATE_NODE
            | messages::DELETE_NODE
            | messages::ABORT_TXN
            | messages::COMMIT_TXN
            | messages::CANCEL_WAIT
            | messages::PLAY
            | messages::PAUSE
            | messages::FLUSH
            | messages::DRAIN
    )
}

fn mutation_fingerprint(record_type: u16, object_id: u64, envelope: &Envelope) -> [u8; 32] {
    let mut canonical = envelope.clone();
    canonical.request_id = 1;
    let mut digest = Sha256::new();
    digest.update(record_type.to_be_bytes());
    digest.update(object_id.to_be_bytes());
    if let Ok(encoded) = canonical.encode() {
        digest.update(encoded);
    }
    digest.finalize().into()
}

fn recorrelate_cached_reply(body: &[u8], request_id: u64) -> io::Result<Vec<u8>> {
    let mut envelope = messages::decode_control(body)?;
    envelope.request_id = request_id;
    envelope.encode().map_err(io::Error::other)
}

fn dispatch_control(
    shared: &Arc<Mutex<State>>,
    session_id: u64,
    record: &Record,
) -> Result<ControlReply, ControlError> {
    if record.flags & !RECORD_OPTIONAL != 0 {
        return Err(ControlError::bad("unknown control record flags"));
    }
    let envelope = messages::decode_control(&record.body)
        .map_err(|_| ControlError::bad("invalid strict control envelope"))?;
    envelope
        .validate_request()
        .map_err(|_| ControlError::bad("request ID must be nonzero"))?;
    let request_id = envelope.request_id;
    let value = Value::Map(envelope.payload.clone());
    let mut state = lock(shared);
    if !state.sessions.contains_key(&session_id) {
        return Err(ControlError::missing("session does not exist"));
    }
    let mutation_cache = if is_idempotent_mutation(record.record_type) {
        envelope.idempotency_key.map(|key| {
            (
                key,
                mutation_fingerprint(record.record_type, record.object_id, &envelope),
            )
        })
    } else {
        None
    };
    if let Some((key, fingerprint)) = mutation_cache
        && let Some(cached) = state.idempotency.get(&(session_id, key))
    {
        if cached.fingerprint != fingerprint {
            return Err(ControlError::bad(
                "idempotency key was reused with different mutation bytes",
            ));
        }
        let body = recorrelate_cached_reply(&cached.body, request_id)
            .map_err(|_| ControlError::bad("cached mutation reply is invalid"))?;
        return Ok(Some((cached.record_type, cached.object_id, body)));
    }
    let reply = match record.record_type {
        messages::PING => (
            messages::PONG,
            0,
            Envelope::new(request_id, envelope.payload).encode(),
        ),
        messages::GOODBYE => (messages::OK, 0, Ok(messages::ok(request_id))),
        messages::SET_OBSERVATION => (messages::OK, 0, Ok(messages::ok(request_id))),
        messages::CREATE_SURFACE => {
            let definition = SurfaceDefinition::decode_create(record.object_id, &value)
                .map_err(|_| ControlError::bad("invalid surface definition"))?;
            validate_surface_for_target(state.config.target.as_ref(), &definition)?;
            require_root_context(&state, session_id, definition.context_id)?;
            let key = SurfaceKey {
                session: session_id,
                context: definition.context_id,
                surface: definition.surface_id,
            };
            if state.surfaces.contains_key(&key) {
                return Err(ControlError::state("surface identity is already live"));
            }
            let surface =
                SurfaceState::new(definition).map_err(|_| ControlError::bad("invalid surface"))?;
            let payload = surface_ready_payload(key, &surface);
            state.surfaces.insert(
                key,
                SurfaceEntry {
                    state: surface,
                    active_slots: HashMap::new(),
                },
            );
            advance_projection(&mut state);
            (
                messages::SURFACE_READY,
                record.object_id,
                Envelope::new(request_id, payload).encode(),
            )
        }
        messages::UPDATE_SURFACE => {
            let map = StrictMap::new(
                "UPDATE_SURFACE",
                &value,
                &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            )
            .map_err(|_| ControlError::bad("invalid surface update"))?;
            let key = surface_key_from_map(session_id, &map)?;
            let current = state
                .surfaces
                .get(&key)
                .ok_or_else(|| ControlError::missing("surface does not exist"))?;
            let replacement = SurfaceDefinition {
                context_id: key.context,
                surface_id: key.surface,
                semantic_profile: current.state.definition.semantic_profile.clone(),
                coordinate_model: current.state.definition.coordinate_model,
                logical_width: required_u64(&map, 4)?,
                logical_height: required_u64(&map, 5)?,
                scale_numerator: required_u64(&map, 6)?,
                scale_denominator: required_u64(&map, 7)?,
                rotation: u16::try_from(required_u64(&map, 8)?)
                    .map_err(|_| ControlError::bad("invalid rotation"))?,
                descriptor: SurfaceDescriptor::from_value(
                    map.required(9)
                        .map_err(|_| ControlError::bad("missing descriptor"))?,
                )
                .map_err(|_| ControlError::bad("invalid descriptor"))?,
                policy: required_u64(&map, 10)?,
                profile_parameters: map
                    .required_map(11)
                    .map_err(|_| ControlError::bad("invalid profile parameters"))?
                    .to_vec(),
            };
            validate_surface_for_target(state.config.target.as_ref(), &replacement)?;
            let current = state
                .surfaces
                .get_mut(&key)
                .ok_or_else(|| ControlError::missing("surface does not exist"))?;
            current
                .state
                .replace_mutable(
                    SurfaceRevision::new(required_u64(&map, 2)?),
                    SurfaceGeneration::new(required_u64(&map, 3)?),
                    replacement,
                )
                .map_err(|_| ControlError::state("stale surface revision or generation"))?;
            advance_projection(&mut state);
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::DESTROY_SURFACE => {
            let map = StrictMap::new("surface identity", &value, &[0, 1])
                .map_err(|_| ControlError::bad("invalid surface identity"))?;
            let key = surface_key_from_map(session_id, &map)?;
            if state.surfaces.remove(&key).is_none() {
                return Err(ControlError::missing("surface does not exist"));
            }
            remove_surface_children(&mut state, key);
            advance_projection(&mut state);
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::QUERY_SURFACE => {
            let map = StrictMap::new("surface identity", &value, &[0, 1])
                .map_err(|_| ControlError::bad("invalid surface identity"))?;
            let key = surface_key_from_map(session_id, &map)?;
            let surface = state
                .surfaces
                .get(&key)
                .ok_or_else(|| ControlError::missing("surface does not exist"))?;
            (
                messages::SURFACE_STATUS,
                record.object_id,
                Envelope::new(request_id, surface_status_payload(key, surface)).encode(),
            )
        }
        messages::PROBE_TRACK_CONFIG => {
            let configuration = TrackConfiguration::decode(0, &value, true)
                .map_err(|_| ControlError::bad("invalid track probe"))?;
            let supported = supports_track(&configuration);
            (
                messages::TRACK_SUPPORT,
                0,
                Envelope::new(
                    request_id,
                    vec![
                        (0, Value::Bool(supported)),
                        (
                            1,
                            Value::Text(if supported {
                                "vvmux-relay".into()
                            } else {
                                "unsupported".into()
                            }),
                        ),
                        (2, Value::Unsigned(1)),
                        (
                            3,
                            Value::Map(configuration.payload(true).unwrap_or_default()),
                        ),
                    ],
                )
                .encode(),
            )
        }
        messages::CREATE_TRACK => {
            let configuration = TrackConfiguration::decode(record.object_id, &value, false)
                .map_err(|_| ControlError::bad("invalid track configuration"))?;
            let surface = SurfaceKey {
                session: session_id,
                context: configuration.context_id,
                surface: configuration.surface_id,
            };
            if !state.surfaces.contains_key(&surface) {
                return Err(ControlError::missing("owning surface does not exist"));
            }
            if !supports_track(&configuration) {
                return Err(ControlError {
                    code: messages::ERROR_UNSUPPORTED_CONFIG,
                    message: "track configuration is unsupported",
                });
            }
            let key = TrackKey {
                surface,
                track: configuration.track_id,
            };
            if state.tracks.contains_key(&key) {
                return Err(ControlError::state("track identity is already live"));
            }
            if state.tracks.len() >= state.config.media.max_sources {
                return Err(ControlError {
                    code: messages::ERROR_LIMIT_EXCEEDED,
                    message: "track capacity is exhausted",
                });
            }
            let track_state = TrackState::new();
            let payload = track_ready_payload(key, &configuration, &track_state);
            state.tracks.insert(
                key,
                TrackEntry {
                    configuration,
                    state: track_state,
                    audio_gain: AudioGain::UNITY,
                    channel_writer: None,
                    retained: None,
                    retained_raster: None,
                    playing: false,
                    play_request: PlayRequest::baseline(),
                    eos_epoch: None,
                    last_record_sequence: 0,
                    last_pts_us: 0,
                    outer_presented: false,
                    recovery_pending: true,
                    recovery_requested: false,
                    recovery_minimum_epoch: 0,
                    projection_blocked: false,
                    discard_blocked_for_recovery: false,
                    gate_linked_audio_for_recovery: false,
                    resume_after_pts_us: None,
                    causation_id: envelope.causation_id,
                },
            );
            advance_projection(&mut state);
            (
                messages::TRACK_READY,
                record.object_id,
                Envelope::new(request_id, payload).encode(),
            )
        }
        messages::DESTROY_TRACK => {
            let key = track_key_from_value(session_id, &value)?;
            remove_track(&mut state, key)?;
            advance_projection(&mut state);
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::QUERY_TRACK => {
            let key = track_key_from_value(session_id, &value)?;
            let gain_supported = state
                .sessions
                .get(&session_id)
                .is_some_and(|session| session.accepted_profiles.contains(registry::AUDIO_GAIN));
            let track = state
                .tracks
                .get(&key)
                .ok_or_else(|| ControlError::missing("track does not exist"))?;
            (
                messages::TRACK_STATUS,
                record.object_id,
                Envelope::new(request_id, track_status_payload(key, track, gain_supported))
                    .encode(),
            )
        }
        messages::ADVANCE_CHANNEL => {
            let map = StrictMap::new("ADVANCE_CHANNEL", &value, &[0, 1, 2, 3, 4, 5])
                .map_err(|_| ControlError::bad("invalid channel advance"))?;
            let key = track_key_from_map(session_id, &map)?;
            let track = state
                .tracks
                .get_mut(&key)
                .ok_or_else(|| ControlError::missing("track does not exist"))?;
            track
                .state
                .advance_channel(
                    ChannelGeneration::new(required_u64(&map, 3)?),
                    ChannelGeneration::new(required_u64(&map, 4)?),
                )
                .map_err(|_| ControlError::state("channel advance is stale"))?;
            track.channel_writer = None;
            track.recovery_pending = true;
            track.recovery_requested = false;
            track.discard_blocked_for_recovery = false;
            (
                messages::CHANNEL_ADVANCED,
                record.object_id,
                Envelope::new(
                    request_id,
                    vec![
                        (0, Value::Unsigned(key.surface.context)),
                        (1, Value::Unsigned(key.surface.surface)),
                        (2, Value::Unsigned(key.track)),
                        (3, Value::Unsigned(track.state.channel_generation.get())),
                        (4, Value::Unsigned(CHANNEL_OPEN_DEADLINE_US)),
                        (5, Value::Unsigned(track.state.revision.get())),
                    ],
                )
                .encode(),
            )
        }
        messages::SET_AUDIO_GAIN => {
            if !state
                .sessions
                .get(&session_id)
                .is_some_and(|session| session.accepted_profiles.contains(registry::AUDIO_GAIN))
            {
                return Err(ControlError {
                    code: messages::ERROR_UNSUPPORTED_PROFILE,
                    message: "audio-gain-v1 was not negotiated",
                });
            }
            let map = StrictMap::new("SET_AUDIO_GAIN", &value, &[0, 1, 2, 3])
                .map_err(|_| ControlError::bad("invalid audio gain"))?;
            let key = track_key_from_map(session_id, &map)?;
            let gain = AudioGain::new(required_u64(&map, 3)?)
                .ok_or_else(|| ControlError::bad("audio gain exceeds 200 percent"))?;
            let track = state
                .tracks
                .get_mut(&key)
                .ok_or_else(|| ControlError::missing("track does not exist"))?;
            if !matches!(track.configuration.kind, KindConfiguration::Audio(_)) {
                return Err(ControlError::state("audio gain requires an audio track"));
            }
            track.audio_gain = gain;
            track.state.revision = track
                .state
                .revision
                .advance()
                .map_err(|_| ControlError::state("track revision exhausted"))?;
            advance_projection(&mut state);
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::ACTIVATE_TRACK => {
            let map = StrictMap::new("ACTIVATE_TRACK", &value, &[0, 1, 2, 3])
                .map_err(|_| ControlError::bad("invalid activation"))?;
            let surface_key = surface_key_from_map(session_id, &map)?;
            let expected_revision = SurfaceRevision::new(required_u64(&map, 3)?);
            let bindings = map
                .required(2)
                .map_err(|_| ControlError::bad("missing bindings"))?
                .as_array()
                .ok_or_else(|| ControlError::bad("bindings are not an array"))?;
            let mut active = HashMap::new();
            for value in bindings {
                let binding = StrictMap::new("slot binding", value, &[0, 1, 2, 3])
                    .map_err(|_| ControlError::bad("invalid slot binding"))?;
                let slot = required_u64(&binding, 0)?;
                let track_id = required_u64(&binding, 1)?;
                if track_id == 0 {
                    continue;
                }
                let track = state
                    .tracks
                    .get(&TrackKey {
                        surface: surface_key,
                        track: track_id,
                    })
                    .ok_or_else(|| ControlError::missing("activation track is absent"))?;
                if track.configuration.slot != slot
                    || track.state.channel_generation.get() != required_u64(&binding, 2)?
                    || track.state.milestones & required_u64(&binding, 3)? == 0
                {
                    return Err(ControlError::state(
                        "activation generation or milestone is not ready",
                    ));
                }
                active.insert(slot, track_id);
            }
            let surface = state
                .surfaces
                .get_mut(&surface_key)
                .ok_or_else(|| ControlError::missing("surface does not exist"))?;
            if surface.state.revision != expected_revision {
                return Err(ControlError::state("surface activation revision is stale"));
            }
            surface.state.revision = surface
                .state
                .revision
                .advance()
                .map_err(|_| ControlError::state("surface revision exhausted"))?;
            surface.active_slots = active.clone();
            let surface_revision = surface.state.revision;
            let mut active_payload = active
                .iter()
                .map(|(slot, track)| (*slot, Value::Unsigned(*track)))
                .collect::<Vec<_>>();
            active_payload.sort_by_key(|(slot, _)| *slot);
            advance_projection(&mut state);
            (
                messages::TRACK_ACTIVATED,
                record.object_id,
                Envelope::new(
                    request_id,
                    vec![
                        (0, Value::Unsigned(surface_key.context)),
                        (1, Value::Unsigned(surface_key.surface)),
                        (2, Value::Map(active_payload)),
                        (3, Value::Unsigned(surface_revision.get())),
                        (4, Value::Unsigned(surface_revision.get())),
                    ],
                )
                .encode(),
            )
        }
        messages::BEGIN_TXN => {
            let map = StrictMap::new("BEGIN_TXN", &value, &[0, 1])
                .map_err(|_| ControlError::bad("invalid transaction"))?;
            let context = required_u64(&map, 0)?;
            require_root_context(&state, session_id, context)?;
            let transaction = required_u64(&map, 1)?;
            if state
                .transactions
                .insert((session_id, context, transaction), Vec::new())
                .is_some()
            {
                return Err(ControlError::state("transaction is already live"));
            }
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::CREATE_NODE | messages::UPDATE_NODE => {
            let transaction = envelope
                .transaction_id
                .ok_or_else(|| ControlError::bad("node mutation omits transaction"))?;
            let node = ProtocolSceneNode::decode(record.object_id, &value)
                .map_err(|_| ControlError::bad("invalid scene node"))?;
            let key = (session_id, node.owning_context_id, transaction);
            let pending = state
                .transactions
                .get_mut(&key)
                .ok_or_else(|| ControlError::missing("transaction does not exist"))?;
            pending.push(if record.record_type == messages::CREATE_NODE {
                NodeMutation::Create(node)
            } else {
                NodeMutation::Update(node)
            });
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::DELETE_NODE => {
            let transaction = envelope
                .transaction_id
                .ok_or_else(|| ControlError::bad("node deletion omits transaction"))?;
            let map = StrictMap::new("DELETE_NODE", &value, &[0, 1])
                .map_err(|_| ControlError::bad("invalid node deletion"))?;
            let context = required_u64(&map, 0)?;
            let node = required_u64(&map, 1)?;
            let pending = state
                .transactions
                .get_mut(&(session_id, context, transaction))
                .ok_or_else(|| ControlError::missing("transaction does not exist"))?;
            pending.push(NodeMutation::Delete(NodeKey {
                session: session_id,
                context,
                node,
            }));
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::ABORT_TXN => {
            let transaction = envelope.transaction_id.unwrap_or(record.object_id);
            state
                .transactions
                .retain(|(session, _, txn), _| *session != session_id || *txn != transaction);
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::COMMIT_TXN => {
            let transaction = envelope.transaction_id.unwrap_or(record.object_id);
            let session = state
                .sessions
                .get(&session_id)
                .ok_or_else(|| ControlError::missing("session does not exist"))?;
            // The two preconditions carry different registered codes and different producer
            // recoveries: a moved target is re-planned against the announcement that caused it,
            // while a failed revision precondition needs the scene re-read. Reporting both as one
            // makes a producer retry a commit that can never succeed.
            if envelope.expected_target_generation != Some(session.target_generation.get()) {
                return Err(ControlError {
                    code: messages::ERROR_STALE_TARGET_GENERATION,
                    message: "scene commit names a stale target generation",
                });
            }
            if envelope
                .preconditions
                .iter()
                .find_map(|(key, value)| (*key == 0).then(|| value.as_u64()).flatten())
                != Some(session.scene_revision.get())
            {
                return Err(ControlError {
                    code: messages::ERROR_PRECONDITION_FAILED,
                    message: "scene commit names a stale scene revision",
                });
            }
            let transaction_key = state
                .transactions
                .keys()
                .find(|(session, _, txn)| *session == session_id && *txn == transaction)
                .copied()
                .ok_or_else(|| ControlError::missing("transaction does not exist"))?;
            let pending = state
                .transactions
                .get(&transaction_key)
                .cloned()
                .unwrap_or_default();
            validate_node_mutations(&state, session_id, &pending)?;
            apply_node_mutations(&mut state, session_id, pending);
            state.transactions.remove(&transaction_key);
            let session = state.sessions.get_mut(&session_id).unwrap();
            session.scene_revision = session
                .scene_revision
                .advance()
                .map_err(|_| ControlError::state("scene revision exhausted"))?;
            let revision = session.scene_revision;
            let target = session.target_generation;
            advance_projection(&mut state);
            (
                messages::SCENE_PRESENTED,
                record.object_id,
                Envelope::new(
                    request_id,
                    vec![
                        (0, Value::Unsigned(revision.get())),
                        (1, Value::Unsigned(target.get())),
                    ],
                )
                .encode(),
            )
        }
        messages::QUERY_ANCHOR => {
            let map = StrictMap::new("QUERY_ANCHOR", &value, &[0, 1])
                .map_err(|_| ControlError::bad("invalid anchor query"))?;
            let context = required_u64(&map, 0)?;
            let anchor = required_u64(&map, 1)?;
            let session = state.sessions.get(&session_id).unwrap();
            let position = session.anchors.get(&(context, anchor));
            let mut payload = vec![
                (0, Value::Unsigned(context)),
                (1, Value::Unsigned(anchor)),
                (2, Value::Unsigned(if position.is_some() { 1 } else { 0 })),
            ];
            if let Some((row, column)) = position {
                payload.push((3, Value::Unsigned(*column as u64)));
                payload.push((4, nonnegative(*row)));
                payload.push((5, Value::Bool(true)));
            }
            payload.push((6, Value::Unsigned(session.target_generation.get())));
            (
                messages::ANCHOR_STATUS,
                record.object_id,
                Envelope::new(request_id, payload).encode(),
            )
        }
        messages::WAIT_TRACK => {
            let map = StrictMap::new("WAIT_TRACK", &value, &[0, 1, 2, 3, 4, 5, 6])
                .map_err(|_| ControlError::bad("invalid track wait"))?;
            let key = track_key_from_map(session_id, &map)?;
            let condition = required_u64(&map, 3)?;
            let condition_value = map
                .optional_u64(4)
                .map_err(|_| ControlError::bad("invalid wait value"))?;
            let timeout = required_u64(&map, 5)?;
            let generation = required_u64(&map, 6)?;
            if timeout == 0 || timeout > MAX_WAIT_US {
                return Err(ControlError::bad("invalid wait timeout"));
            }
            let track = state
                .tracks
                .get(&key)
                .ok_or_else(|| ControlError::missing("track does not exist"))?;
            if track.state.channel_generation.get() != generation {
                return Err(ControlError {
                    code: messages::ERROR_STALE_CHANNEL_GENERATION,
                    message: "track wait generation is stale",
                });
            }
            if let Some(observed) = evaluate_wait(track, condition, condition_value) {
                (
                    messages::WAIT_SATISFIED,
                    record.object_id,
                    Envelope::new(request_id, wait_payload(key, track, condition, observed))
                        .encode(),
                )
            } else {
                let session = state.sessions.get_mut(&session_id).unwrap();
                if session.pending_waits >= MAX_WAITS {
                    return Err(ControlError {
                        code: messages::ERROR_LIMIT_EXCEEDED,
                        message: "track wait capacity is exhausted",
                    });
                }
                session.pending_waits += 1;
                let writer = session.writer.clone();
                drop(state);
                spawn_wait(
                    shared.clone(),
                    writer,
                    session_id,
                    key,
                    request_id,
                    record.object_id,
                    condition,
                    condition_value,
                    generation,
                    timeout,
                );
                return Ok(None);
            }
        }
        messages::CANCEL_WAIT => {
            let map = StrictMap::new("CANCEL_WAIT", &value, &[0])
                .map_err(|_| ControlError::bad("invalid wait cancellation"))?;
            state
                .sessions
                .get_mut(&session_id)
                .unwrap()
                .cancelled_waits
                .insert(required_u64(&map, 0)?);
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        messages::PLAY | messages::PAUSE | messages::FLUSH | messages::DRAIN => {
            let key = track_key_from_value(session_id, &value)?;
            let mut linked_play = None;
            let mut linked_pause = false;
            let track = state
                .tracks
                .get_mut(&key)
                .ok_or_else(|| ControlError::missing("track does not exist"))?;
            match record.record_type {
                messages::PLAY => {
                    let map = StrictMap::new("PLAY", &value, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
                        .map_err(|_| ControlError::bad("invalid PLAY"))?;
                    let request = PlayRequest {
                        start_pts_us: map
                            .required(3)
                            .map_err(|_| ControlError::bad("missing start PTS"))?
                            .as_i64()
                            .ok_or_else(|| ControlError::bad("invalid start PTS"))?,
                        minimum_buffer_us: required_u64(&map, 4)?,
                        maximum_latency_us: required_u64(&map, 5)?,
                        rate_32_32: map
                            .required(6)
                            .map_err(|_| ControlError::bad("missing rate"))?
                            .as_i64()
                            .ok_or_else(|| ControlError::bad("invalid rate"))?,
                        late_policy: required_u64(&map, 7)?,
                        loop_count: required_u64(&map, 8)?,
                        start_policy: required_u64(&map, 9)?,
                    };
                    if request.minimum_buffer_us > request.maximum_latency_us
                        || request.rate_32_32 != 1_i64 << 32
                        || required_u64(&map, 10)? != track.state.channel_generation.get()
                    {
                        return Err(ControlError::state("PLAY policy or generation is invalid"));
                    }
                    track.playing = true;
                    track.play_request = request;
                    track.state.milestones |= MILESTONE_CLOCK_STARTED;
                    linked_play = Some(request);
                    #[cfg(any(test, feature = "testing"))]
                    state.play_commands.push(bridge_track_key(key));
                }
                messages::PAUSE => {
                    track.playing = false;
                    linked_pause = true;
                }
                messages::FLUSH => {
                    let map = StrictMap::new("FLUSH", &value, &[0, 1, 2, 3])
                        .map_err(|_| ControlError::bad("invalid FLUSH"))?;
                    let epoch = u32::try_from(required_u64(&map, 3)?)
                        .map_err(|_| ControlError::bad("invalid FLUSH epoch"))?;
                    if epoch <= track.state.media_epoch {
                        return Err(ControlError::state("FLUSH epoch did not advance"));
                    }
                    track.state.media_epoch = epoch;
                    track.state.last_media_id = 0;
                    track.recovery_pending = true;
                    track.recovery_requested = false;
                    track.recovery_minimum_epoch = epoch;
                    track.discard_blocked_for_recovery = false;
                    track.retained = None;
                    track.retained_raster = None;
                }
                messages::DRAIN => {
                    if track.eos_epoch.is_none() {
                        return Err(ControlError::state("DRAIN requires channel EOS"));
                    }
                }
                _ => unreachable!(),
            }
            if linked_play.is_some() || linked_pause {
                let active_tracks = state
                    .surfaces
                    .get(&key.surface)
                    .map(|surface| surface.active_slots.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                for track_id in active_tracks {
                    let member = TrackKey {
                        surface: key.surface,
                        track: track_id,
                    };
                    let Some(track) = state.tracks.get_mut(&member) else {
                        continue;
                    };
                    if let Some(request) = linked_play {
                        track.playing = true;
                        track.play_request = request;
                        track.state.milestones |= MILESTONE_CLOCK_STARTED;
                    } else {
                        track.playing = false;
                    }
                }
            }
            advance_projection(&mut state);
            (messages::OK, record.object_id, Ok(messages::ok(request_id)))
        }
        _ if record.flags & RECORD_OPTIONAL != 0 => return Ok(None),
        _ => {
            return Err(ControlError {
                code: messages::ERROR_UNSUPPORTED_PROFILE,
                message: "control record is not implemented by vvmux",
            });
        }
    };
    let body = reply
        .2
        .map_err(|_| ControlError::bad("reply encoding failed"))?;
    let response = (reply.0, reply.1, body);
    if let Some((key, fingerprint)) = mutation_cache {
        let cache_key = (session_id, key);
        if !state.idempotency.contains_key(&cache_key) {
            while state.idempotency.len() >= 256 {
                let Some(oldest) = state.idempotency_order.pop_front() else {
                    break;
                };
                state.idempotency.remove(&oldest);
            }
            state.idempotency_order.push_back(cache_key);
        }
        state.idempotency.insert(
            cache_key,
            CachedMutation {
                fingerprint,
                record_type: response.0,
                object_id: response.1,
                body: response.2.clone(),
            },
        );
    }
    Ok(Some(response))
}

fn handle_track(
    reader: &mut Reader,
    shared: &Arc<Mutex<State>>,
    changed: &Arc<Condvar>,
) -> io::Result<()> {
    let writer = reader.writer();
    let first = reader.read_record(ConnectionKind::Track)?;
    let envelope = messages::decode_control(&first.body)?;
    let request_id = envelope.request_id;
    let open = ChannelOpen::decode(first.object_id, &first.body)?;
    let key = TrackKey {
        surface: SurfaceKey {
            session: open.session_id,
            context: open.context_id,
            surface: open.surface_id,
        },
        track: open.track_id,
    };
    let generation = ChannelGeneration::new(open.channel_generation);
    {
        let mut state = lock(shared);
        let session = state
            .sessions
            .get(&open.session_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "session does not exist"))?;
        if session.closed {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "session is no longer live",
            ));
        }
        let expected = auth::channel_tag(
            session.channel_key.expose(),
            open.session_id,
            open.context_id,
            open.surface_id,
            open.track_id,
            open.channel_generation,
            open.track_kind as u32,
            open.lane as u32,
            &open.client_nonce,
        );
        if !auth::verify_tag(&expected, &open.authentication_tag) {
            writer.write_record(
                messages::ERROR,
                open.track_id,
                &protocol_error(
                    request_id,
                    messages::ERROR_AUTH_FAILED,
                    true,
                    "channel authentication failed",
                )?,
            )?;
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "channel authentication failed",
            ));
        }
        let track = state
            .tracks
            .get_mut(&key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "track does not exist"))?;
        if track.state.channel_generation.get() != open.channel_generation
            || track.configuration.kind.kind() != open.track_kind
            || track.configuration.lane != open.lane
        {
            writer.write_record(
                messages::ERROR,
                open.track_id,
                &protocol_error(
                    request_id,
                    messages::ERROR_STALE_CHANNEL_GENERATION,
                    true,
                    "CHANNEL_OPEN does not match the live track generation",
                )?,
            )?;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stale channel generation",
            ));
        }
        if track.channel_writer.is_some() {
            writer.write_record(
                messages::ERROR,
                open.track_id,
                &protocol_error(
                    request_id,
                    messages::ERROR_CHANNEL_BUSY,
                    true,
                    "track generation already has a live channel",
                )?,
            )?;
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "track channel is busy",
            ));
        }
        let maximum_bytes = u64::from(track.configuration.maximum_record_body);
        track
            .state
            .accept_channel(
                generation,
                maximum_bytes,
                INITIAL_FLOW_RECORDS,
                track.configuration.maximum_record_body,
            )
            .map_err(io::Error::other)?;
        track.channel_writer = Some(writer.clone());
        writer.write_record(
            messages::CHANNEL_ACCEPTED,
            open.track_id,
            &Envelope::new(
                request_id,
                vec![
                    (0, Value::Unsigned(open.context_id)),
                    (1, Value::Unsigned(open.surface_id)),
                    (2, Value::Unsigned(open.track_id)),
                    (3, Value::Unsigned(open.channel_generation)),
                    (4, Value::Unsigned(maximum_bytes)),
                    (5, Value::Unsigned(INITIAL_FLOW_RECORDS)),
                    (
                        6,
                        Value::Unsigned(u64::from(track.configuration.maximum_record_body)),
                    ),
                    (7, Value::Unsigned(track.state.revision.get())),
                ],
            )
            .encode()?,
        )?;
        reader.set_maximum(track.configuration.maximum_record_body)?;
    }
    reader.clear_read_deadline()?;
    let result = track_loop(reader, shared, changed, key, generation);
    let mut state = lock(shared);
    let mut changed_payload = None;
    let gain_supported = state
        .sessions
        .get(&key.surface.session)
        .is_some_and(|session| session.accepted_profiles.contains(registry::AUDIO_GAIN));
    if let Some(track) = state
        .tracks
        .get_mut(&key)
        .filter(|track| track.state.channel_generation == generation)
    {
        // A retired channel can observe EOF after ADVANCE_CHANNEL has already accepted its
        // replacement. Only the generation that still owns the track attachment may detach or
        // mark it lost; otherwise the old cleanup stops the newly opened video generation.
        track.channel_writer = None;
        let _ = track.state.detach();
        if result.is_err() || track.eos_epoch.is_none() {
            let _ = track.state.lose();
            track.recovery_pending = true;
            track.recovery_requested = false;
            track.discard_blocked_for_recovery = false;
        }
        changed_payload = Envelope::new(0, track_status_payload(key, track, gain_supported))
            .encode()
            .ok();
    }
    let control = state
        .sessions
        .get(&key.surface.session)
        .map(|session| session.writer.clone());
    if changed_payload.is_some() {
        advance_projection(&mut state);
    }
    drop(state);
    if let Some(payload) = changed_payload
        && let Some(control) = control
    {
        let _ = control.write_record(messages::TRACK_CHANGED, key.track, &payload);
    }
    if result.is_err()
        && let Ok(body) = protocol_error(
            0,
            messages::ERROR_BAD_MESSAGE,
            true,
            "track channel failed validation",
        )
    {
        let _ = writer.write_record(messages::ERROR, key.track, &body);
    }
    result
}

fn track_loop(
    reader: &mut Reader,
    shared: &Arc<Mutex<State>>,
    changed: &Arc<Condvar>,
    key: TrackKey,
    generation: ChannelGeneration,
) -> io::Result<()> {
    loop {
        let record = match reader.read_record(ConnectionKind::Track) {
            Ok(record) => record,
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        if record.object_id != key.track {
            return Err(invalid("track record object ID is not the accepted track"));
        }
        if record.record_type == messages::CHANNEL_EOS {
            let envelope = messages::decode_control(&record.body)?;
            if envelope.request_id != 0 {
                return Err(invalid("CHANNEL_EOS must be uncorrelated"));
            }
            let value = Value::Map(envelope.payload);
            let eos = StrictMap::new("CHANNEL_EOS", &value, &[0, 1, 2, 3, 4, 5])
                .map_err(io::Error::other)?;
            let mut state = lock(shared);
            let track = state
                .tracks
                .get_mut(&key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "track disappeared"))?;
            if track.state.channel_generation != generation {
                return Ok(());
            }
            if eos.required_u64(0).ok() != Some(key.surface.context)
                || eos.required_u64(1).ok() != Some(key.surface.surface)
                || eos.required_u64(2).ok() != Some(key.track)
                || eos.required_u64(3).ok() != Some(generation.get())
                || eos.required_u64(5).ok() != Some(record.sequence.saturating_sub(1))
            {
                return Err(invalid("CHANNEL_EOS identity or sequence is invalid"));
            }
            let epoch = eos
                .required_u64(4)
                .ok()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| invalid("CHANNEL_EOS epoch is invalid"))?;
            if epoch < track.state.media_epoch {
                return Err(invalid("CHANNEL_EOS epoch is stale"));
            }
            track.eos_epoch = Some(epoch);
            track.state.milestones |= MILESTONE_EOS_ACCEPTED;
            track.last_record_sequence = record.sequence;
            advance_projection(&mut state);
            continue;
        }

        let mut state = lock(shared);
        let events = state.events.clone();
        let wakeup = state.media_wakeup.clone();
        let Some(track) = state.tracks.get(&key) else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "track disappeared"));
        };
        if track.state.channel_generation != generation {
            return Ok(());
        }
        let configuration = track.configuration.clone();
        let (epoch, media_id, random_access, pts, retained) =
            validate_media_record(&configuration, &record)?;
        let timed = matches!(
            configuration.kind,
            KindConfiguration::Video(_) | KindConfiguration::Audio(_)
        );
        let source = bridge_track_key(key);
        let linked_video = matches!(configuration.kind, KindConfiguration::Audio(_))
            .then(|| {
                state.tracks.iter().find_map(|(candidate, track)| {
                    (candidate.surface == key.surface
                        && matches!(track.configuration.kind, KindConfiguration::Video(_)))
                    .then_some(*candidate)
                })
            })
            .flatten();
        let prime_live_audio_before_projection = configuration.mode == TrackMode::Live
            && matches!(configuration.kind, KindConfiguration::Audio(_))
            && state
                .tracks
                .get(&key)
                .is_some_and(|track| track.state.milestones & MILESTONE_OUTPUT_READY == 0);
        if prime_live_audio_before_projection {
            // A live PCM producer must send packet 1 before it can wait for OUTPUT_READY and
            // activate the audio slot. The outer snapshot acknowledgement can be delayed behind
            // an already-live raster stream, so gating admission on that acknowledgement creates
            // a cycle: no admitted packet, no readiness, and therefore no activation. Admit only
            // the first packet here, but keep its delivery parked below. Its consumed one-record
            // allowance also keeps every later packet bounded until the projection is applied.
            let track = state.tracks.get_mut(&key).unwrap();
            track
                .state
                .admit_media(
                    generation,
                    u32::try_from(record.body.len())
                        .map_err(|_| invalid("media body exceeds u32"))?,
                    epoch,
                    media_id,
                    random_access,
                )
                .map_err(io::Error::other)?;
            track.state.milestones |= MILESTONE_DECODER_INITIALIZED | MILESTONE_OUTPUT_READY;
            track.last_record_sequence = record.sequence;
            track.last_pts_us = pts;
            changed.notify_all();
        }
        loop {
            let projected = state.projected_sources.contains(&source);
            let linked_video_recovering = linked_video.is_some_and(|video| {
                state.tracks.get(&video).is_some_and(|track| {
                    track.recovery_pending && track.gate_linked_audio_for_recovery
                })
            });
            if !timed || (projected && !linked_video_recovering) {
                break;
            }
            // Hidden and detached timed media must exert backpressure. Rejecting a packet and
            // immediately returning its allowance lets independent audio/video producers race
            // through the file at disk speed; audio can reach EOS while video is still recovering.
            // Linked audio also stays parked until the replacement video keyframe is accepted, so
            // stale samples cannot start the new physical audio clock.
            if !projected && let Some(track) = state.tracks.get_mut(&key) {
                track.projection_blocked = true;
            }
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match state.tracks.get(&key) {
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "track disappeared while waiting for projection",
                    ));
                }
                Some(track) if track.state.channel_generation != generation => return Ok(()),
                Some(_) => {}
            }
        }
        let (
            recovery_pending,
            recovery_minimum_epoch,
            discard_blocked_for_recovery,
            recovery_gates_audio,
        ) = {
            let track = state.tracks.get_mut(&key).unwrap();
            track.projection_blocked = false;
            (
                track.recovery_pending,
                track.recovery_minimum_epoch,
                std::mem::take(&mut track.discard_blocked_for_recovery),
                track.gate_linked_audio_for_recovery,
            )
        };
        let is_video = matches!(configuration.kind, KindConfiguration::Video(_));
        let recovery_keyframe_inflight = is_video
            && recovery_pending
            && state
                .deliveries
                .values()
                .any(|delivery| delivery.track == key && delivery.random_access);
        let recovering_keyframe = is_video
            && recovery_pending
            && !recovery_keyframe_inflight
            && random_access
            && epoch >= recovery_minimum_epoch
            && !discard_blocked_for_recovery;
        let discard_for_recovery = is_video
            && recovery_pending
            && !recovery_keyframe_inflight
            && (!random_access || epoch < recovery_minimum_epoch || discard_blocked_for_recovery);
        let discard_for_audio_catchup = if matches!(configuration.kind, KindConfiguration::Audio(_))
        {
            let track = state.tracks.get_mut(&key).unwrap();
            match track.resume_after_pts_us {
                Some(floor) if pts == i64::MIN || pts < floor => true,
                Some(_) => {
                    track.resume_after_pts_us = None;
                    false
                }
                None => false,
            }
        } else {
            false
        };
        let discard_locally = discard_for_recovery || discard_for_audio_catchup;
        {
            let track = state.tracks.get_mut(&key).unwrap();
            if !prime_live_audio_before_projection {
                track
                    .state
                    .admit_media(
                        generation,
                        u32::try_from(record.body.len())
                            .map_err(|_| invalid("media body exceeds u32"))?,
                        epoch,
                        media_id,
                        random_access,
                    )
                    .map_err(io::Error::other)?;
                if !discard_locally {
                    track.state.milestones |=
                        MILESTONE_DECODER_INITIALIZED | MILESTONE_OUTPUT_READY;
                }
                track.last_record_sequence = record.sequence;
                track.last_pts_us = pts;
            }
            if random_access && events.is_none() {
                // Focused eventless tests terminate delivery locally. A live bridge keeps
                // recovery pending until complete_bridge_delivery confirms outer acceptance.
                track.recovery_pending = false;
                track.recovery_requested = false;
                track.discard_blocked_for_recovery = false;
            }
            update_retained_media(key, track, &record)?;
        }
        if recovering_keyframe && recovery_gates_audio {
            let linked_audio = state
                .tracks
                .iter()
                .filter_map(|(candidate, track)| {
                    (candidate.surface == key.surface
                        && matches!(track.configuration.kind, KindConfiguration::Audio(_)))
                    .then_some(*candidate)
                })
                .collect::<Vec<_>>();
            for audio in linked_audio {
                if let Some(track) = state.tracks.get_mut(&audio) {
                    track.resume_after_pts_us = Some(pts);
                }
            }
        }
        if discard_locally {
            // Recovery video and pre-recovery linked audio cannot enter the replacement outer
            // clock. Consume and credit them locally; audio catch-up intentionally runs without
            // media-time pacing so it reaches the accepted video PTS promptly.
            let track = state.tracks.get_mut(&key).unwrap();
            track.state.flow.raise_maxima(
                track
                    .state
                    .flow
                    .maximum_body_bytes
                    .saturating_add(record.body.len() as u64),
                track.state.flow.maximum_media_records.saturating_add(1),
            );
            send_flow_update(key, track);
            drop(state);
            changed.notify_all();
            continue;
        }
        if events.is_none() {
            // The eventless constructor is used by focused presenter/bridge tests. It terminates
            // the media locally, so successful validation is immediately reusable flow.
            let track = state.tracks.get_mut(&key).unwrap();
            track.outer_presented = true;
            track.state.milestones |= MILESTONE_PRESENTED;
            track.state.flow.raise_maxima(
                track
                    .state
                    .flow
                    .maximum_body_bytes
                    .saturating_add(record.body.len() as u64),
                track.state.flow.maximum_media_records.saturating_add(1),
            );
            send_flow_update(key, track);
            advance_projection(&mut state);
            drop(state);
            changed.notify_all();
            continue;
        }
        state.next_delivery = state
            .next_delivery
            .checked_add(1)
            .ok_or_else(|| io::Error::other("delivery ID exhausted"))?;
        let delivery_id = state.next_delivery;
        state.deliveries.insert(
            delivery_id,
            PendingDelivery {
                track: key,
                bytes: record.body.len() as u64,
                random_access,
            },
        );
        let recovered_keyframe = recovering_keyframe.then_some((epoch, pts));
        let event = MediaEvent {
            delivery_id,
            source,
            record_type: record.record_type,
            recovered_keyframe,
            body: record.body,
        };
        let queued = events
            .as_ref()
            .is_some_and(|sender| sender.try_send(event).is_ok());
        if !queued {
            state.deliveries.remove(&delivery_id);
            let track = state.tracks.get_mut(&key).unwrap();
            track.recovery_pending = true;
            track.recovery_requested = false;
            track.discard_blocked_for_recovery = false;
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "bounded bridge media queue is full",
            ));
        }
        // Timed packets change delivery state, not the outer scene projection. Advancing the
        // projection for every audio/video packet makes the attached bridge reconcile a no-op
        // snapshot before nearly every media record; linked audio then accumulates behind video
        // and catches up only after video EOS. Retained image/raster bodies do belong to the
        // authoritative projection and still advance it for rehydration.
        if retained {
            advance_projection(&mut state);
        }
        drop(state);
        changed.notify_all();
        if let Some(wakeup) = wakeup {
            wakeup();
        }
    }
}

fn has_retained_media(track: &TrackEntry) -> bool {
    track.retained.is_some() || track.retained_raster.is_some()
}

/// Terminate an inner raster delta chain into one owner-scoped latest framebuffer.
///
/// A nested presenter cannot retain only the last full record: interactive producers such as
/// vvpaint send an initial full canvas followed by small overwrite deltas. Replaying that old full
/// record after an outer attachment is recreated restores a blank canvas. Compose every accepted
/// delta here so a later outer hop starts from the actual latest pixels and gets a new hop-local
/// full-frame identity.
fn update_retained_media(key: TrackKey, track: &mut TrackEntry, record: &Record) -> io::Result<()> {
    match (&track.configuration.kind, record.record_type) {
        (KindConfiguration::EncodedImage(_), messages::IMAGE_DATA) => {
            track.retained = Some(Arc::from(record.body.clone()));
            track.retained_raster = None;
        }
        (KindConfiguration::Raster(config), messages::RASTER_FRAME) => {
            let flags = record
                .body
                .get(4..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_be_bytes)
                .ok_or_else(|| invalid("raster record is truncated"))?;
            track.retained = None;
            if flags & media::RASTER_FRAME_DELTA == 0 {
                let frame = media::parse_full_raster_frame(&record.body)?;
                let pixels = media::decode_raster_pixels(frame)?;
                track.retained_raster = Some(RetainedRaster {
                    epoch: frame.epoch,
                    frame_id: frame.frame_id,
                    width: frame.width,
                    height: frame.height,
                    pixels: Arc::from(pixels),
                });
            } else {
                let frame = media::parse_delta_raster_frame(
                    &record.body,
                    config.width,
                    config.height,
                    u32::from(config.maximum_delta_operations),
                )?;
                let Some(retained) = track.retained_raster.as_mut() else {
                    track.recovery_pending = true;
                    send_need_full_frame(key, track);
                    return Err(invalid("raster delta has no retained full-frame base"));
                };
                if retained.epoch != frame.epoch || retained.frame_id != frame.base_frame_id {
                    track.recovery_pending = true;
                    send_need_full_frame(key, track);
                    return Err(invalid(
                        "raster delta does not name the retained base frame",
                    ));
                }
                let pixels = Arc::make_mut(&mut retained.pixels);
                for operation in frame.operations {
                    apply_retained_raster_operation(
                        pixels,
                        retained.width,
                        retained.height,
                        operation,
                    )?;
                }
                retained.epoch = frame.epoch;
                retained.frame_id = frame.frame_id;
            }
        }
        _ => {}
    }
    Ok(())
}

fn apply_retained_raster_operation(
    pixels: &mut [u8],
    canvas_width: u32,
    canvas_height: u32,
    operation: media::ParsedRasterDeltaOperation<'_>,
) -> io::Result<()> {
    let stride = usize::try_from(canvas_width)
        .ok()
        .and_then(|width| width.checked_mul(4))
        .ok_or_else(|| invalid("raster stride overflows"))?;
    let expected = media::rgba8_pixel_len(canvas_width, canvas_height)
        .map_err(|_| invalid("raster dimensions overflow"))? as usize;
    if pixels.len() != expected {
        return Err(invalid("retained raster pixel length is inconsistent"));
    }
    match operation {
        media::ParsedRasterDeltaOperation::Overwrite {
            x,
            y,
            width,
            height,
            rgba,
        } => {
            let row_bytes = usize::try_from(width)
                .ok()
                .and_then(|width| width.checked_mul(4))
                .ok_or_else(|| invalid("raster overwrite row overflows"))?;
            let x_bytes = usize::try_from(x)
                .ok()
                .and_then(|x| x.checked_mul(4))
                .ok_or_else(|| invalid("raster overwrite offset overflows"))?;
            let height = usize::try_from(height)
                .map_err(|_| invalid("raster overwrite height exceeds address space"))?;
            for row in 0..height {
                let destination = usize::try_from(y)
                    .ok()
                    .and_then(|y| y.checked_add(row))
                    .and_then(|y| y.checked_mul(stride))
                    .and_then(|offset| offset.checked_add(x_bytes))
                    .ok_or_else(|| invalid("raster overwrite destination overflows"))?;
                let source = row
                    .checked_mul(row_bytes)
                    .ok_or_else(|| invalid("raster overwrite source overflows"))?;
                let destination_end = destination
                    .checked_add(row_bytes)
                    .ok_or_else(|| invalid("raster overwrite destination extent overflows"))?;
                let source_end = source
                    .checked_add(row_bytes)
                    .ok_or_else(|| invalid("raster overwrite source extent overflows"))?;
                if destination_end > pixels.len() || source_end > rgba.len() {
                    return Err(invalid("raster overwrite extent exceeds its buffers"));
                }
                pixels[destination..destination_end].copy_from_slice(&rgba[source..source_end]);
            }
        }
        media::ParsedRasterDeltaOperation::Copy {
            destination_x,
            destination_y,
            width,
            height,
            source_x,
            source_y,
        } => {
            let row_bytes = usize::try_from(width)
                .ok()
                .and_then(|width| width.checked_mul(4))
                .ok_or_else(|| invalid("raster copy row overflows"))?;
            let source_x = usize::try_from(source_x)
                .ok()
                .and_then(|x| x.checked_mul(4))
                .ok_or_else(|| invalid("raster copy source offset overflows"))?;
            let destination_x = usize::try_from(destination_x)
                .ok()
                .and_then(|x| x.checked_mul(4))
                .ok_or_else(|| invalid("raster copy destination offset overflows"))?;
            let height = usize::try_from(height)
                .map_err(|_| invalid("raster copy height exceeds address space"))?;
            let mut copy_row = |row: usize| -> io::Result<()> {
                let source = usize::try_from(source_y)
                    .ok()
                    .and_then(|y| y.checked_add(row))
                    .and_then(|y| y.checked_mul(stride))
                    .and_then(|offset| offset.checked_add(source_x))
                    .ok_or_else(|| invalid("raster copy source overflows"))?;
                let destination = usize::try_from(destination_y)
                    .ok()
                    .and_then(|y| y.checked_add(row))
                    .and_then(|y| y.checked_mul(stride))
                    .and_then(|offset| offset.checked_add(destination_x))
                    .ok_or_else(|| invalid("raster copy destination overflows"))?;
                let source_end = source
                    .checked_add(row_bytes)
                    .ok_or_else(|| invalid("raster copy source extent overflows"))?;
                let destination_end = destination
                    .checked_add(row_bytes)
                    .ok_or_else(|| invalid("raster copy destination extent overflows"))?;
                if source_end > pixels.len() || destination_end > pixels.len() {
                    return Err(invalid("raster copy extent exceeds the retained canvas"));
                }
                pixels.copy_within(source..source_end, destination);
                Ok(())
            };
            if destination_y > source_y {
                for row in (0..height).rev() {
                    copy_row(row)?;
                }
            } else {
                for row in 0..height {
                    copy_row(row)?;
                }
            }
        }
    }
    Ok(())
}

fn validate_media_record(
    configuration: &TrackConfiguration,
    record: &Record,
) -> io::Result<(u32, u64, bool, i64, bool)> {
    match (&configuration.kind, record.record_type) {
        (KindConfiguration::Video(config), messages::VIDEO_PACKET) => {
            let packet = media::parse_video_packet(&record.body)?;
            if packet.data.len() > config.maximum_access_unit_bytes as usize {
                return Err(invalid("video packet exceeds immutable configuration"));
            }
            Ok((
                packet.epoch,
                packet.packet_id,
                packet.flags & media::VIDEO_PACKET_KEY != 0,
                packet.pts_us,
                false,
            ))
        }
        (KindConfiguration::Audio(config), messages::AUDIO_PACKET) => {
            let packet = media::parse_audio_packet(&record.body)?;
            if packet.data.len() > config.maximum_access_unit_bytes as usize {
                return Err(invalid("audio packet exceeds immutable configuration"));
            }
            Ok((packet.epoch, packet.packet_id, true, packet.pts_us, false))
        }
        (KindConfiguration::Raster(config), messages::RASTER_FRAME) => {
            let flags = record
                .body
                .get(4..8)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_be_bytes)
                .ok_or_else(|| invalid("raster record is truncated"))?;
            if flags & media::RASTER_FRAME_DELTA == 0 {
                let frame = media::parse_full_raster_frame(&record.body)?;
                if (frame.width, frame.height) != (config.width, config.height) {
                    return Err(invalid("raster dimensions differ from track configuration"));
                }
                let _ = media::decode_raster_pixels(frame)?;
                Ok((frame.epoch, frame.frame_id, true, frame.pts_us, true))
            } else {
                if !config.delta_enabled {
                    return Err(invalid("raster delta was not negotiated"));
                }
                let frame = media::parse_delta_raster_frame(
                    &record.body,
                    config.width,
                    config.height,
                    u32::from(config.maximum_delta_operations),
                )?;
                Ok((frame.epoch, frame.frame_id, false, frame.pts_us, false))
            }
        }
        (KindConfiguration::EncodedImage(config), messages::IMAGE_DATA) => {
            if record.body.len() != config.encoded_length as usize
                || config.sha256.is_some_and(|expected| {
                    let actual: [u8; 32] = Sha256::digest(&record.body).into();
                    actual != expected
                })
            {
                return Err(invalid(
                    "image length or hash differs from track configuration",
                ));
            }
            Ok((0, 1, true, 0, true))
        }
        _ => Err(invalid("media record type does not match its track kind")),
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_wait(
    shared: Arc<Mutex<State>>,
    writer: Arc<Writer>,
    session_id: u64,
    key: TrackKey,
    request_id: u64,
    object_id: u64,
    condition: u64,
    condition_value: Option<u64>,
    generation: u64,
    timeout_us: u64,
) {
    thread::spawn(move || {
        enum WaitOutcome {
            Satisfied(Vec<u8>),
            Failed(u64, &'static str),
        }
        let deadline = Instant::now() + Duration::from_micros(timeout_us);
        loop {
            let outcome = {
                let mut state = lock(&shared);
                let cancelled = state
                    .sessions
                    .get_mut(&session_id)
                    .is_some_and(|session| session.cancelled_waits.remove(&request_id));
                if cancelled {
                    Some(WaitOutcome::Failed(
                        messages::ERROR_CANCELLED,
                        "track wait was cancelled",
                    ))
                } else {
                    match state.tracks.get(&key) {
                        None => Some(WaitOutcome::Failed(
                            messages::ERROR_NOT_FOUND,
                            "track was destroyed while waiting",
                        )),
                        Some(track) if track.state.channel_generation.get() != generation => {
                            Some(WaitOutcome::Failed(
                                messages::ERROR_STALE_CHANNEL_GENERATION,
                                "channel generation changed while waiting",
                            ))
                        }
                        Some(track) => {
                            evaluate_wait(track, condition, condition_value).map(|observed| {
                                match Envelope::new(
                                    request_id,
                                    wait_payload(key, track, condition, observed),
                                )
                                .encode()
                                {
                                    Ok(body) => WaitOutcome::Satisfied(body),
                                    Err(_) => WaitOutcome::Failed(
                                        messages::ERROR_BAD_MESSAGE,
                                        "wait reply encoding failed",
                                    ),
                                }
                            })
                        }
                    }
                }
            };
            if let Some(outcome) = outcome {
                match outcome {
                    WaitOutcome::Satisfied(body) => {
                        let _ = writer.write_record(messages::WAIT_SATISFIED, object_id, &body);
                    }
                    WaitOutcome::Failed(code, diagnostic) => {
                        if let Ok(body) = protocol_error(request_id, code, false, diagnostic) {
                            let _ = writer.write_record(messages::ERROR, object_id, &body);
                        }
                    }
                }
                break;
            }
            if Instant::now() >= deadline {
                if let Ok(body) = protocol_error(
                    request_id,
                    messages::ERROR_TIMEOUT,
                    false,
                    "track wait timed out",
                ) {
                    let _ = writer.write_record(messages::ERROR, object_id, &body);
                }
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }
        if let Some(session) = lock(&shared).sessions.get_mut(&session_id) {
            session.pending_waits = session.pending_waits.saturating_sub(1);
        }
    });
}

fn evaluate_wait(track: &TrackEntry, condition: u64, value: Option<u64>) -> Option<u64> {
    match condition {
        1 => (track.state.revision.get() > value?).then_some(track.state.revision.get()),
        2 => {
            let mask = value?;
            (mask != 0 && track.state.milestones & mask == mask).then_some(track.state.milestones)
        }
        3 => (track.outer_presented && track.state.last_media_id >= value?)
            .then_some(track.state.last_media_id),
        4 => {
            let pts = i64::try_from(value?).ok()?;
            (track.outer_presented && track.last_pts_us >= pts)
                .then_some(track.last_pts_us.max(0) as u64)
        }
        5 => track.playing.then_some(1),
        6 => (track.state.milestones & MILESTONE_BUFFERED_ENDED != 0).then_some(1),
        7 => (track.state.milestones & MILESTONE_CHANNEL_ACCEPTED != 0).then_some(1),
        8 => (track.state.milestones & MILESTONE_CHANNEL_DETACHED != 0).then_some(1),
        9 => track.state.lost.then_some(1),
        _ => None,
    }
}

fn wait_payload(
    key: TrackKey,
    track: &TrackEntry,
    condition: u64,
    observed: u64,
) -> Vec<(u64, Value)> {
    vec![
        (0, Value::Unsigned(key.surface.context)),
        (1, Value::Unsigned(key.surface.surface)),
        (2, Value::Unsigned(key.track)),
        (3, Value::Unsigned(track.state.revision.get())),
        (4, Value::Unsigned(track.state.channel_generation.get())),
        (5, Value::Unsigned(condition)),
        (6, Value::Unsigned(observed)),
    ]
}

fn validate_node_mutations(
    state: &State,
    session_id: u64,
    mutations: &[NodeMutation],
) -> Result<(), ControlError> {
    let mut live = state
        .nodes
        .iter()
        .filter_map(|(key, entry)| {
            (key.session == session_id).then_some((*key, entry.node.clone()))
        })
        .collect::<HashMap<_, _>>();
    for mutation in mutations {
        match mutation {
            NodeMutation::Create(node) => {
                let key = NodeKey {
                    session: session_id,
                    context: node.owning_context_id,
                    node: node.node_id,
                };
                if live.contains_key(&key) {
                    return Err(ControlError::state("scene node identity is already live"));
                }
                validate_node_surface(state, session_id, node)?;
                live.insert(key, node.clone());
            }
            NodeMutation::Update(node) => {
                let key = NodeKey {
                    session: session_id,
                    context: node.owning_context_id,
                    node: node.node_id,
                };
                if !live.contains_key(&key) {
                    return Err(ControlError::missing("scene node does not exist"));
                }
                validate_node_surface(state, session_id, node)?;
                live.insert(key, node.clone());
            }
            NodeMutation::Delete(key) => {
                if live.remove(key).is_none() {
                    return Err(ControlError::missing("scene node does not exist"));
                }
            }
        }
    }
    if live.len() > state.config.media.max_nodes {
        return Err(ControlError {
            code: messages::ERROR_LIMIT_EXCEEDED,
            message: "scene node capacity is exhausted",
        });
    }
    Ok(())
}

fn validate_node_surface(
    state: &State,
    session_id: u64,
    node: &ProtocolSceneNode,
) -> Result<(), ControlError> {
    state
        .config
        .target
        .validate_node(node)
        .map_err(ControlError::bad)?;
    let surface = SurfaceKey {
        session: session_id,
        context: node.surface_context_id,
        surface: node.surface_id,
    };
    if !state.surfaces.contains_key(&surface) {
        return Err(ControlError::missing(
            "scene node references a missing surface",
        ));
    }
    if node.owning_context_id
        != state
            .sessions
            .get(&session_id)
            .map_or(0, |session| session.root_context)
    {
        return Err(ControlError::state(
            "scene node is outside the root context",
        ));
    }
    Ok(())
}

fn validate_surface_for_target(
    target: &dyn crate::PresentationTarget,
    definition: &SurfaceDefinition,
) -> Result<(), ControlError> {
    target
        .validate_surface(definition)
        .map_err(ControlError::bad)
}

fn apply_node_mutations(state: &mut State, session_id: u64, mutations: Vec<NodeMutation>) {
    let pane = state
        .sessions
        .get(&session_id)
        .map_or(0, |session| session.pane);
    for mutation in mutations {
        match mutation {
            NodeMutation::Create(node) | NodeMutation::Update(node) => {
                state.nodes.insert(
                    NodeKey {
                        session: session_id,
                        context: node.owning_context_id,
                        node: node.node_id,
                    },
                    NodeEntry { pane, node },
                );
            }
            NodeMutation::Delete(key) => {
                state.nodes.remove(&key);
            }
        }
    }
}

fn selected_visual_track(state: &State, surface: SurfaceKey) -> Option<TrackKey> {
    let active = state.surfaces.get(&surface)?.active_slots.values();
    for track in active {
        let key = TrackKey {
            surface,
            track: *track,
        };
        if state
            .tracks
            .get(&key)
            .is_some_and(|entry| !matches!(entry.configuration.kind, KindConfiguration::Audio(_)))
        {
            return Some(key);
        }
    }
    state.tracks.iter().find_map(|(key, entry)| {
        (key.surface == surface && !matches!(entry.configuration.kind, KindConfiguration::Audio(_)))
            .then_some(*key)
    })
}

fn projected_node_config(
    node: &ProtocolSceneNode,
    track: SourceKey,
    session: &SessionRuntime,
    viewport_offset: usize,
    target_profile: &str,
    target_extent: Option<vivid_protocol::geometry::TargetExtent>,
) -> Option<SceneNodeConfig> {
    if target_profile == registry::DESKTOP_SURFACE {
        let geometry = NodeGeometry::decode(&node.geometry).ok()?;
        let rect = geometry.project(target_extent?).ok()?;
        let clip = node.clip.as_ref().and_then(|clip| {
            decode_clip(clip).ok().map(|clip| ClipRect {
                x: clip.x,
                y: clip.y,
                width: clip.width,
                height: clip.height,
            })
        });
        return Some(SceneNodeConfig {
            node: NodeConfig {
                node_id: node.node_id,
                track,
                x: rect.x,
                y: rect.y,
                width: rect.width,
                height: rect.height,
                z_index: node.z_index,
                visible: node.visible,
                anchor_id: None,
            },
            clip,
        });
    }
    let geometry_value = Value::Map(node.geometry.clone());
    let geometry = StrictMap::new(
        "terminal scene geometry",
        &geometry_value,
        &[0, 1, 2, 3, 4, 5, 6, 7],
    )
    .ok()?;
    let kind = geometry.required_u64(0).ok()?;
    let mut x = geometry.required(1).ok()?.as_i64()?;
    let mut y = geometry.required(2).ok()?.as_i64()?;
    let width = geometry.required(3).ok()?.as_i64()?;
    let height = geometry.required(4).ok()?.as_i64()?;
    if width <= 0 || height <= 0 {
        return None;
    }
    let anchor_id = if kind == 2 {
        let context = geometry.required_u64(6).ok()?;
        let anchor = geometry.required_u64(7).ok()?;
        let (row, column) = session.anchors.get(&(context, anchor)).copied()?;
        x = x.checked_add(i64::try_from(column).ok()?.checked_shl(32)?)?;
        y = y.checked_add(i64::from(row).checked_shl(32)?)?;
        Some(anchor)
    } else if kind == 1 {
        None
    } else {
        return None;
    };
    y = y.checked_sub(i64::try_from(viewport_offset).ok()?.checked_shl(32)?)?;
    let clip = node.clip.as_ref().and_then(|clip| {
        let clip_value = Value::Map(clip.clone());
        let clip = StrictMap::new("terminal clip", &clip_value, &[0, 1, 2, 3]).ok()?;
        Some(ClipRect {
            x: clip.required(0).ok()?.as_i64()?,
            y: clip.required(1).ok()?.as_i64()?,
            width: clip.required(2).ok()?.as_i64()?,
            height: clip.required(3).ok()?.as_i64()?,
        })
    });
    Some(SceneNodeConfig {
        node: NodeConfig {
            node_id: node.node_id,
            track,
            x,
            y,
            width,
            height,
            z_index: node.z_index,
            visible: node.visible,
            anchor_id,
        },
        clip,
    })
}

fn source_descriptor(
    tracks: &HashMap<TrackKey, TrackEntry>,
    key: TrackKey,
    track: &TrackEntry,
) -> SourceDescriptor {
    match &track.configuration.kind {
        KindConfiguration::Raster(config) => SourceDescriptor::Raster(config.clone()),
        KindConfiguration::EncodedImage(config) => SourceDescriptor::Image(config.clone()),
        KindConfiguration::Video(config) => SourceDescriptor::Video(config.clone()),
        KindConfiguration::Audio(config) => {
            let linked_video_source_id = tracks.iter().find_map(|(candidate, entry)| {
                (candidate.surface == key.surface
                    && candidate.track != key.track
                    && matches!(entry.configuration.kind, KindConfiguration::Video(_)))
                .then_some(candidate.track)
            });
            SourceDescriptor::Audio(AudioSourceConfig {
                linked_video_source_id,
                codec: config.codec.clone(),
                packetization: config.packetization.clone(),
                extradata: config.extradata.clone(),
                sample_rate: config.sample_rate,
                channels: u16::from(config.channels),
                channel_mask: config.channel_mask,
                bitrate: track.configuration.maximum_encoded_bits_per_second,
                max_access_unit_bytes: config.maximum_access_unit_bytes,
                codec_string: config.codec_string.clone(),
            })
        }
    }
}

fn semantic_descriptor(descriptor: &SurfaceDescriptor) -> SemanticDescriptor {
    SemanticDescriptor {
        role: descriptor.role as u64,
        title: descriptor.title.clone(),
        content_revision: descriptor.semantic_content_revision,
        semantic_availability: descriptor.semantic_availability,
        locator: descriptor.locator_hint.clone(),
    }
}

fn supports_track(configuration: &TrackConfiguration) -> bool {
    (1..=4).contains(&configuration.slot)
        && match &configuration.kind {
            KindConfiguration::Video(video) => {
                media::is_portable_packetization(&video.codec, &video.packetization)
            }
            KindConfiguration::Audio(audio) => media::validate_audio_initialization(
                &audio.codec,
                &audio.packetization,
                &audio.extradata,
                audio.sample_rate,
                u16::from(audio.channels),
            )
            .is_ok(),
            KindConfiguration::Raster(_) | KindConfiguration::EncodedImage(_) => true,
        }
}

fn surface_ready_payload(key: SurfaceKey, surface: &SurfaceState) -> Vec<(u64, Value)> {
    vec![
        (0, Value::Unsigned(key.context)),
        (1, Value::Unsigned(key.surface)),
        (2, Value::Unsigned(surface.revision.get())),
        (3, Value::Unsigned(surface.generation.get())),
        (4, Value::Unsigned(surface.definition.policy)),
        (5, Value::Map(surface.definition.profile_parameters.clone())),
    ]
}

fn surface_status_payload(key: SurfaceKey, surface: &SurfaceEntry) -> Vec<(u64, Value)> {
    let definition = &surface.state.definition;
    vec![
        (0, Value::Unsigned(key.context)),
        (1, Value::Unsigned(key.surface)),
        (2, Value::Unsigned(surface.state.revision.get())),
        (3, Value::Unsigned(surface.state.generation.get())),
        (4, Value::Text(definition.semantic_profile.clone())),
        (5, Value::Unsigned(definition.coordinate_model as u64)),
        (6, Value::Unsigned(definition.logical_width)),
        (7, Value::Unsigned(definition.logical_height)),
        (8, Value::Unsigned(definition.scale_numerator)),
        (9, Value::Unsigned(definition.scale_denominator)),
        (10, Value::Unsigned(u64::from(definition.rotation))),
        (
            11,
            definition
                .descriptor
                .to_value()
                .unwrap_or(Value::Map(vec![])),
        ),
        (12, Value::Unsigned(definition.policy)),
        (
            13,
            Value::Map(
                surface
                    .active_slots
                    .iter()
                    .map(|(slot, track)| (*slot, Value::Unsigned(*track)))
                    .collect(),
            ),
        ),
        (14, Value::Unsigned(1)),
        (15, Value::Map(definition.profile_parameters.clone())),
    ]
}

fn track_ready_payload(
    key: TrackKey,
    configuration: &TrackConfiguration,
    state: &TrackState,
) -> Vec<(u64, Value)> {
    let mut payload = vec![
        (0, Value::Unsigned(key.surface.context)),
        (1, Value::Unsigned(key.surface.surface)),
        (2, Value::Unsigned(key.track)),
        (3, Value::Unsigned(state.revision.get())),
        (4, Value::Unsigned(state.channel_generation.get())),
        (5, Value::Unsigned(CHANNEL_OPEN_DEADLINE_US)),
        (
            6,
            Value::Unsigned(u64::from(configuration.maximum_record_body)),
        ),
        (
            7,
            Value::Map(configuration.payload(false).unwrap_or_default()),
        ),
        (8, Value::Bool(true)),
    ];
    if let KindConfiguration::Raster(raster) = &configuration.kind
        && raster.delta_enabled
    {
        payload.push((
            9,
            Value::Unsigned(u64::from(raster.maximum_delta_operations)),
        ));
    }
    payload
}

fn track_status_payload(
    key: TrackKey,
    track: &TrackEntry,
    gain_supported: bool,
) -> Vec<(u64, Value)> {
    let mut payload = vec![
        (0, Value::Unsigned(key.surface.context)),
        (1, Value::Unsigned(key.surface.surface)),
        (2, Value::Unsigned(key.track)),
        (3, Value::Unsigned(track.state.revision.get())),
        (4, Value::Unsigned(track.configuration.kind.kind() as u64)),
        (5, Value::Unsigned(track.configuration.mode as u64)),
        (6, Value::Unsigned(if track.state.lost { 6 } else { 1 })),
        (7, Value::Unsigned(track.state.channel_generation.get())),
        (
            8,
            Value::Unsigned(if track.channel_writer.is_some() { 1 } else { 0 }),
        ),
        (9, Value::Unsigned(track.state.milestones)),
        (10, Value::Unsigned(u64::from(track.state.media_epoch))),
        (11, Value::Unsigned(track.state.last_media_id)),
        (12, Value::Unsigned(track.last_record_sequence)),
        (13, signed(track.last_pts_us)),
        (
            14,
            signed(if track.outer_presented {
                track.last_pts_us
            } else {
                0
            }),
        ),
        (15, Value::Unsigned(u64::from(track.outer_presented))),
        (16, Value::Unsigned(track.state.flow.sent_body_bytes)),
        (17, Value::Unsigned(track.state.flow.sent_media_records)),
        (18, Value::Unsigned(track.state.flow.maximum_body_bytes)),
        (19, Value::Unsigned(track.state.flow.maximum_media_records)),
        (20, Value::Unsigned(0)),
    ];
    if gain_supported && matches!(track.configuration.kind, KindConfiguration::Audio(_)) {
        payload.push((23, Value::Unsigned(track.audio_gain.raw())));
    }
    payload
}

fn surface_key_from_map(session: u64, map: &StrictMap<'_>) -> Result<SurfaceKey, ControlError> {
    Ok(SurfaceKey {
        session,
        context: required_u64(map, 0)?,
        surface: required_u64(map, 1)?,
    })
}

fn track_key_from_map(session: u64, map: &StrictMap<'_>) -> Result<TrackKey, ControlError> {
    Ok(TrackKey {
        surface: surface_key_from_map(session, map)?,
        track: required_u64(map, 2)?,
    })
}

fn track_key_from_value(session: u64, value: &Value) -> Result<TrackKey, ControlError> {
    let map = StrictMap::new("track identity", value, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        .map_err(|_| ControlError::bad("invalid track identity"))?;
    track_key_from_map(session, &map)
}

fn required_u64(map: &StrictMap<'_>, key: u64) -> Result<u64, ControlError> {
    map.required_u64(key)
        .map_err(|_| ControlError::bad("missing or invalid unsigned field"))
}

fn require_root_context(
    state: &State,
    session_id: u64,
    context_id: u64,
) -> Result<(), ControlError> {
    if state
        .sessions
        .get(&session_id)
        .is_some_and(|session| session.root_context == context_id)
    {
        Ok(())
    } else {
        Err(ControlError::state(
            "vvmux exposes only its finite root context",
        ))
    }
}

fn remove_track(state: &mut State, key: TrackKey) -> Result<(), ControlError> {
    if !state.tracks.contains_key(&key) {
        return Err(ControlError::missing("track does not exist"));
    }
    let pending_deliveries = state
        .deliveries
        .iter()
        .filter_map(|(delivery_id, delivery)| (delivery.track == key).then_some(*delivery_id))
        .collect::<Vec<_>>();
    for delivery_id in pending_deliveries {
        if let Some(delivery) = state.deliveries.remove(&delivery_id) {
            // Release while the track and its channel writer still exist. Dropping the delivery
            // first strands a paced outer writer in its capacity wait during seek teardown.
            release_delivery_allowance(state, &delivery);
        }
    }
    state
        .tracks
        .remove(&key)
        .expect("track existence checked above");
    if let Some(surface) = state.surfaces.get_mut(&key.surface) {
        let slots = surface.active_slots.len();
        surface
            .active_slots
            .retain(|_, track_id| *track_id != key.track);
        if surface.active_slots.len() != slots {
            surface.state.revision = surface
                .state
                .revision
                .advance()
                .map_err(|_| ControlError::state("surface revision exhausted"))?;
        }
    }
    Ok(())
}

fn remove_surface_children(state: &mut State, surface: SurfaceKey) {
    let tracks = state
        .tracks
        .keys()
        .copied()
        .filter(|key| key.surface == surface)
        .collect::<Vec<_>>();
    for track in tracks {
        let _ = remove_track(state, track);
    }
    state.nodes.retain(|key, node| {
        !(key.session == surface.session
            && node.node.surface_context_id == surface.context
            && node.node.surface_id == surface.surface)
    });
}

fn cleanup_session(state: &mut State, session: u64) {
    state.sessions.remove(&session);
    let surfaces = state
        .surfaces
        .keys()
        .copied()
        .filter(|key| key.session == session)
        .collect::<Vec<_>>();
    for surface in surfaces {
        state.surfaces.remove(&surface);
        remove_surface_children(state, surface);
    }
    state.nodes.retain(|key, _| key.session != session);
    state
        .transactions
        .retain(|(owner, _, _), _| *owner != session);
    state.idempotency.retain(|(owner, _), _| *owner != session);
    state
        .idempotency_order
        .retain(|(owner, _)| *owner != session);
    state
        .projected_sources
        .retain(|source| source.producer != session);
}

fn suspend_session(
    state: &mut State,
    session_id: u64,
    lease_key: (u64, u64),
) -> Option<((u64, u64), u64, Instant)> {
    let (resume_key, generation) = state.sessions.get(&session_id).map(|session| {
        (
            Secret32::new(*session.resume_key.expose()),
            session.resume_generation,
        )
    })?;
    let deadline = {
        let lease = state.leases.get_mut(&lease_key)?;
        if lease.definition.cleanup_policy != CleanupPolicy::SuspendOnUncleanLoss
            || lease.definition.requested_disconnect_grace_us == 0
            || lease.machine.confirm_transport_lost(false).ok() != Some(LeaseState::Suspended)
        {
            cleanup_session(state, session_id);
            state.leases.remove(&lease_key);
            return None;
        }
        let deadline =
            Instant::now() + Duration::from_micros(lease.definition.requested_disconnect_grace_us);
        lease.resume_key = Some(resume_key);
        lease.grace_deadline = Some(deadline);
        lease.revision = lease.revision.saturating_add(1);
        deadline
    };
    if let Some(session) = state.sessions.get_mut(&session_id) {
        session.closed = true;
        session.cancelled_waits.clear();
        session.pending_waits = 0;
        session.anchors.clear();
        session.seen_anchors.clear();
    }
    for (key, track) in &mut state.tracks {
        if key.surface.session != session_id {
            continue;
        }
        track.channel_writer = None;
        track.playing = false;
        track.recovery_pending = matches!(track.configuration.kind, KindConfiguration::Video(_));
        track.recovery_requested = false;
        track.discard_blocked_for_recovery = false;
        if matches!(
            track.configuration.kind,
            KindConfiguration::Video(_) | KindConfiguration::Audio(_)
        ) {
            track.retained = None;
            track.retained_raster = None;
        }
        let _ = track.state.detach();
    }
    state
        .deliveries
        .retain(|_, delivery| delivery.track.surface.session != session_id);
    state
        .projected_sources
        .retain(|source| source.producer != session_id);
    Some((lease_key, generation, deadline))
}

fn spawn_lease_expiry(
    shared: Arc<Mutex<State>>,
    lease_key: (u64, u64),
    session_id: u64,
    generation: u64,
    deadline: Instant,
) {
    let _ = thread::Builder::new()
        .name("vvmux-vivid-lease-grace".into())
        .spawn(move || {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                thread::sleep(remaining);
            }
            let mut state = lock(&shared);
            let expired = state.leases.get(&lease_key).is_some_and(|lease| {
                lease.active_session == Some(session_id)
                    && lease.machine.state() == LeaseState::Suspended
                    && lease.machine.resume_generation().get() == generation
                    && lease
                        .grace_deadline
                        .is_some_and(|value| value <= Instant::now())
            });
            if expired {
                cleanup_session(&mut state, session_id);
                if let Some(mut lease) = state.leases.remove(&lease_key) {
                    let _ = lease.machine.expire();
                }
                advance_projection(&mut state);
            }
        });
}

fn detach_session(state: &mut State, session: u64) {
    let Some(runtime) = state.sessions.get_mut(&session) else {
        return;
    };
    runtime.closed = true;
    runtime.cancelled_waits.clear();
    runtime.pending_waits = 0;
    let anchors = runtime.anchors.clone();

    let tracks = state
        .tracks
        .keys()
        .copied()
        .filter(|key| key.surface.session == session)
        .collect::<Vec<_>>();
    for key in tracks {
        let retain = state.tracks.get(&key).is_some_and(|track| {
            has_retained_media(track)
                && matches!(
                    track.configuration.kind,
                    KindConfiguration::EncodedImage(_) | KindConfiguration::Raster(_)
                )
        });
        if retain {
            if let Some(track) = state.tracks.get_mut(&key) {
                track.channel_writer = None;
                track.playing = false;
            }
        } else {
            let _ = remove_track(state, key);
        }
    }

    let static_surfaces = state
        .tracks
        .keys()
        .filter(|key| key.surface.session == session)
        .map(|key| key.surface)
        .collect::<HashSet<_>>();
    let retained_surfaces = state
        .nodes
        .values()
        .filter_map(|node| {
            let surface = SurfaceKey {
                session,
                context: node.node.surface_context_id,
                surface: node.node.surface_id,
            };
            (node_uses_live_anchor(&node.node, &anchors) && static_surfaces.contains(&surface))
                .then_some(surface)
        })
        .collect::<HashSet<_>>();
    let tracks = state
        .tracks
        .keys()
        .copied()
        .filter(|key| key.surface.session == session && !retained_surfaces.contains(&key.surface))
        .collect::<Vec<_>>();
    for track in tracks {
        let _ = remove_track(state, track);
    }
    state
        .surfaces
        .retain(|key, _| key.session != session || retained_surfaces.contains(key));
    state.nodes.retain(|key, node| {
        key.session != session
            || (node_uses_live_anchor(&node.node, &anchors)
                && retained_surfaces.contains(&SurfaceKey {
                    session,
                    context: node.node.surface_context_id,
                    surface: node.node.surface_id,
                }))
    });
    state
        .transactions
        .retain(|(owner, _, _), _| *owner != session);
    state.idempotency.retain(|(owner, _), _| *owner != session);
    state
        .idempotency_order
        .retain(|(owner, _)| *owner != session);
}

fn node_uses_live_anchor(
    node: &ProtocolSceneNode,
    anchors: &HashMap<(u64, u64), (i32, usize)>,
) -> bool {
    let geometry = Value::Map(node.geometry.clone());
    let Ok(geometry) = StrictMap::new(
        "terminal scene geometry",
        &geometry,
        &[0, 1, 2, 3, 4, 5, 6, 7],
    ) else {
        return false;
    };
    geometry.required_u64(0).ok() == Some(2)
        && geometry
            .required_u64(6)
            .ok()
            .zip(geometry.required_u64(7).ok())
            .is_some_and(|anchor| anchors.contains_key(&anchor))
}

/// Return the flow allowance a finished delivery was holding.
///
/// A pane producer spends allowance when it writes a record and only gets it back when the
/// record leaves the bridge. Whether the outer presenter displayed the record decides milestones
/// and recovery, not flow: a delivery the bridge failed or abandoned - which every outer resize
/// can cause, since it rebuilds the outer session under in-flight media - has stopped occupying
/// the bridge either way. Withholding the allowance from those would shrink the producer's window
/// by one record each time until it reaches zero and the producer blocks in a credit wait forever.
fn release_delivery_allowance(state: &mut State, delivery: &PendingDelivery) {
    let projected = state
        .projected_sources
        .contains(&bridge_track_key(delivery.track));
    let Some(track) = state.tracks.get_mut(&delivery.track) else {
        return;
    };
    let returned_bytes = track
        .state
        .flow
        .maximum_body_bytes
        .saturating_add(delivery.bytes);
    let returned_records = track.state.flow.maximum_media_records.saturating_add(1);
    let maximum_record_body = u64::from(track.configuration.maximum_record_body);
    let rolling_records = if projected {
        ROLLING_FLOW_RECORDS.min(
            track
                .configuration
                .maximum_inflight_body_bytes
                .checked_div(maximum_record_body)
                .unwrap_or(0)
                .max(INITIAL_FLOW_RECORDS),
        )
    } else {
        INITIAL_FLOW_RECORDS
    };
    let rolling_bytes = if projected {
        track
            .configuration
            .maximum_inflight_body_bytes
            .min(maximum_record_body.saturating_mul(rolling_records))
    } else {
        maximum_record_body
    };
    let available_bytes = returned_bytes.saturating_sub(track.state.flow.sent_body_bytes);
    let available_records = returned_records.saturating_sub(track.state.flow.sent_media_records);
    let bytes = returned_bytes.saturating_add(rolling_bytes.saturating_sub(available_bytes));
    let records =
        returned_records.saturating_add(rolling_records.saturating_sub(available_records));
    track.state.flow.raise_maxima(bytes, records);
    send_flow_update(delivery.track, track);
}

fn send_flow_update(key: TrackKey, track: &TrackEntry) {
    let Some(writer) = &track.channel_writer else {
        return;
    };
    if let Ok(body) = Envelope::new(
        0,
        vec![
            (0, Value::Unsigned(key.surface.context)),
            (1, Value::Unsigned(key.surface.surface)),
            (2, Value::Unsigned(key.track)),
            (3, Value::Unsigned(track.state.channel_generation.get())),
            (4, Value::Unsigned(track.state.flow.maximum_body_bytes)),
            (5, Value::Unsigned(track.state.flow.maximum_media_records)),
        ],
    )
    .encode()
    {
        let _ = writer.write_record(messages::MAX_CHANNEL_DATA, key.track, &body);
    }
}

fn send_need_keyframe(key: TrackKey, track: &TrackEntry, minimum_epoch: u32, reason: u64) -> bool {
    let Some(writer) = &track.channel_writer else {
        return false;
    };
    if let Ok(body) = Envelope::new(
        0,
        vec![
            (0, Value::Unsigned(key.surface.context)),
            (1, Value::Unsigned(key.surface.surface)),
            (2, Value::Unsigned(key.track)),
            (3, Value::Unsigned(track.state.channel_generation.get())),
            (4, Value::Unsigned(u64::from(minimum_epoch))),
            (5, Value::Unsigned(reason)),
        ],
    )
    .encode()
    {
        return writer
            .write_record(messages::NEED_KEYFRAME, key.track, &body)
            .is_ok();
    }
    false
}

fn send_need_full_frame(key: TrackKey, track: &TrackEntry) {
    let Some(writer) = &track.channel_writer else {
        return;
    };
    if let Ok(body) = Envelope::new(
        0,
        vec![
            (0, Value::Unsigned(key.surface.context)),
            (1, Value::Unsigned(key.surface.surface)),
            (2, Value::Unsigned(key.track)),
            (3, Value::Unsigned(track.state.channel_generation.get())),
            (4, Value::Unsigned(1)),
        ],
    )
    .encode()
    {
        let _ = writer.write_record(messages::NEED_FULL_FRAME, key.track, &body);
    }
}

fn presenter_contract(
    config: &MediaConfig,
    target: &dyn crate::PresentationTarget,
) -> ResourceContract {
    let mut contract = ResourceContract::denied();
    for (resource, value) in [
        (Resource::Surfaces, config.max_sources as u64),
        (Resource::Tracks, config.max_sources as u64),
        (Resource::Nodes, config.max_nodes as u64),
        (Resource::VideoTracks, config.max_sources as u64),
        (Resource::AudioTracks, config.max_sources as u64),
        (Resource::RasterTracks, config.max_sources as u64),
        (Resource::ImageTracks, config.max_sources as u64),
        (Resource::DecoderInstances, config.max_sources as u64),
        (Resource::CodedPixelsPerTrack, 8192 * 8192),
        (Resource::DecodedPixelsPerSecond, 8192 * 8192 * 60),
        (Resource::EncodedBitsPerSecond, 1_000_000_000),
        (Resource::MediaRecordsPerSecond, 4_000),
        (Resource::AudioSampleRate, 192_000),
        (Resource::AudioChannelsPerTrack, 8),
        (Resource::InflightMediaBytes, config.ipc_queue_bytes as u64),
        (Resource::TrackConnections, config.max_sources as u64),
        (
            Resource::RetainedPixels,
            config.aggregate_retained_bytes / 4,
        ),
        (
            Resource::MediaRecordBody,
            u64::from(vivid_protocol::HARD_MAX_RECORD_BODY),
        ),
        (
            Resource::ControlRecordBody,
            u64::from(vivid_protocol::CONTROL_MAX_RECORD_BODY),
        ),
        (Resource::PendingRequests, 256),
        (Resource::RegisteredWaits, MAX_WAITS as u64),
        (Resource::IdempotencyEntries, 256),
        (Resource::ChildSessionLeases, 0),
        (Resource::DisconnectGraceUs, 0),
        (Resource::InputEventsPerSecond, 0),
        (Resource::ObservationQueueEntries, 64),
        (Resource::ImageCacheBytes, config.aggregate_retained_bytes),
        (Resource::OpenSceneTransactions, config.max_nodes as u64),
        (Resource::ChildContexts, 0),
        (Resource::SuspendedChildSessions, 0),
        (
            Resource::PendingChannelOpenAttempts,
            config.max_sources as u64,
        ),
        (
            Resource::ActiveTerminalAnchors,
            if target.accepts_anchors() {
                config.max_anchors as u64
            } else {
                0
            },
        ),
        (
            Resource::SeenTerminalAnchorIds,
            if target.accepts_anchors() {
                config.max_anchors as u64
            } else {
                0
            },
        ),
    ] {
        contract.set(resource, value);
    }
    contract
}

fn target_descriptor(metrics: Metrics) -> Vec<(u64, Value)> {
    vec![
        (0, Value::Unsigned(u64::from(metrics.viewport_width))),
        (1, Value::Unsigned(u64::from(metrics.viewport_height))),
        (2, Value::Unsigned(u64::from(metrics.columns))),
        (3, Value::Unsigned(u64::from(metrics.rows))),
        (4, Value::Unsigned(u64::from(metrics.cell_width))),
        (5, Value::Unsigned(u64::from(metrics.cell_height))),
        (6, Value::Bool(true)),
        (7, Value::Unsigned(3)),
        (8, Value::Unsigned(MAX_ACTIVE_ANCHORS as u64)),
    ]
}

fn target_changed_payload(target: &TargetState, reason_mask: u64) -> Vec<(u64, Value)> {
    let mut payload = target.descriptor.clone();
    payload.push((9, Value::Unsigned(target.generation)));
    payload.push((10, Value::Unsigned(reason_mask)));
    payload
}

fn protocol_error(
    request_id: u64,
    code: u64,
    fatal: bool,
    diagnostic: impl Into<String>,
) -> io::Result<Vec<u8>> {
    ErrorReply {
        code,
        request_id,
        detail: ErrorDetail::new(vec![]).map_err(io::Error::other)?,
        fatal,
        diagnostic: diagnostic.into(),
    }
    .encode()
    .map_err(io::Error::other)
}

fn send_fatal(writer: &Writer, request_id: u64, code: u64, diagnostic: &'static str) -> io::Error {
    if let Ok(body) = protocol_error(request_id, code, true, diagnostic) {
        let _ = writer.write_record(messages::ERROR, 0, &body);
    }
    io::Error::new(io::ErrorKind::InvalidData, diagnostic)
}

fn advance_projection(state: &mut State) {
    state.projection_revision = state.projection_revision.saturating_add(1);
}

fn kind_name(kind: &KindConfiguration) -> &'static str {
    match kind {
        KindConfiguration::Video(_) => "video",
        KindConfiguration::Audio(_) => "audio",
        KindConfiguration::Raster(_) => "raster",
        KindConfiguration::EncodedImage(_) => "image",
    }
}

fn nonnegative(value: i32) -> Value {
    if value >= 0 {
        Value::Unsigned(value as u64)
    } else {
        Value::Negative(i64::from(value))
    }
}

fn signed(value: i64) -> Value {
    if value >= 0 {
        Value::Unsigned(value as u64)
    } else {
        Value::Negative(value)
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(DIGITS[(byte >> 4) as usize] as char);
        value.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    value
}

fn profile_fingerprint(target_profile: &str, accepted_profiles: &[String]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"VIVID-GATEWAY-PROFILES-1");
    hasher.update((target_profile.len() as u64).to_be_bytes());
    hasher.update(target_profile.as_bytes());
    for profile in accepted_profiles {
        hasher.update((profile.len() as u64).to_be_bytes());
        hasher.update(profile.as_bytes());
    }
    hasher.finalize().into()
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn with_context(error: io::Error, context: &'static str) -> io::Error {
    io::Error::new(error.kind(), format!("{context}: {error}"))
}

fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BridgeSourceKey;
    use vivid_protocol::track::{
        AudioConfiguration, KindConfiguration, RasterConfiguration, TrackMode,
    };
    use vivid_sdk::{
        CoordinateModel, Fit, LaneClass, MILESTONE_OUTPUT_READY, ProducerAuthentication,
        ProducerConfig, RequestMetadata, SceneNode, SessionLeaseBuilder, SlotBinding,
        SurfaceDefinition, SurfaceDescriptor, SurfaceRole, TrackWaitCondition,
    };

    /// A local `PresenterListener` for gateway tests. Unix uses a filesystem socket while Windows
    /// uses the same loopback TCP transport as the shipping nested presenter.
    #[cfg(unix)]
    struct TestSocketListener {
        inner: std::os::unix::net::UnixListener,
        endpoint: std::path::PathBuf,
    }

    #[cfg(unix)]
    impl TestSocketListener {
        fn bind(path: std::path::PathBuf) -> io::Result<Self> {
            let inner = std::os::unix::net::UnixListener::bind(&path)?;
            inner.set_nonblocking(true)?;
            Ok(Self {
                inner,
                endpoint: path,
            })
        }
    }

    #[cfg(unix)]
    impl PresenterListener for TestSocketListener {
        fn endpoint(&self) -> String {
            format!("unix:{}", self.endpoint.display())
        }

        fn accept(&self) -> io::Result<Transport> {
            let (stream, _) = self.inner.accept()?;
            stream.set_nonblocking(false)?;
            let reader = stream.try_clone()?;
            let cancel_reader = reader.try_clone()?;
            let cancel_writer = stream.try_clone()?;
            let cancel = crate::ConnectionCancel::new(move || {
                let _ = cancel_reader.shutdown(std::net::Shutdown::Both);
                let _ = cancel_writer.shutdown(std::net::Shutdown::Both);
            });
            let timeout = Arc::new(|_: Option<Duration>| Ok(()));
            Ok(Transport::new(
                Box::new(reader),
                Box::new(stream),
                cancel,
                timeout,
            ))
        }
    }

    #[cfg(windows)]
    struct TestSocketListener {
        inner: std::net::TcpListener,
        endpoint: String,
    }

    #[cfg(windows)]
    impl TestSocketListener {
        fn bind(_path: std::path::PathBuf) -> io::Result<Self> {
            let inner = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
            inner.set_nonblocking(true)?;
            let endpoint = format!("tcp:{}", inner.local_addr()?);
            Ok(Self { inner, endpoint })
        }
    }

    #[cfg(windows)]
    impl PresenterListener for TestSocketListener {
        fn endpoint(&self) -> String {
            self.endpoint.clone()
        }

        fn accept(&self) -> io::Result<Transport> {
            let (stream, peer) = self.inner.accept()?;
            if !peer.ip().is_loopback() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "test presenter peer is not loopback",
                ));
            }
            stream.set_nonblocking(false)?;
            stream.set_nodelay(true)?;
            let reader = stream.try_clone()?;
            let timeout_stream = stream.try_clone()?;
            let cancel_reader = stream.try_clone()?;
            let cancel_writer = stream.try_clone()?;
            let cancel = crate::ConnectionCancel::new(move || {
                let _ = cancel_reader.shutdown(std::net::Shutdown::Both);
                let _ = cancel_writer.shutdown(std::net::Shutdown::Both);
            });
            let timeout = Arc::new(move |value| timeout_stream.set_read_timeout(value));
            Ok(Transport::new(
                Box::new(reader),
                Box::new(stream),
                cancel,
                timeout,
            ))
        }
    }

    fn producer(endpoint: String, secret: &str) -> ProducerConfig {
        ProducerConfig {
            endpoint_control: Some(endpoint),
            authentication: ProducerAuthentication::root_hex(secret).unwrap(),
            producer_name: "vvmux-inner-test".into(),
            producer_version: "1.5".into(),
            target_profile: vivid_sdk::TERMINAL_SURFACE.into(),
            required_profiles: vec![
                vivid_sdk::LIVE_MEDIA.into(),
                vivid_sdk::OBSERVABILITY.into(),
                vivid_sdk::TERMINAL_SURFACE.into(),
                vivid_sdk::TIMED_MEDIA.into(),
                vivid_sdk::CORE_CONTROL.into(),
            ],
            optional_profiles: vec![vivid_sdk::AUDIO_GAIN.into()],
            ..ProducerConfig::default()
        }
    }

    fn surface(context_id: u64, surface_id: u64) -> SurfaceDefinition {
        SurfaceDefinition {
            context_id,
            surface_id,
            semantic_profile: vivid_sdk::GENERIC_CONTENT.into(),
            coordinate_model: CoordinateModel::DesktopLogicalPixels,
            logical_width: 2,
            logical_height: 2,
            scale_numerator: 1,
            scale_denominator: 1,
            rotation: 0,
            descriptor: SurfaceDescriptor {
                role: SurfaceRole::Figure,
                title: "nested-test".into(),
                semantic_content_revision: 1,
                semantic_availability: 0,
                locator_hint: String::new(),
            },
            policy: 0,
            profile_parameters: vec![],
        }
    }

    fn raster(context_id: u64, surface_id: u64, track_id: u64) -> TrackConfiguration {
        TrackConfiguration {
            context_id,
            surface_id,
            track_id,
            slot: 3,
            mode: TrackMode::Live,
            lane: LaneClass::Bulk,
            maximum_record_body: media::rgba8_raw_frame_body_len(2, 2).unwrap(),
            maximum_rate_millihertz: 60_000,
            maximum_encoded_bits_per_second: 1_000_000,
            maximum_records_per_second: 60,
            maximum_inflight_body_bytes: 1024,
            kind: KindConfiguration::Raster(RasterConfiguration {
                width: 2,
                height: 2,
                alpha_mode: 1,
                delta_enabled: false,
                maximum_delta_operations: 1,
                zstd_enabled: false,
            }),
            target_latency_us: 16_000,
            maximum_latency_us: 100_000,
            retained_pixel_charge: 4,
        }
    }

    fn video(context_id: u64, surface_id: u64, track_id: u64) -> TrackConfiguration {
        TrackConfiguration {
            context_id,
            surface_id,
            track_id,
            slot: 1,
            mode: TrackMode::Timed,
            lane: LaneClass::Realtime,
            maximum_record_body: media::video_body_len(1024).unwrap(),
            maximum_rate_millihertz: 60_000,
            maximum_encoded_bits_per_second: 8_000_000,
            maximum_records_per_second: 60,
            maximum_inflight_body_bytes: 16 * 1024,
            kind: KindConfiguration::Video(VideoConfiguration {
                codec: "h264".into(),
                packetization: "h264-annexb-au-v1".into(),
                extradata: Vec::new(),
                coded_width: 16,
                coded_height: 16,
                profile: 0,
                level: 0,
                maximum_reorder_depth: 16,
                color_primaries: 1,
                transfer: 1,
                matrix: 1,
                signal_range: 1,
                aspect_numerator: 1,
                aspect_denominator: 1,
                maximum_access_unit_bytes: 1024,
                codec_string: None,
                decoder_configuration: None,
            }),
            target_latency_us: 20_000,
            maximum_latency_us: 1_000_000,
            retained_pixel_charge: 256,
        }
    }

    fn audio(context_id: u64, surface_id: u64, track_id: u64) -> TrackConfiguration {
        let maximum_record_body = media::audio_body_len(256).unwrap();
        TrackConfiguration {
            context_id,
            surface_id,
            track_id,
            slot: 2,
            mode: TrackMode::Timed,
            lane: LaneClass::Realtime,
            maximum_record_body,
            maximum_rate_millihertz: 50_000,
            maximum_encoded_bits_per_second: 512_000,
            maximum_records_per_second: 50,
            maximum_inflight_body_bytes: u64::from(maximum_record_body) * ROLLING_FLOW_RECORDS,
            kind: KindConfiguration::Audio(AudioConfiguration {
                codec: "pcm_s16le".into(),
                packetization: "pcm-packet-v1".into(),
                extradata: Vec::new(),
                sample_rate: 48_000,
                channels: 2,
                channel_mask: 3,
                maximum_access_unit_bytes: 256,
                codec_string: None,
            }),
            target_latency_us: 0,
            maximum_latency_us: 1_000_000,
            retained_pixel_charge: 0,
        }
    }

    #[test]
    fn idempotency_fingerprint_ignores_only_request_correlation() {
        let mut first = Envelope::new(41, vec![(0, Value::Unsigned(7))]);
        first.idempotency_key = Some([3; messages::IDEMPOTENCY_KEY_BYTES]);
        let mut retried = first.clone();
        retried.request_id = 99;
        assert_eq!(
            mutation_fingerprint(messages::PAUSE, 17, &first),
            mutation_fingerprint(messages::PAUSE, 17, &retried)
        );

        retried.payload = vec![(0, Value::Unsigned(8))];
        assert_ne!(
            mutation_fingerprint(messages::PAUSE, 17, &first),
            mutation_fingerprint(messages::PAUSE, 17, &retried)
        );

        let cached = messages::ok(41);
        let recorrelated = recorrelate_cached_reply(&cached, 99).unwrap();
        assert_eq!(
            messages::decode_control(&recorrelated).unwrap().request_id,
            99
        );
    }

    #[test]
    fn root_authenticated_sdk_session_relays_one_priming_raster() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        let surface = client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let track = client
            .create_track(raster(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let channel = client.open_track_channel(&track).unwrap();
        channel
            .send_raster(0, 1, &[0, 0, 0, 255].repeat(4), false)
            .unwrap();
        let event = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            event.source,
            BridgeSourceKey {
                producer: client.info().session_id,
                context,
                surface: 9,
                track: 11,
            }
        );
        assert_eq!(event.record_type, messages::RASTER_FRAME);
        assert!(!presenter.complete_bridge_delivery(event.delivery_id, true));
        client
            .activate_tracks(
                &surface,
                &[SlotBinding {
                    slot: 3,
                    track_id: track.id(),
                    expected_channel_generation: track.channel_generation(),
                    required_milestone: MILESTONE_OUTPUT_READY,
                }],
                &RequestMetadata::default(),
            )
            .unwrap();
        let bridged = presenter
            .projection_snapshot(&HashSet::from([7]))
            .bridge_projection();
        assert!(bridged.sources[0].live);
        assert!(bridged.sources[0].active);
        client.close().unwrap();
    }

    #[test]
    fn negotiated_audio_gain_is_reported_and_projected_to_the_outer_bridge() {
        let directory = tempfile::tempdir().unwrap();
        let presenter = VirtualVivid::start(
            TestSocketListener::bind(directory.path().join("audio-gain.sock")).unwrap(),
            MediaConfig::default(),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        assert!(client.supports(registry::AUDIO_GAIN));
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let track = client
            .create_track(audio(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let gain = AudioGain::from_percent(35).unwrap();

        client.set_audio_gain(&track, gain).unwrap();

        assert_eq!(client.query_track(&track).unwrap().audio_gain, Some(gain));
        let snapshot = presenter.projection_snapshot(&HashSet::from([7]));
        let projected = snapshot
            .sources
            .iter()
            .find(|source| source.key.track == track.id())
            .expect("audio source is projected");
        assert_eq!(projected.audio_gain, Some(gain));
        assert_eq!(
            snapshot.bridge_projection().sources[0].audio_gain,
            Some(gain.raw())
        );
        client.close().unwrap();
    }

    #[test]
    fn keyframe_recovery_remains_pending_until_outer_delivery_completes() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let track = client
            .create_track(video(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let channel = client.open_track_channel(&track).unwrap();
        presenter.projection_snapshot(&HashSet::from([7]));
        channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 41_667,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0x88],
            })
            .unwrap();
        let event = received.recv_timeout(Duration::from_secs(2)).unwrap();
        let source = BridgeSourceKey {
            producer: client.info().session_id,
            context,
            surface: 9,
            track: 11,
        };
        assert_eq!(event.source, source);
        assert_eq!(
            presenter.request_keyframe(source, None, 5),
            KeyframeRequestOutcome::Damped,
            "a recovery request racing a good in-flight keyframe must not make Vivi discard the \
             rest of the GOP"
        );
        assert!(
            lock(&presenter.state)
                .tracks
                .get(&inner_track_key(source))
                .is_some_and(|track| track.recovery_pending),
            "recovery must remain pending until the outer delivery completes"
        );
        let recovering = presenter.projection_snapshot(&HashSet::from([7]));
        assert!(
            recovering.videos_needing_keyframes.is_empty(),
            "a good in-flight keyframe must not make a brand-new projection request a replacement"
        );

        assert!(!presenter.complete_bridge_delivery(event.delivery_id, true));
        let recovered = presenter.projection_snapshot(&HashSet::from([7]));
        assert!(
            recovered.videos_needing_keyframes.is_empty(),
            "successful outer delivery must publish the end of the recovery episode"
        );
        assert_eq!(
            presenter.request_keyframe(source, None, 5),
            KeyframeRequestOutcome::Forwarded,
            "a later recovery episode must still reach the producer"
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match channel.take_event().unwrap() {
                Some(vivid_sdk::ChannelEvent::NeedKeyframe(_)) => break,
                Some(other) => panic!("unexpected video channel event: {other:?}"),
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(2)),
                None => panic!("the later recovery request never reached the producer"),
            }
        }

        channel
            .send_video(media::VideoPacket {
                epoch: 2,
                packet_id: 2,
                pts_us: 41_667,
                dts_us: 41_667,
                duration_us: 41_667,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0x99],
            })
            .unwrap();
        let recovered = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!presenter.complete_bridge_delivery(recovered.delivery_id, true));
        {
            let mut state = lock(&presenter.state);
            state
                .tracks
                .get_mut(&inner_track_key(source))
                .unwrap()
                .playing = true;
        }

        presenter.projection_snapshot(&HashSet::new());
        assert_eq!(
            presenter
                .projection_snapshot(&HashSet::from([7]))
                .videos_needing_keyframes,
            [source],
            "returning to a hidden tab must recover its replacement video decoder"
        );

        presenter.deactivate_bridge();
        let detached = presenter.projection_snapshot(&HashSet::from([7]));
        assert_eq!(
            detached.videos_needing_keyframes,
            [source],
            "a playing video must require decoder recovery after its presenter detaches"
        );
        channel
            .send_video(media::VideoPacket {
                epoch: 2,
                packet_id: 3,
                pts_us: 83_334,
                dts_us: 83_334,
                duration_us: 41_667,
                key: false,
                data: &[0, 0, 0, 1, 0x41, 0x77],
            })
            .unwrap();
        thread::sleep(Duration::from_millis(20));
        assert!(
            received.try_recv().is_err(),
            "inter-frame video must not poison the fresh decoder while recovery is pending"
        );
        assert_eq!(
            presenter.request_keyframe(source, None, 5),
            KeyframeRequestOutcome::Forwarded,
            "a fresh presenter must re-arm recovery instead of inheriting the detached bridge's \
             damped request state"
        );
        assert_eq!(
            presenter.request_keyframe(source, None, 5),
            KeyframeRequestOutcome::Damped,
            "only one producer request should be issued for the reattached recovery episode"
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match channel.take_event().unwrap() {
                Some(vivid_sdk::ChannelEvent::NeedKeyframe(_)) => break,
                Some(other) => panic!("unexpected video channel event: {other:?}"),
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(2)),
                None => panic!("reattached presenter recovery never reached the producer"),
            }
        }
        channel
            .send_video(media::VideoPacket {
                epoch: 3,
                packet_id: 4,
                pts_us: 125_001,
                dts_us: 125_001,
                duration_us: 41_667,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0xaa],
            })
            .unwrap();
        let reattached_keyframe = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(reattached_keyframe.recovered_keyframe, Some((3, 125_001)));
        assert!(!presenter.complete_bridge_delivery(reattached_keyframe.delivery_id, true));
        assert!(
            presenter
                .projection_snapshot(&HashSet::from([7]))
                .videos_needing_keyframes
                .is_empty(),
            "successful reattached keyframe delivery must complete the recovery episode"
        );
        client.close().unwrap();
    }

    #[test]
    fn superseded_delivery_releases_flow_without_starting_another_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("superseded.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let track = client
            .create_track(video(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let channel = client.open_track_channel(&track).unwrap();
        presenter.projection_snapshot(&HashSet::from([7]));
        let send = |packet_id, key| {
            channel
                .send_video(media::VideoPacket {
                    epoch: 1,
                    packet_id,
                    pts_us: i64::try_from(packet_id).unwrap() * 41_667,
                    dts_us: i64::try_from(packet_id).unwrap() * 41_667,
                    duration_us: 41_667,
                    key,
                    data: if key {
                        &[0, 0, 0, 1, 0x65, 0x88]
                    } else {
                        &[0, 0, 0, 1, 0x41, 0x88]
                    },
                })
                .unwrap();
        };

        send(1, true);
        let keyframe = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!presenter.complete_bridge_delivery(keyframe.delivery_id, true));
        send(2, false);
        let superseded = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(presenter.release_bridge_delivery(superseded.delivery_id));

        let source = BridgeSourceKey {
            producer: client.info().session_id,
            context,
            surface: 9,
            track: 11,
        };
        assert!(
            !lock(&presenter.state)
                .tracks
                .get(&inner_track_key(source))
                .unwrap()
                .recovery_pending,
            "superseding an obsolete interframe started a new recovery episode"
        );
        send(3, false);
        let next = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!presenter.complete_bridge_delivery(next.delivery_id, true));
        client.close().unwrap();
    }

    #[test]
    fn retired_channel_cleanup_cannot_stop_its_replacement_or_another_owner() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(16);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut seeking =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let mut neighbor =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let seeking_context = seeking.info().root_context_id;
        let neighbor_context = neighbor.info().root_context_id;
        // Both owners deliberately reuse the same numeric surface and track IDs.
        let seeking_surface = seeking
            .create_surface(surface(seeking_context, 9), &RequestMetadata::default())
            .unwrap();
        let neighbor_surface = neighbor
            .create_surface(surface(neighbor_context, 9), &RequestMetadata::default())
            .unwrap();
        let seeking_track = seeking
            .create_track(video(seeking_context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let neighbor_track = neighbor
            .create_track(video(neighbor_context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let seeking_old_channel = seeking.open_track_channel(&seeking_track).unwrap();
        let neighbor_channel = neighbor.open_track_channel(&neighbor_track).unwrap();
        presenter.projection_snapshot(&HashSet::from([7]));

        for (channel, pts_us) in [(&seeking_old_channel, 0), (&neighbor_channel, 1_000)] {
            channel
                .send_video(media::VideoPacket {
                    epoch: 1,
                    packet_id: 1,
                    pts_us,
                    dts_us: pts_us,
                    duration_us: 41_667,
                    key: true,
                    data: &[0, 0, 0, 1, 0x65, 0x88],
                })
                .unwrap();
        }
        for _ in 0..2 {
            let event = received.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(!presenter.complete_bridge_delivery(event.delivery_id, true));
        }
        for (session, surface, track) in [
            (&mut seeking, &seeking_surface, &seeking_track),
            (&mut neighbor, &neighbor_surface, &neighbor_track),
        ] {
            session
                .wait_track(
                    track,
                    vivid_sdk::TrackWaitCondition::MilestoneSet,
                    Some(MILESTONE_OUTPUT_READY),
                    1_000_000,
                )
                .unwrap();
            session
                .activate_tracks(
                    surface,
                    &[SlotBinding {
                        slot: 1,
                        track_id: track.id(),
                        expected_channel_generation: track.channel_generation(),
                        required_milestone: MILESTONE_OUTPUT_READY,
                    }],
                    &RequestMetadata::default(),
                )
                .unwrap();
            session.play(track, 0, 1, 1_000_000).unwrap();
        }

        seeking.pause(&seeking_track).unwrap();
        seeking.flush(&seeking_track, 2).unwrap();
        seeking
            .advance_channel(&seeking_track, 1, &RequestMetadata::default())
            .unwrap();
        let seeking_new_channel = seeking.open_track_channel(&seeking_track).unwrap();
        seeking_old_channel.close().unwrap();
        drop(seeking_old_channel);

        // Give the retired channel worker time to observe EOF after generation two is live. Its
        // cleanup used to detach and lose the replacement track here.
        thread::sleep(Duration::from_millis(100));
        let seeking_status = seeking.query_track(&seeking_track).unwrap();
        let neighbor_status = neighbor.query_track(&neighbor_track).unwrap();
        assert_eq!(seeking_status.lifecycle, 1);
        assert_eq!(seeking_status.channel_generation, ChannelGeneration::new(2));
        assert_eq!(seeking_status.attachment_state, 1);
        assert_eq!(neighbor_status.lifecycle, 1);
        assert_eq!(neighbor_status.channel_generation, ChannelGeneration::ONE);
        assert_eq!(neighbor_status.attachment_state, 1);

        seeking_new_channel
            .send_video(media::VideoPacket {
                epoch: 2,
                packet_id: 2,
                pts_us: 10_000_000,
                dts_us: 10_000_000,
                duration_us: 41_667,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0x99],
            })
            .unwrap();
        neighbor_channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 2,
                pts_us: 42_667,
                dts_us: 42_667,
                duration_us: 41_667,
                key: false,
                data: &[0, 0, 0, 1, 0x41, 0x88],
            })
            .unwrap();
        for _ in 0..2 {
            let event = received.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(!presenter.complete_bridge_delivery(event.delivery_id, true));
        }
        seeking
            .wait_track(
                &seeking_track,
                vivid_sdk::TrackWaitCondition::MilestoneSet,
                Some(MILESTONE_OUTPUT_READY),
                1_000_000,
            )
            .unwrap();
        seeking
            .activate_tracks(
                &seeking_surface,
                &[SlotBinding {
                    slot: 1,
                    track_id: seeking_track.id(),
                    expected_channel_generation: seeking_track.channel_generation(),
                    required_milestone: MILESTONE_OUTPUT_READY,
                }],
                &RequestMetadata::default(),
            )
            .unwrap();
        seeking
            .play(&seeking_track, 10_000_000, 1, 1_000_000)
            .unwrap();

        let projection = presenter.projection_snapshot(&HashSet::from([7]));
        let source_playing = |producer| {
            projection
                .sources
                .iter()
                .find(|source| source.key.producer == producer && source.key.track == 11)
                .is_some_and(|source| source.playing)
        };
        assert!(source_playing(seeking.info().session_id));
        assert!(source_playing(neighbor.info().session_id));
        seeking.close().unwrap();
        neighbor.close().unwrap();
    }

    #[test]
    fn failed_video_recovery_is_scoped_when_owners_reuse_numeric_ids() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(8);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        presenter.update_metrics(8, 80, 24, (8, 16));
        let first_secret = presenter.issue_pane_capability(7).unwrap();
        let second_secret = presenter.issue_pane_capability(8).unwrap();
        let mut first =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &first_secret)).unwrap();
        let mut second =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &second_secret)).unwrap();
        let first_context = first.info().root_context_id;
        let second_context = second.info().root_context_id;
        for (client, context) in [(&mut first, first_context), (&mut second, second_context)] {
            client
                .create_surface(surface(context, 9), &RequestMetadata::default())
                .unwrap();
        }
        let first_track = first
            .create_track(video(first_context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let second_track = second
            .create_track(video(second_context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let first_channel = first.open_track_channel(&first_track).unwrap();
        let second_channel = second.open_track_channel(&second_track).unwrap();
        let first_source = BridgeSourceKey {
            producer: first.info().session_id,
            context: first_context,
            surface: 9,
            track: 11,
        };
        let second_source = BridgeSourceKey {
            producer: second.info().session_id,
            context: second_context,
            surface: 9,
            track: 11,
        };
        presenter.projection_snapshot(&HashSet::from([7, 8]));

        for channel in [&first_channel, &second_channel] {
            channel
                .send_video(media::VideoPacket {
                    epoch: 1,
                    packet_id: 1,
                    pts_us: 0,
                    dts_us: 0,
                    duration_us: 41_667,
                    key: true,
                    data: &[0, 0, 0, 1, 0x65, 0x88],
                })
                .unwrap();
        }
        let initial = [
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
        ];
        assert_eq!(
            initial
                .iter()
                .map(|event| event.source)
                .collect::<HashSet<_>>(),
            HashSet::from([first_source, second_source])
        );
        for event in initial {
            assert!(!presenter.complete_bridge_delivery(event.delivery_id, true));
        }

        first_channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 2,
                pts_us: 41_667,
                dts_us: 41_667,
                duration_us: 41_667,
                key: false,
                data: &[0, 0, 0, 1, 0x41, 0x77],
            })
            .unwrap();
        let failed = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(failed.source, first_source);
        assert!(presenter.complete_bridge_delivery(failed.delivery_id, false));

        let recovering = presenter.projection_snapshot(&HashSet::from([7, 8]));
        assert_eq!(recovering.sources.len(), 2);
        assert_eq!(
            recovering
                .videos_needing_keyframes
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            HashSet::from([first_source])
        );
        let unaffected = recovering
            .sources
            .iter()
            .find(|source| source.key == second_source)
            .unwrap();
        assert!(
            unaffected.first_visible_presented,
            "the unrelated owner's presented state was changed by another owner's failure"
        );

        second_channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 2,
                pts_us: 41_667,
                dts_us: 41_667,
                duration_us: 41_667,
                key: false,
                data: &[0, 0, 0, 1, 0x41, 0x99],
            })
            .unwrap();
        let next = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(next.source, second_source);
        assert!(!presenter.complete_bridge_delivery(next.delivery_id, true));
        let after = presenter.projection_snapshot(&HashSet::from([7, 8]));
        assert_eq!(
            after
                .sources
                .iter()
                .find(|source| source.key == second_source)
                .unwrap()
                .last_inner_record_sequence,
            3,
            "the unrelated owner could not deliver its next valid media update"
        );
        assert_eq!(
            after
                .videos_needing_keyframes
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            HashSet::from([first_source])
        );
        first.close().unwrap();
        second.close().unwrap();
    }

    #[test]
    fn first_audio_delivery_opens_a_source_scoped_rolling_window() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(32);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let video = client
            .create_track(video(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let audio = client
            .create_track(audio(context, 9, 12), &RequestMetadata::default())
            .unwrap();
        let video_channel = Arc::new(client.open_track_channel(&video).unwrap());
        let audio_channel = Arc::new(client.open_track_channel(&audio).unwrap());
        let projection = presenter.projection_snapshot(&HashSet::from([7]));
        assert_eq!(projection.sources.len(), 2);

        video_channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 41_667,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0x88],
            })
            .unwrap();
        audio_channel
            .send_audio(media::AudioPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 21_333,
                trim_start_samples: 0,
                trim_end_samples: 0,
                data: &[0; 16],
            })
            .unwrap();

        let first = received.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = received.recv_timeout(Duration::from_secs(2)).unwrap();
        let (audio_delivery, video_delivery) = if first.source.track == 12 {
            (first.delivery_id, second.delivery_id)
        } else {
            (second.delivery_id, first.delivery_id)
        };
        assert!(!presenter.complete_bridge_delivery(audio_delivery, true));

        let (audio_written, audio_writes) = mpsc::sync_channel(ROLLING_FLOW_RECORDS as usize);
        let audio_writer = {
            let channel = audio_channel.clone();
            thread::spawn(move || {
                for packet_id in 2..=ROLLING_FLOW_RECORDS + 1 {
                    channel
                        .send_audio(media::AudioPacket {
                            epoch: 1,
                            packet_id,
                            pts_us: i64::try_from(packet_id - 1).unwrap() * 21_333,
                            dts_us: i64::try_from(packet_id - 1).unwrap() * 21_333,
                            duration_us: 21_333,
                            trim_start_samples: 0,
                            trim_end_samples: 0,
                            data: &[0; 16],
                        })
                        .unwrap();
                    audio_written.send(packet_id).unwrap();
                }
            })
        };
        let (video_written, video_writes) = mpsc::sync_channel(1);
        let video_writer = {
            let channel = video_channel.clone();
            thread::spawn(move || {
                channel
                    .send_video(media::VideoPacket {
                        epoch: 1,
                        packet_id: 2,
                        pts_us: 41_667,
                        dts_us: 41_667,
                        duration_us: 41_667,
                        key: false,
                        data: &[0, 0, 0, 1, 0x41, 0x88],
                    })
                    .unwrap();
                video_written.send(()).unwrap();
            })
        };

        for packet_id in 2..=ROLLING_FLOW_RECORDS + 1 {
            assert_eq!(
                audio_writes.recv_timeout(Duration::from_secs(2)).ok(),
                Some(packet_id),
                "audio remained stop-and-wait instead of receiving its bounded rolling window"
            );
        }
        assert!(
            video_writes
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "returning audio allowance must not enlarge an unrelated video track's window"
        );

        assert!(!presenter.complete_bridge_delivery(video_delivery, true));
        video_writes.recv_timeout(Duration::from_secs(2)).unwrap();
        audio_writer.join().unwrap();
        video_writer.join().unwrap();
        client.close().unwrap();
    }

    #[test]
    fn live_audio_primes_before_projection_ack_but_delivery_remains_parked() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let mut configuration = audio(context, 9, 12);
        configuration.mode = TrackMode::Live;
        let audio = client
            .create_track(configuration, &RequestMetadata::default())
            .unwrap();
        let channel = client.open_track_channel(&audio).unwrap();
        let prepared = presenter
            .prepare_projection_snapshot_with_viewports(&HashSet::from([7]), &HashMap::new());
        let source = prepared.sources[0].key;

        channel
            .send_audio(media::AudioPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 20_000,
                trim_start_samples: 0,
                trim_end_samples: 0,
                data: &[0; 16],
            })
            .unwrap();
        client
            .wait_track(
                &audio,
                TrackWaitCondition::MilestoneSet,
                Some(MILESTONE_OUTPUT_READY),
                500_000,
            )
            .unwrap();
        assert!(
            received.recv_timeout(Duration::from_millis(100)).is_err(),
            "the priming packet escaped before the outer projection acknowledgement"
        );

        presenter.activate_bridge_projection(
            &prepared.sources.iter().map(|source| source.key).collect(),
        );
        let delivered = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(delivered.source, source);
        assert!(!presenter.complete_bridge_delivery(delivered.delivery_id, true));
        client.close().unwrap();
    }

    #[test]
    fn hidden_and_detached_timed_media_stay_bounded_and_owner_scoped() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(32);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        presenter.update_metrics(8, 80, 24, (8, 16));
        let first_secret = presenter.issue_pane_capability(7).unwrap();
        let second_secret = presenter.issue_pane_capability(8).unwrap();
        let mut first =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &first_secret)).unwrap();
        let mut second =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &second_secret)).unwrap();
        let mut channels = Vec::new();
        for client in [&mut first, &mut second] {
            let context = client.info().root_context_id;
            client
                .create_surface(surface(context, 9), &RequestMetadata::default())
                .unwrap();
            let track = client
                .create_track(audio(context, 9, 12), &RequestMetadata::default())
                .unwrap();
            channels.push(Arc::new(client.open_track_channel(&track).unwrap()));
        }
        let first_source = BridgeSourceKey {
            producer: first.info().session_id,
            context: first.info().root_context_id,
            surface: 9,
            track: 12,
        };
        let second_source = BridgeSourceKey {
            producer: second.info().session_id,
            context: second.info().root_context_id,
            surface: 9,
            track: 12,
        };
        presenter.projection_snapshot(&HashSet::from([7, 8]));

        for channel in &channels {
            channel
                .send_audio(media::AudioPacket {
                    epoch: 1,
                    packet_id: 1,
                    pts_us: 0,
                    dts_us: 0,
                    duration_us: 20_000,
                    trim_start_samples: 0,
                    trim_end_samples: 0,
                    data: &[0; 16],
                })
                .unwrap();
        }
        for _ in 0..2 {
            let event = received.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(!presenter.complete_bridge_delivery(event.delivery_id, true));
        }

        // Tab 1 is now hidden. Both owners deliberately use surface 9 / track 12, so this also
        // proves the visibility mutation and backpressure stay scoped by full owner identity.
        presenter.projection_snapshot(&HashSet::from([8]));
        let hidden_packets = ROLLING_FLOW_RECORDS + 3;
        let (written, writes) = mpsc::channel();
        let hidden_channel = channels[0].clone();
        let hidden_writer = thread::spawn(move || {
            for packet_id in 2..=hidden_packets + 1 {
                hidden_channel
                    .send_audio(media::AudioPacket {
                        epoch: 1,
                        packet_id,
                        pts_us: i64::try_from(packet_id - 1).unwrap() * 20_000,
                        dts_us: i64::try_from(packet_id - 1).unwrap() * 20_000,
                        duration_us: 20_000,
                        trim_start_samples: 0,
                        trim_end_samples: 0,
                        data: &[0; 16],
                    })
                    .unwrap();
                written.send(packet_id).unwrap();
            }
        });
        for packet_id in 2..=ROLLING_FLOW_RECORDS + 1 {
            assert_eq!(
                writes.recv_timeout(Duration::from_secs(2)).ok(),
                Some(packet_id),
                "the hidden source did not spend its already-granted bounded window"
            );
        }
        assert!(
            writes.recv_timeout(Duration::from_millis(100)).is_err(),
            "the hidden audio source kept racing through the file after its bounded window"
        );
        assert!(
            received.try_recv().is_err(),
            "a hidden timed packet escaped into the outer delivery queue"
        );

        channels[1]
            .send_audio(media::AudioPacket {
                epoch: 1,
                packet_id: 2,
                pts_us: 20_000,
                dts_us: 20_000,
                duration_us: 20_000,
                trim_start_samples: 0,
                trim_end_samples: 0,
                data: &[0; 16],
            })
            .unwrap();
        let unaffected = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(unaffected.source, second_source);
        assert!(!presenter.complete_bridge_delivery(unaffected.delivery_id, true));

        let prepared = presenter
            .prepare_projection_snapshot_with_viewports(&HashSet::from([7, 8]), &HashMap::new());
        assert!(
            received.recv_timeout(Duration::from_millis(100)).is_err(),
            "submitting a visible tab must not release timed media before its apply ack"
        );
        presenter.activate_bridge_projection(
            &prepared.sources.iter().map(|source| source.key).collect(),
        );
        for _ in 0..hidden_packets {
            let resumed = received.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(resumed.source, first_source);
            assert!(!presenter.complete_bridge_delivery(resumed.delivery_id, true));
        }
        hidden_writer.join().unwrap();
        assert_eq!(
            writes.try_iter().collect::<Vec<_>>(),
            ((ROLLING_FLOW_RECORDS + 2)..=hidden_packets + 1).collect::<Vec<_>>()
        );

        // A foreground detach removes every projected source. It must use the same bounded pause
        // instead of returning credits in a discard loop until the audio worker reaches EOS.
        presenter.deactivate_bridge();
        let detached_first_id = hidden_packets + 2;
        let detached_packets = ROLLING_FLOW_RECORDS * 4;
        let (detached_written, detached_writes) = mpsc::channel();
        let detached_channel = channels[0].clone();
        let detached_writer = thread::spawn(move || {
            for packet_id in detached_first_id..detached_first_id + detached_packets {
                detached_channel
                    .send_audio(media::AudioPacket {
                        epoch: 1,
                        packet_id,
                        pts_us: i64::try_from(packet_id - 1).unwrap() * 20_000,
                        dts_us: i64::try_from(packet_id - 1).unwrap() * 20_000,
                        duration_us: 20_000,
                        trim_start_samples: 0,
                        trim_end_samples: 0,
                        data: &[0; 16],
                    })
                    .unwrap();
                detached_written.send(packet_id).unwrap();
            }
        });
        let mut detached_before_projection = Vec::new();
        while let Ok(packet_id) = detached_writes.recv_timeout(Duration::from_millis(100)) {
            detached_before_projection.push(packet_id);
        }
        assert!(
            detached_before_projection.len() < detached_packets as usize,
            "detached audio consumed the file instead of stopping at its bounded grant"
        );
        assert!(received.try_recv().is_err());

        let prepared = presenter
            .prepare_projection_snapshot_with_viewports(&HashSet::from([7, 8]), &HashMap::new());
        assert!(
            received.recv_timeout(Duration::from_millis(100)).is_err(),
            "reattach submission released audio before the new presenter applied its tracks"
        );
        presenter.activate_bridge_projection(
            &prepared.sources.iter().map(|source| source.key).collect(),
        );
        for _ in 0..detached_packets {
            let resumed = received.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(resumed.source, first_source);
            assert!(!presenter.complete_bridge_delivery(resumed.delivery_id, true));
        }
        detached_writer.join().unwrap();
        detached_before_projection.extend(detached_writes.try_iter());
        assert_eq!(
            detached_before_projection,
            (detached_first_id..detached_first_id + detached_packets).collect::<Vec<_>>()
        );
        first.close().unwrap();
        second.close().unwrap();
    }

    #[test]
    fn reapplied_video_holds_and_catches_up_linked_audio_to_its_recovery_pts() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(16);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let video = client
            .create_track(video(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let audio = client
            .create_track(audio(context, 9, 12), &RequestMetadata::default())
            .unwrap();
        let video_channel = Arc::new(client.open_track_channel(&video).unwrap());
        let audio_channel = Arc::new(client.open_track_channel(&audio).unwrap());
        let initial = presenter.projection_snapshot(&HashSet::from([7]));
        let video_source = initial
            .sources
            .iter()
            .find(|source| matches!(source.descriptor, SourceDescriptor::Video(_)))
            .unwrap()
            .key;
        let audio_source = initial
            .sources
            .iter()
            .find(|source| matches!(source.descriptor, SourceDescriptor::Audio(_)))
            .unwrap()
            .key;

        video_channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 40_000,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0x88],
            })
            .unwrap();
        let initial_video = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(initial_video.source, video_source);
        assert!(!presenter.complete_bridge_delivery(initial_video.delivery_id, true));
        audio_channel
            .send_audio(media::AudioPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 20_000,
                trim_start_samples: 0,
                trim_end_samples: 0,
                data: &[0; 16],
            })
            .unwrap();
        let initial_audio = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(initial_audio.source, audio_source);
        assert!(!presenter.complete_bridge_delivery(initial_audio.delivery_id, true));
        video_channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 2,
                pts_us: 40_000,
                dts_us: 40_000,
                duration_us: 40_000,
                key: false,
                data: &[0, 0, 0, 1, 0x41, 0x88],
            })
            .unwrap();
        let stale_before_hide = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(stale_before_hide.source, video_source);
        {
            let mut state = lock(&presenter.state);
            for track in state
                .tracks
                .iter_mut()
                .filter(|(key, _)| key.surface.session == client.info().session_id)
                .map(|(_, track)| track)
            {
                track.playing = true;
            }
        }

        presenter.projection_snapshot(&HashSet::new());
        assert!(
            !presenter.bridge_delivery_is_pending(
                stale_before_hide.delivery_id,
                stale_before_hide.source
            ),
            "a delivery admitted before the falling visibility edge survived the handoff"
        );
        let (video_returned, video_return) = mpsc::sync_channel(1);
        let blocked_video = video_channel.clone();
        let blocked_video_writer = thread::spawn(move || {
            blocked_video
                .send_video(media::VideoPacket {
                    epoch: 1,
                    packet_id: 3,
                    pts_us: 80_000,
                    dts_us: 80_000,
                    duration_us: 40_000,
                    key: true,
                    data: &[0, 0, 0, 1, 0x65, 0x99],
                })
                .unwrap();
            video_returned.send(()).unwrap();
        });
        let (audio_returned, audio_return) = mpsc::sync_channel(1);
        let blocked_audio = audio_channel.clone();
        let blocked_audio_writer = thread::spawn(move || {
            blocked_audio
                .send_audio(media::AudioPacket {
                    epoch: 1,
                    packet_id: 2,
                    pts_us: 20_000,
                    dts_us: 20_000,
                    duration_us: 20_000,
                    trim_start_samples: 0,
                    trim_end_samples: 0,
                    data: &[0; 16],
                })
                .unwrap();
            audio_returned.send(()).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let blocked = {
                let state = lock(&presenter.state);
                state
                    .tracks
                    .get(&inner_track_key(video_source))
                    .is_some_and(|track| track.projection_blocked)
                    && state
                        .tracks
                        .get(&inner_track_key(audio_source))
                        .is_some_and(|track| track.projection_blocked)
            };
            if blocked {
                break;
            }
            assert!(Instant::now() < deadline, "timed tracks never parked");
            thread::sleep(Duration::from_millis(2));
        }

        let prepared = presenter
            .prepare_projection_snapshot_with_viewports(&HashSet::from([7]), &HashMap::new());
        assert_eq!(prepared.videos_needing_keyframes, [video_source]);
        assert_eq!(
            presenter.request_keyframe(video_source, None, 5),
            KeyframeRequestOutcome::Forwarded
        );
        presenter.activate_bridge_projection(
            &prepared.sources.iter().map(|source| source.key).collect(),
        );
        video_return.recv_timeout(Duration::from_secs(2)).unwrap();
        {
            let state = lock(&presenter.state);
            let video = state.tracks.get(&inner_track_key(video_source)).unwrap();
            assert!(video.recovery_pending);
            assert!(video.gate_linked_audio_for_recovery);
        }
        assert!(
            received.try_recv().is_err(),
            "the video packet blocked before the recovery request reached the replacement decoder"
        );
        audio_return.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            received.try_recv().is_err(),
            "linked audio escaped while video recovery was still pending"
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match video_channel.take_event().unwrap() {
                Some(vivid_sdk::ChannelEvent::NeedKeyframe(_)) => break,
                Some(other) => panic!("unexpected video channel event: {other:?}"),
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(2)),
                None => panic!("recovery request never reached the video producer"),
            }
        }

        let recovery_pts_us = 8_000_000;
        video_channel
            .send_video(media::VideoPacket {
                epoch: 2,
                packet_id: 4,
                pts_us: recovery_pts_us,
                dts_us: recovery_pts_us,
                duration_us: 40_000,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0xaa],
            })
            .unwrap();
        let recovered = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(recovered.source, video_source);
        assert_eq!(recovered.recovered_keyframe, Some((2, recovery_pts_us)));

        video_channel
            .send_video(media::VideoPacket {
                epoch: 2,
                packet_id: 5,
                pts_us: recovery_pts_us + 40_000,
                dts_us: recovery_pts_us + 40_000,
                duration_us: 40_000,
                key: false,
                data: &[0, 0, 0, 1, 0x41, 0xbb],
            })
            .unwrap();
        let recovery_follower = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(recovery_follower.source, video_source);
        assert_eq!(recovery_follower.recovered_keyframe, None);
        assert_eq!(
            media::parse_video_packet(&recovery_follower.body)
                .unwrap()
                .pts_us,
            recovery_pts_us + 40_000,
            "interframes following an admitted recovery keyframe must remain contiguous while its outer delivery is in flight"
        );

        assert!(!presenter.complete_bridge_delivery(recovered.delivery_id, true));
        assert!(!presenter.complete_bridge_delivery(recovery_follower.delivery_id, true));

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let consumed = lock(&presenter.state)
                .tracks
                .get(&inner_track_key(audio_source))
                .is_some_and(|track| track.state.last_media_id >= 2);
            if consumed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "linked audio did not resume after video recovery"
            );
            thread::sleep(Duration::from_millis(2));
        }
        assert!(received.try_recv().is_err());
        audio_channel
            .send_audio(media::AudioPacket {
                epoch: 1,
                packet_id: 3,
                pts_us: recovery_pts_us - 20_000,
                dts_us: recovery_pts_us - 20_000,
                duration_us: 20_000,
                trim_start_samples: 0,
                trim_end_samples: 0,
                data: &[0; 16],
            })
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let consumed = lock(&presenter.state)
                .tracks
                .get(&inner_track_key(audio_source))
                .is_some_and(|track| track.state.last_media_id >= 3);
            if consumed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "linked audio did not catch up toward the video recovery PTS"
            );
            thread::sleep(Duration::from_millis(2));
        }
        assert!(
            received.try_recv().is_err(),
            "audio below the accepted video recovery PTS reached the outer presenter"
        );
        audio_channel
            .send_audio(media::AudioPacket {
                epoch: 1,
                packet_id: 4,
                pts_us: recovery_pts_us + 1_000,
                dts_us: recovery_pts_us + 1_000,
                duration_us: 20_000,
                trim_start_samples: 0,
                trim_end_samples: 0,
                data: &[0; 16],
            })
            .unwrap();
        let caught_up = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(caught_up.source, audio_source);
        assert!(!presenter.complete_bridge_delivery(caught_up.delivery_id, true));

        blocked_video_writer.join().unwrap();
        blocked_audio_writer.join().unwrap();
        client.close().unwrap();
    }

    #[test]
    fn timed_packet_ingest_does_not_advance_the_scene_projection() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let video = client
            .create_track(video(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let audio = client
            .create_track(audio(context, 9, 12), &RequestMetadata::default())
            .unwrap();
        let video_channel = client.open_track_channel(&video).unwrap();
        let audio_channel = client.open_track_channel(&audio).unwrap();
        presenter.projection_snapshot(&HashSet::from([7]));
        let projection_before_packets = presenter.revision();

        video_channel
            .send_video(media::VideoPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 41_667,
                key: true,
                data: &[0, 0, 0, 1, 0x65, 0x88],
            })
            .unwrap();
        audio_channel
            .send_audio(media::AudioPacket {
                epoch: 1,
                packet_id: 1,
                pts_us: 0,
                dts_us: 0,
                duration_us: 21_333,
                trim_start_samples: 0,
                trim_end_samples: 0,
                data: &[0; 16],
            })
            .unwrap();
        let first = received.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = received.recv_timeout(Duration::from_secs(2)).unwrap();

        assert_eq!(
            presenter.revision(),
            projection_before_packets,
            "ordinary timed packets must not generate no-op scene snapshots"
        );
        presenter.complete_bridge_delivery(first.delivery_id, true);
        presenter.complete_bridge_delivery(second.delivery_id, true);
        client.close().unwrap();
    }

    /// A freshly accepted channel carries one record of allowance, so a producer can only write
    /// its next frame once the previous delivery has returned what it held. An outer resize fails
    /// deliveries in bulk - it rebuilds the outer session under whatever is in flight - and if a
    /// failed one kept its allowance the producer would run out and block in a credit wait with
    /// no event able to release it: the pane would freeze mid-frame and stop reading its input.
    #[test]
    fn a_delivery_the_bridge_failed_returns_the_allowance_it_held() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let track = client
            .create_track(raster(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let channel = client.open_track_channel(&track).unwrap();

        let frames = 4;
        let (written, writes) = mpsc::sync_channel(0);
        let writer = thread::spawn(move || {
            for frame in 1..=frames {
                if channel
                    .send_raster(0, frame, &[0, 0, 0, 255].repeat(4), false)
                    .is_err()
                    || written.send(frame).is_err()
                {
                    return;
                }
            }
        });

        for frame in 1..=frames {
            assert_eq!(
                writes.recv_timeout(Duration::from_secs(10)).ok(),
                Some(frame),
                "frame {frame} never left the producer, so a failed delivery kept its allowance"
            );
            let event = received.recv_timeout(Duration::from_secs(10)).unwrap();
            presenter.complete_bridge_delivery(event.delivery_id, false);
        }
        writer.join().unwrap();
        client.close().unwrap();
    }

    #[test]
    fn clean_goodbye_retains_only_anchored_static_content() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let session = client.info().session_id;
        let context = client.info().root_context_id;
        let surface = client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let track = client
            .create_track(raster(context, 9, 11), &RequestMetadata::default())
            .unwrap();
        let channel = client.open_track_channel(&track).unwrap();
        channel
            .send_raster(0, 1, &[0, 0, 0, 255].repeat(4), false)
            .unwrap();
        let event = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!presenter.complete_bridge_delivery(event.delivery_id, true));

        let marker = client.anchor_marker(context, 13).unwrap();
        presenter.observe_marker(7, &marker[2..marker.len() - 2], 2, 3);
        client
            .create_node(
                &SceneNode {
                    owning_context_id: context,
                    node_id: 17,
                    surface_context_id: surface.context_id(),
                    surface_id: surface.id(),
                    geometry: vec![
                        (0, Value::Unsigned(2)),
                        (1, Value::Unsigned(0)),
                        (2, Value::Unsigned(0)),
                        (3, Value::Unsigned(2_u64 << 32)),
                        (4, Value::Unsigned(2_u64 << 32)),
                        (5, Value::Unsigned(1)),
                        (6, Value::Unsigned(context)),
                        (7, Value::Unsigned(13)),
                    ],
                    fit: Fit::Contain,
                    linear_sampling: true,
                    z_index: 0,
                    visible: true,
                    opacity: u16::MAX,
                    clip: None,
                },
                &RequestMetadata::default(),
            )
            .unwrap();
        client.close().unwrap();

        let snapshot = presenter.projection_snapshot(&HashSet::from([7]));
        assert_eq!(snapshot.surfaces.len(), 1);
        assert_eq!(snapshot.sources.len(), 1);
        assert_eq!(snapshot.nodes.len(), 1);
        assert_eq!(snapshot.sources[0].key.producer, session);
        assert!(snapshot.sources[0].retained_raster.is_some());
        assert_eq!(snapshot.nodes[0].config.node.anchor_id, Some(13));
    }

    #[test]
    fn retained_raster_composes_vvpaint_style_deltas_into_latest_canvas() {
        let directory = tempfile::tempdir().unwrap();
        let (events, received) = mpsc::sync_channel(4);
        let presenter = VirtualVivid::start_with_events(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
            Some(events),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();
        let context = client.info().root_context_id;
        client
            .create_surface(surface(context, 9), &RequestMetadata::default())
            .unwrap();
        let mut configuration = raster(context, 9, 11);
        let KindConfiguration::Raster(raster) = &mut configuration.kind else {
            unreachable!()
        };
        raster.delta_enabled = true;
        raster.maximum_delta_operations = 4;
        let track = client
            .create_track(configuration, &RequestMetadata::default())
            .unwrap();
        let channel = client.open_track_channel(&track).unwrap();

        let blank = [0xff_u8; 16];
        channel.send_raster(1, 1, &blank, false).unwrap();
        let full = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!presenter.complete_bridge_delivery(full.delivery_id, true));

        let stroke = [0x10, 0x20, 0x30, 0xff];
        channel
            .send_raster_delta(
                1,
                2,
                1,
                16_000,
                16_000,
                &[media::RasterDeltaOperation::Overwrite {
                    x: 1,
                    y: 0,
                    width: 1,
                    height: 1,
                    rgba: &stroke,
                }],
                false,
            )
            .unwrap();
        let delta = received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!presenter.complete_bridge_delivery(delta.delivery_id, true));

        let snapshot = presenter.projection_snapshot(&HashSet::from([7]));
        let retained = snapshot.sources[0]
            .retained_raster
            .as_ref()
            .expect("delta-capable raster did not retain a composed canvas");
        assert_eq!(retained.epoch, 1);
        assert_eq!(retained.frame_id, 2);
        let mut expected = blank;
        expected[4..8].copy_from_slice(&stroke);
        assert_eq!(&*retained.pixels, expected);
        assert!(snapshot.sources[0].retained.is_none());

        client.close().unwrap();
    }

    #[test]
    fn reused_numeric_identities_are_isolated_by_session_and_pane() {
        let directory = tempfile::tempdir().unwrap();
        let presenter = VirtualVivid::start(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        presenter.update_metrics(8, 80, 24, (8, 16));
        let first_secret = presenter.issue_pane_capability(7).unwrap();
        let second_secret = presenter.issue_pane_capability(8).unwrap();
        let mut first =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &first_secret)).unwrap();
        let mut second =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &second_secret)).unwrap();
        for client in [&mut first, &mut second] {
            let context = client.info().root_context_id;
            client
                .create_surface(surface(context, 9), &RequestMetadata::default())
                .unwrap();
            client
                .create_track(raster(context, 9, 11), &RequestMetadata::default())
                .unwrap();
        }
        presenter.revoke_pane(7);
        let snapshot = presenter.projection_snapshot(&HashSet::from([8]));
        assert_eq!(snapshot.sources.len(), 1);
        assert_eq!(snapshot.sources[0].key.producer, second.info().session_id);
        assert_eq!(
            snapshot.sources[0].key.context,
            second.info().root_context_id
        );
        assert_eq!(snapshot.sources[0].key.surface, 9);
        assert_eq!(snapshot.sources[0].key.track, 11);
        second.close().unwrap();
    }

    #[test]
    fn reused_numeric_identities_are_isolated_across_exact_lease_revocation() {
        let directory = tempfile::tempdir().unwrap();
        let presenter = VirtualVivid::start(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
        )
        .unwrap();
        let profiles = vec![
            vivid_sdk::CORE_CONTROL.into(),
            vivid_sdk::LIVE_MEDIA.into(),
            vivid_sdk::OBSERVABILITY.into(),
            vivid_sdk::TERMINAL_SURFACE.into(),
            vivid_sdk::TIMED_MEDIA.into(),
        ];
        let contract =
            ResourceContract::new([u64::MAX / 4; vivid_protocol::resource::RESOURCE_COUNT]);
        let mut clients = Vec::new();
        let mut retry_secret = None;
        for context in [41_u64, 42] {
            presenter.update_metrics(context, 80, 24, (8, 16));
            let (definition, mut activation) = SessionLeaseBuilder::new(context, 9)
                .permitted_profiles(profiles.clone())
                .contract(contract.clone())
                .build()
                .unwrap();
            presenter.issue_lease(context, definition).unwrap();
            let secret = activation.take().unwrap();
            if context == 41 {
                retry_secret = Some(Secret32::new(*secret.expose()));
            }
            let mut config = producer(presenter.endpoint(), &"11".repeat(32));
            config.authentication =
                ProducerAuthentication::lease_activation_bytes(context, 9, secret).unwrap();
            clients.push(vivid_sdk::Session::connect(config).unwrap());
        }

        let mut retry = producer(presenter.endpoint(), &"11".repeat(32));
        retry.authentication = ProducerAuthentication::lease_activation_bytes(
            41,
            9,
            retry_secret.expect("first secret was retained only for a replay check"),
        )
        .unwrap();
        assert!(
            vivid_sdk::Session::connect(retry).is_err(),
            "a lease activation secret is one-use"
        );

        for (pane, client) in [41_u64, 42].into_iter().zip(clients.iter_mut()) {
            let context = client.info().root_context_id;
            let surface = client
                .create_surface(surface(context, 9), &RequestMetadata::default())
                .unwrap();
            client
                .create_track(raster(context, 9, 11), &RequestMetadata::default())
                .unwrap();
            let marker = client.anchor_marker(context, 13).unwrap();
            presenter.observe_marker(pane, &marker[2..marker.len() - 2], 2, 3);
            client
                .create_node(
                    &SceneNode {
                        owning_context_id: context,
                        node_id: 17,
                        surface_context_id: surface.context_id(),
                        surface_id: surface.id(),
                        geometry: vec![
                            (0, Value::Unsigned(2)),
                            (1, Value::Unsigned(0)),
                            (2, Value::Unsigned(0)),
                            (3, Value::Unsigned(2_u64 << 32)),
                            (4, Value::Unsigned(2_u64 << 32)),
                            (5, Value::Unsigned(1)),
                            (6, Value::Unsigned(context)),
                            (7, Value::Unsigned(13)),
                        ],
                        fit: Fit::Contain,
                        linear_sampling: true,
                        z_index: 0,
                        visible: true,
                        opacity: u16::MAX,
                        clip: None,
                    },
                    &RequestMetadata::default(),
                )
                .unwrap();
        }

        let before = presenter.projection_snapshot(&HashSet::from([41, 42]));
        assert_eq!(before.sources.len(), 2);
        assert_eq!(before.nodes.len(), 2);
        presenter.revoke_lease(41, 9).unwrap();
        let after = presenter.projection_snapshot(&HashSet::from([41, 42]));
        assert_eq!(after.sources.len(), 1);
        assert_eq!(after.nodes.len(), 1);
        assert_eq!(after.sources[0].key.producer, clients[1].info().session_id);
        assert_eq!(after.sources[0].key.surface, 9);
        assert_eq!(after.sources[0].key.track, 11);
        assert_eq!(after.nodes[0].config.node.node_id, 17);

        let survivor_context = clients[1].info().root_context_id;
        clients[1]
            .create_surface(surface(survivor_context, 10), &RequestMetadata::default())
            .unwrap();
        clients[1]
            .create_track(
                raster(survivor_context, 10, 12),
                &RequestMetadata::default(),
            )
            .unwrap();
        let final_snapshot = presenter.projection_snapshot(&HashSet::from([42]));
        assert_eq!(final_snapshot.sources.len(), 2);
        assert!(
            final_snapshot
                .sources
                .iter()
                .all(|source| { source.key.producer == clients[1].info().session_id })
        );
        clients.swap_remove(1).close().unwrap();
    }

    #[test]
    fn pane_resize_emits_an_sdk_compatible_flat_target_change() {
        let directory = tempfile::tempdir().unwrap();
        let presenter = VirtualVivid::start(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let mut client =
            vivid_sdk::Session::connect(producer(presenter.endpoint(), &secret)).unwrap();

        presenter.update_metrics(7, 100, 40, (9, 18));
        let deadline = Instant::now() + Duration::from_secs(1);
        let payload = loop {
            if let Some(vivid_sdk::SessionEvent::TargetChanged(payload)) =
                client.take_event().unwrap()
            {
                break payload;
            }
            assert!(
                Instant::now() < deadline,
                "virtual presenter did not deliver TARGET_CHANGED"
            );
            thread::sleep(Duration::from_millis(1));
        };
        assert_eq!(
            payload.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            (0..=10).collect::<Vec<_>>(),
            "the target descriptor must be flat in the event payload"
        );
        assert_eq!(
            client.apply_target_changed(&payload).unwrap(),
            TargetGeneration::new(2)
        );
        assert_eq!(client.info().target_descriptor[3].1.as_u64(), Some(40));
        client.close().unwrap();
    }

    #[test]
    fn wrong_and_revoked_pane_secrets_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let presenter = VirtualVivid::start(
            TestSocketListener::bind(directory.path().join("vivid.sock")).unwrap(),
            MediaConfig::default(),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let revoked = presenter.issue_pane_capability(7).unwrap();
        presenter.revoke_pane(7);
        assert!(vivid_sdk::Session::connect(producer(presenter.endpoint(), &revoked)).is_err());

        presenter.update_metrics(7, 80, 24, (8, 16));
        let valid = presenter.issue_pane_capability(7).unwrap();
        let mut wrong = valid.into_bytes();
        wrong[0] = if wrong[0] == b'0' { b'1' } else { b'0' };
        let wrong = String::from_utf8(wrong).unwrap();
        assert!(vivid_sdk::Session::connect(producer(presenter.endpoint(), &wrong)).is_err());
    }
}
