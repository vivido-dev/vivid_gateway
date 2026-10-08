use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Instant;
use vivid_protocol::auth::Secret32;
use vivid_protocol::cbor::Value;
use vivid_protocol::surface::POLICY_KNOWN_MASK;
use vivid_protocol::track::{AudioGain, TrackMode};
use vivid_sdk::presenter::{
    BridgeKeyframeRequest, BridgeNode, BridgePlayRequest, BridgeSource, BridgeSourceKey,
    BridgeSourceKind, BridgeSurface, BridgeSurfaceKey, DisplayMetrics,
};
use vivid_sdk::{
    ConnectionFactory, RequestMetadata, SceneNode, SessionEvent, Surface, Track, TrackChannel,
};

mod builder;
mod config;
mod overlay;
mod playback;
mod relay;
mod scene;

pub use builder::OuterBridgeBuilder;
pub use playback::{ClockState, EosState, PlaybackSnapshot};
pub use relay::{DeliveryOutcome, MediaChunk};

use config::{display_from_target, producer_config};
use overlay::{OverlayLayouts, OverlayRevisions, OverlayWindowState};
use playback::{
    PositionJob, PositionObserver, RetiringSurface, hold_source, preferred_surface_clock,
};
use relay::{MediaCompletion, OuterMediaCommand, PendingBody};

/// Bounded linked pre-roll forwarded before outer PLAY.
///
/// One H.264 access unit may not produce output until reordered frames arrive. Keep the bounded
/// packet budget above the advertised reorder depth. Readiness is observed independently;
/// waiting for a remote credit/query round trip per access unit multiplies decoder startup delay.
const OUTER_TIMED_PREROLL_RECORDS: usize = 32;

/// PLAY start policy that holds the clock until every active video track has its target picture
/// (media specification §15.2, `timed-media-sync-v1`).
const START_POLICY_SYNCHRONIZED: u64 = 2;

/// Control-side state of one outer track the bridge created for an inner source.
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent protocol milestones observed at different times, not one state machine"
)]
struct OuterTrack {
    writer_id: u64,
    surface_key: BridgeSurfaceKey,
    track: Track,
    channel: Arc<TrackChannel>,
    media_sender: mpsc::SyncSender<OuterMediaCommand>,
    slot_activated: Arc<AtomicBool>,
    kind: BridgeSourceKind,
    decoder_reset_serial: u64,
    mode: TrackMode,
    /// Control-side mirror of [`OuterTrack::slot_activated`], read without atomic ordering.
    activated: bool,
    /// Current-generation readiness observed independently of media forwarding.
    output_ready: bool,
    media_inflight: usize,
    media_submitted: usize,
    media_completed: usize,
    preplay_ceiling: usize,
    /// Submission count at the last PAUSE, cleared by the following PLAY.
    ///
    /// An activated audio slot can retain decoded samples while paused, but restarting it before
    /// the nested producer has resumed its feed lets that tail drain into an underrun. Waiting for
    /// one new submission proves the feed is moving again without waiting for presenter capacity,
    /// which can itself depend on PLAY when the paused audio ring is full.
    resume_after_submission: Option<usize>,
    /// The surface position this outer track was last told, or `None` while it has never been
    /// given one.
    ///
    /// A replacement track created for a seek starts here, which is what distinguishes "the
    /// producer has not positioned this surface" from "the position it holds was never relayed".
    published_play_request: Option<BridgePlayRequest>,
    playing: bool,
    eos_requested: bool,
    eos: bool,
    reported_eos_state: EosState,
    target_picture_ready: bool,
}

/// How a bridge reaches its outer presenter, kept so a replacement session goes the same way.
enum Route {
    /// Native endpoints; a missing lane falls back as the protocol prescribes.
    Native {
        control: String,
        realtime: Option<String>,
        bulk: Option<String>,
    },
    /// Connections a product-supplied factory opens.
    Factory(Arc<dyn ConnectionFactory>),
}

impl Route {
    /// Connects one new outer producer session along this route.
    fn connect(
        &self,
        authentication: &Secret32,
        target_profile: &str,
    ) -> io::Result<vivid_sdk::Session> {
        match self {
            Self::Native {
                control,
                realtime,
                bulk,
            } => vivid_sdk::Session::connect(producer_config(
                Some(control.clone()),
                realtime.clone(),
                bulk.clone(),
                authentication,
                target_profile,
            )?),
            Self::Factory(factory) => vivid_sdk::Session::connect_with_factory(
                producer_config(None, None, None, authentication, target_profile)?,
                Arc::clone(factory),
            ),
        }
    }

    fn factory(&self) -> Option<Arc<dyn ConnectionFactory>> {
        match self {
            Self::Native { .. } => None,
            Self::Factory(factory) => Some(Arc::clone(factory)),
        }
    }
}

/// Cancels a bridge from another thread, including any session that later replaces its current one.
///
/// Obtained from [`OuterBridge::cancel_handle`]. Cancellation is sticky: a replacement session
/// installed after [`cancel`](Self::cancel) is cancelled as soon as it is installed.
#[derive(Clone)]
pub struct BridgeCancel {
    cancelled: Arc<AtomicBool>,
    current: Arc<std::sync::Mutex<Arc<dyn Fn() + Send + Sync>>>,
    factory: Option<Arc<dyn ConnectionFactory>>,
}

impl std::fmt::Debug for BridgeCancel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeCancel")
            .field("cancelled", &self.cancelled.load(Ordering::Acquire))
            .field("factory", &self.factory.is_some())
            .finish_non_exhaustive()
    }
}

impl BridgeCancel {
    /// Cancels the bridge's current outer session, its connection factory, and any later one.
    ///
    /// Blocking calls on the bridge's thread return promptly with an error once this runs.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(factory) = &self.factory {
            factory.cancel();
        }
        let cancel = self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        cancel();
    }
    fn install(&self, session: &vivid_sdk::Session) -> io::Result<()> {
        *self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = session.cancel_handle();
        if self.cancelled.load(Ordering::Acquire) {
            self.cancel();
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "bridge cancelled",
            ));
        }
        Ok(())
    }
}

/// Vivid 1.5 producer side of the nested presenter.
///
/// Every object allocated here belongs exclusively to the outer session. Inner IDs are lookup
/// keys only and never become outer Vivid IDs, revisions, generations, epochs, or media IDs.
pub struct OuterBridge {
    overlay_jobs: Vec<(u64, thread::JoinHandle<io::Result<Vec<u8>>>)>,
    overlay_results: Vec<(u64, io::Result<Vec<u8>>)>,
    overlay_input: Option<Arc<vivid_sdk::OverlayInputLane>>,
    overlay_environment: Option<Vec<u8>>,
    overlay_renewed: Instant,
    overlay_revisions: OverlayRevisions,
    overlay_layouts: Arc<Mutex<OverlayLayouts>>,
    cancellation: BridgeCancel,
    microphones: crate::microphone::Microphones,
    session: vivid_sdk::Session,
    authentication: Secret32,
    route: Route,
    target_profile: String,
    enforced_surface_policy: u64,
    display: DisplayMetrics,
    surfaces: HashMap<BridgeSurfaceKey, Surface>,
    /// One outer `SET_OVERLAY_WINDOW` handshake state per surface that hosts an overlay window.
    overlay_windows: HashMap<BridgeSurfaceKey, OverlayWindowState>,
    tracks: HashMap<BridgeSourceKey, OuterTrack>,
    unfinished_tracks: HashMap<BridgeSourceKey, Track>,
    surface_refresh: HashSet<BridgeSurfaceKey>,
    active_sources: HashMap<BridgeSourceKey, BridgeSource>,
    nodes: HashMap<(u64, u64, u8), (u64, SceneNode)>,
    desired_nodes: Vec<BridgeNode>,
    desired_surfaces: Vec<BridgeSurface>,
    pending: HashMap<BridgeSourceKey, PendingBody>,
    completions: Vec<MediaCompletion>,
    writer_completions_tx: mpsc::Sender<MediaCompletion>,
    writer_completions_rx: mpsc::Receiver<MediaCompletion>,
    next_writer_id: u64,
    keyframes: Vec<BridgeKeyframeRequest>,
    full_frames: HashSet<BridgeSourceKey>,
    losses: HashSet<BridgeSourceKey>,
    playback: Vec<(BridgeSourceKey, PlaybackSnapshot)>,
    positions: Vec<(
        BridgeSourceKey,
        vivid_sdk::presenter::BridgePositionSnapshot,
    )>,
    position_poll_at: Instant,
    position_cursor: usize,
    position_observer: Option<PositionObserver>,
    retiring_surfaces: HashMap<BridgeSurfaceKey, RetiringSurface>,
    hold_updates: HashMap<BridgeSourceKey, vivid_sdk::presenter::BridgeHoldSnapshot>,
    hold_serials: HashMap<BridgeSurfaceKey, u64>,
    incompatible_sources: HashMap<BridgeSourceKey, u64>,
    source_errors: Vec<(BridgeSourceKey, u64)>,
    /// The last position this bridge published for each outer surface clock.
    ///
    /// Its presence is the evidence that the nested producer has positioned that surface at all,
    /// which a track's own `play_request` cannot say: an unplayed track carries the inner
    /// presenter's baseline request, not a producer position.
    surface_clock: HashMap<BridgeSurfaceKey, BridgePlayRequest>,
    outer_applied_revision: u64,
    diagnostic_generation: u64,
    terminal_error: Option<String>,
    display_changed: bool,
}

impl OuterBridge {
    /// Returns a handle that cancels this bridge from another thread.
    pub fn cancel_handle(&self) -> BridgeCancel {
        self.cancellation.clone()
    }

    /// Reconciles the outer microphone routes with the inner microphone `requests`.
    ///
    /// Each request gets its own outer surface and uplink audio track. A request whose generation
    /// changed keeps its outer track but advances its channel, so a new generation never inherits the
    /// old one's capture consent. A route that fails during setup is destroyed on the next call.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::Unsupported`] when `requests` is not empty and the outer presenter did
    /// not accept `audio-input-v1`, and an error when there are more than 64 requests. Failures to
    /// create, advance or destroy outer objects are returned as the outer session reports them.
    pub fn sync_microphones(
        &mut self,
        requests: &[vivid_sdk::presenter::MicrophoneRequest],
    ) -> io::Result<()> {
        self.microphones.sync(&mut self.session, requests)
    }

    /// Takes the microphone packets the outer presenter captured since the last call.
    ///
    /// A packet whose `packet` is empty marks a route that ended; it sends nothing more.
    ///
    /// # Errors
    ///
    /// Returns an error when a captured packet cannot be encoded or its route's capture grant cannot
    /// be renewed.
    pub fn take_microphone_packets(&mut self) -> io::Result<Vec<crate::MicrophonePacket>> {
        self.microphones.take()
    }

    /// Starts a bridge over an already connected outer `session`.
    fn from_session(
        session: vivid_sdk::Session,
        authentication: Secret32,
        route: Route,
        fallback_display: DisplayMetrics,
    ) -> io::Result<Self> {
        let display = display_from_target(&session, fallback_display)?;
        let target_profile = session.info().target_profile.clone();
        let (writer_completions_tx, writer_completions_rx) = mpsc::channel();
        let cancellation = BridgeCancel {
            cancelled: Arc::new(AtomicBool::new(false)),
            current: Arc::new(std::sync::Mutex::new(session.cancel_handle())),
            factory: route.factory(),
        };
        let position_observer = PositionObserver::new(&session)?;
        Ok(Self {
            cancellation,
            session,
            microphones: crate::microphone::Microphones::default(),
            authentication,
            route,
            target_profile,
            enforced_surface_policy: 0,
            display,
            surfaces: HashMap::new(),
            overlay_windows: HashMap::new(),
            overlay_input: None,
            overlay_jobs: Vec::new(),
            overlay_results: Vec::new(),
            overlay_environment: None,
            overlay_renewed: Instant::now(),
            overlay_layouts: Arc::default(),
            overlay_revisions: Arc::default(),
            tracks: HashMap::new(),
            unfinished_tracks: HashMap::new(),
            surface_refresh: HashSet::new(),
            active_sources: HashMap::new(),
            nodes: HashMap::new(),
            desired_nodes: Vec::new(),
            desired_surfaces: Vec::new(),
            pending: HashMap::new(),
            completions: Vec::new(),
            writer_completions_tx,
            writer_completions_rx,
            next_writer_id: 0,
            keyframes: Vec::new(),
            full_frames: HashSet::new(),
            losses: HashSet::new(),
            playback: Vec::new(),
            positions: Vec::new(),
            position_poll_at: Instant::now(),
            position_cursor: 0,
            position_observer,
            retiring_surfaces: HashMap::new(),
            hold_updates: HashMap::new(),
            hold_serials: HashMap::new(),
            incompatible_sources: HashMap::new(),
            source_errors: Vec::new(),
            surface_clock: HashMap::new(),
            outer_applied_revision: 0,
            diagnostic_generation: 1,
            terminal_error: None,
            display_changed: false,
        })
    }

    /// The terminal geometry of the outer target.
    ///
    /// This follows `TARGET_CHANGED` as [`service_session_events`](Self::service_session_events)
    /// applies it, and is the builder's fallback for a desktop target.
    pub fn display_metrics(&self) -> DisplayMetrics {
        self.display
    }

    /// The outer target profile this bridge's session negotiated.
    pub fn target_profile(&self) -> &str {
        &self.target_profile
    }

    /// Adds route-level capture restrictions to every re-originated surface.
    ///
    /// Capture policy bits are restrictions, so they are combined with each inner surface's own
    /// policy as a union that can never relax it. Surfaces pick the policy up on the next
    /// reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] when `policy` has a bit the registry has not assigned.
    pub fn set_enforced_surface_policy(&mut self, policy: u64) -> io::Result<()> {
        if policy & !POLICY_KNOWN_MASK != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "gateway surface policy contains unassigned bits",
            ));
        }
        self.enforced_surface_policy = policy;
        Ok(())
    }

    /// The outer surface ID that re-originates inner surface `key`, if it exists.
    pub fn outer_surface_id(&self, key: BridgeSurfaceKey) -> Option<u64> {
        self.surfaces.get(&key).map(Surface::id)
    }

    /// The outer track ID that re-originates inner source `key`, if it exists.
    pub fn outer_track_id(&self, key: BridgeSourceKey) -> Option<u64> {
        self.tracks.get(&key).map(|track| track.track.id())
    }

    /// Counts one applied inner projection and returns the new count.
    pub fn mark_projection_applied(&mut self) -> u64 {
        self.outer_applied_revision = self.outer_applied_revision.saturating_add(1);
        self.outer_applied_revision
    }

    /// Identifies the current outer session in diagnostics.
    ///
    /// It starts at one and increases with every [`replace_session`](Self::replace_session).
    pub fn diagnostic_instance_generation(&self) -> u64 {
        self.diagnostic_generation
    }

    /// The outer channel generation of every relayed track, ordered by inner identity.
    pub fn attachment_generations(&self) -> Vec<(BridgeSourceKey, u64)> {
        let mut values = self
            .tracks
            .iter()
            .map(|(key, track)| (*key, track.track.channel_generation().get()))
            .collect::<Vec<_>>();
        values.sort_by_key(|(key, _)| (key.producer, key.context, key.surface, key.track));
        values
    }

    /// Adopt the decoder-reset serial published in response to this outer track's own recovery
    /// request without replacing that track.
    ///
    /// The physical decoder has already reset when it emits `NEED_KEYFRAME`. Recreating it again
    /// when the nested producer answers with `ADVANCE_CHANNEL`/`FLUSH` closes media already queued
    /// to the recovering track and turns an in-place recovery into a source-loss loop. Callers must
    /// use this only for a reset correlated with a request read from the same live outer track.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotFound`] when `key` has no outer track, and
    /// [`io::ErrorKind::InvalidData`] when `decoder_reset_serial` is older than the track's.
    pub fn acknowledge_outer_requested_decoder_reset(
        &mut self,
        key: BridgeSourceKey,
        decoder_reset_serial: u64,
    ) -> io::Result<()> {
        let track = self
            .tracks
            .get_mut(&key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "outer track is missing"))?;
        if decoder_reset_serial < track.decoder_reset_serial {
            return Err(invalid_data("outer decoder reset serial moved backward"));
        }
        track.decoder_reset_serial = decoder_reset_serial;
        track.output_ready = false;
        if let Some(source) = self.active_sources.get_mut(&key) {
            source.decoder_reset_serial = decoder_reset_serial;
        }
        Ok(())
    }

    /// Reconciles the outer session with one inner projection snapshot.
    ///
    /// Creates, updates and destroys outer surfaces, tracks and scene nodes until they mirror
    /// `surfaces`, `sources` and `nodes`, then applies playback changes. Returns the sources whose outer
    /// track was created by this call; their producers must send a fresh keyframe or full frame.
    ///
    /// Setup that fails part-way keeps what it needs to clean up, and the next call retries it.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] for an inconsistent snapshot: duplicate or zero
    /// identities, a track or node naming a missing surface, or invalid geometry or audio gain.
    /// Returns [`io::ErrorKind::Unsupported`] when the outer presenter refuses a relayed track
    /// configuration and [`io::ErrorKind::WouldBlock`] when a scene commit outlives a moving outer
    /// target. Other outer session failures are returned as the session reports them.
    pub fn rebuild(
        &mut self,
        surfaces: &[BridgeSurface],
        sources: &[BridgeSource],
        nodes: &[BridgeNode],
    ) -> io::Result<HashSet<BridgeSourceKey>> {
        self.rebuild_resetting(surfaces, sources, nodes, &HashSet::new())
    }

    /// Reconcile one projection while replacing timed decoders on newly recovering surfaces.
    ///
    /// PAUSE deliberately preserves buffered media, which is wrong for a seek: a saturated old
    /// outer decoder cannot return flow while paused, so the replacement keyframe never enters.
    /// Replacing only the timed tracks on the affected owner-qualified surfaces discards that
    /// stale buffer and grants fresh, source-scoped ingress without disturbing other panes.
    ///
    /// # Errors
    ///
    /// The errors of [`rebuild`](Self::rebuild).
    pub fn rebuild_resetting(
        &mut self,
        surfaces: &[BridgeSurface],
        sources: &[BridgeSource],
        nodes: &[BridgeNode],
        recovering: &HashSet<BridgeSourceKey>,
    ) -> io::Result<HashSet<BridgeSourceKey>> {
        validate_snapshot(surfaces, sources, nodes)?;
        self.desired_surfaces = surfaces.to_vec();
        let unfinished = self.unfinished_tracks.keys().copied().collect::<Vec<_>>();
        for key in unfinished {
            let track = &self.unfinished_tracks[&key];
            self.session
                .destroy_track(track, &RequestMetadata::default())?;
            self.unfinished_tracks.remove(&key);
        }
        for key in self.surface_refresh.iter().copied().collect::<Vec<_>>() {
            if let Some(surface) = self.surfaces.get(&key) {
                self.session.query_surface(surface)?;
            }
            self.surface_refresh.remove(&key);
        }
        self.reconcile_surfaces(surfaces)?;
        let current = sources
            .iter()
            .map(|source| source.key)
            .collect::<HashSet<_>>();
        let recovering_surfaces = recovering
            .iter()
            .map(|source| BridgeSurfaceKey {
                producer: source.producer,
                context: source.context,
                surface: source.surface,
            })
            .collect::<HashSet<_>>();
        let removed = self
            .tracks
            .iter()
            .filter_map(|(key, track)| {
                (!current.contains(key)
                    || (recovering_surfaces.contains(&track.surface_key)
                        && matches!(
                            track.kind,
                            BridgeSourceKind::Video { .. } | BridgeSourceKind::Audio { .. }
                        )))
                .then_some(*key)
            })
            .collect::<Vec<_>>();
        let mut frozen_surfaces = HashSet::new();
        for key in &removed {
            let track = &self.tracks[key];
            if !current.contains(key) && track.mode == TrackMode::Timed && track.activated {
                frozen_surfaces.insert(track.surface_key);
            }
        }
        for surface in frozen_surfaces {
            let Some(fallback) = removed
                .iter()
                .find(|key| {
                    self.tracks
                        .get(key)
                        .is_some_and(|track| track.surface_key == surface)
                })
                .copied()
            else {
                continue;
            };
            let clock = preferred_surface_clock(&self.active_sources, surface, fallback);
            let Some(track) = self.tracks.get(&clock) else {
                continue;
            };
            let Some(source) = self.active_sources.get(&clock) else {
                continue;
            };
            let clock = PositionJob {
                key: clock,
                writer_id: track.writer_id,
                track: track.track.clone(),
                decoder_reset_serial: source.decoder_reset_serial,
                playing: source.playing,
                start_pts_us: source.play_request.start_pts_us,
            };
            // Leave the old outer slots alive only until PAUSE + QUERY completes. Replacement
            // slots may prime independently, but activation is gated on this completion.
            let keys = removed
                .iter()
                .filter(|key| {
                    self.tracks
                        .get(key)
                        .is_some_and(|track| track.surface_key == surface)
                })
                .copied()
                .collect::<Vec<_>>();
            let tracks = keys
                .into_iter()
                .filter_map(|key| {
                    self.pending.remove(&key);
                    self.tracks.remove(&key)
                })
                .collect();
            if let Some(existing) = self.retiring_surfaces.get_mut(&surface) {
                existing.tracks.extend(tracks);
            } else {
                self.retiring_surfaces.insert(
                    surface,
                    RetiringSurface {
                        clock,
                        tracks,
                        result: None,
                    },
                );
            }
        }
        self.poll_retiring_surfaces()?;
        for key in removed {
            self.remove_track(key)?;
        }

        let mut recreated = HashSet::new();
        for source in sources {
            let changed = self.tracks.get(&source.key).is_some_and(|track| {
                track.kind != source.kind
                    || track.decoder_reset_serial != source.decoder_reset_serial
                    || (track.mode == TrackMode::Live) != source.live
            });
            if changed {
                self.remove_track(source.key)?;
            }
            if !self.tracks.contains_key(&source.key) {
                self.create_outer_track(source)?;
                recreated.insert(source.key);
            }
        }
        self.active_sources = sources
            .iter()
            .cloned()
            .map(|source| (source.key, source))
            .collect();
        self.reconcile_nodes(nodes)?;
        self.update_playback(&[], sources)?;
        self.remove_absent_surfaces(surfaces)?;
        Ok(recreated)
    }

    /// Replaces the outer session with a new one and rebuilds this projection on it.
    ///
    /// The old session is cancelled without waiting for its GOODBYE. Every outer object, queued
    /// notification and in-flight overlay request of the old session is discarded, and microphone
    /// routes are re-created on the new session.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::Interrupted`] once the bridge has been cancelled. Connection and
    /// authentication failures are returned as the new session reports them, and the snapshot and
    /// reconciliation errors are those of [`rebuild`](Self::rebuild).
    pub fn replace_session(
        &mut self,
        surfaces: &[BridgeSurface],
        sources: &[BridgeSource],
        nodes: &[BridgeNode],
    ) -> io::Result<HashSet<BridgeSourceKey>> {
        validate_snapshot(surfaces, sources, nodes)?;
        if self.cancellation.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "bridge cancelled",
            ));
        }
        let session = self
            .route
            .connect(&self.authentication, &self.target_profile)?;
        let display = display_from_target(&session, self.display)?;
        self.cancellation.install(&session)?;
        let replaced = std::mem::replace(&mut self.session, session);
        // Drop cancels the retired SDK transport without waiting for GOODBYE.
        drop(replaced);
        // Before joining the request workers: one waiting on a retired scene is released by this.
        self.overlay_revisions.retire(None);
        self.retire_overlay_jobs();
        let microphone_requests = self.microphones.requests();
        self.microphones = crate::microphone::Microphones::default();
        self.display = display;
        self.surfaces.clear();
        self.overlay_input.take();
        self.overlay_windows.clear();
        *self
            .overlay_layouts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = OverlayLayouts::default();
        self.overlay_environment = None;
        for track in self.tracks.values() {
            let _ = track.channel.close();
        }
        self.tracks.clear();
        self.retiring_surfaces.clear();
        self.hold_updates.clear();
        self.hold_serials.clear();
        self.incompatible_sources.clear();
        self.source_errors.clear();
        self.nodes.clear();
        self.desired_nodes.clear();
        self.desired_surfaces.clear();
        self.pending.clear();
        self.active_sources.clear();
        self.surface_clock.clear();
        self.unfinished_tracks.clear();
        self.surface_refresh.clear();
        self.losses.clear();
        self.full_frames.clear();
        self.keyframes.clear();
        self.playback.clear();
        self.positions.clear();
        self.position_observer = PositionObserver::new(&self.session)?;
        self.terminal_error = None;
        self.display_changed = true;
        self.diagnostic_generation = self.diagnostic_generation.saturating_add(1);
        self.sync_microphones(&microphone_requests)?;
        self.rebuild(surfaces, sources, nodes)
    }

    /// Apply actionable outer-session events before issuing another scene transaction.
    ///
    /// The SDK intentionally exposes `TARGET_CHANGED` as an event plus an explicit cache update.
    /// If the bridge leaves it queued, every later node commit names the stale target generation
    /// and the relayed grid remains at its old height.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::BrokenPipe`] once the outer connection has closed, and keeps returning it
    /// until [`replace_session`](Self::replace_session). A target change that cannot be applied is
    /// returned as the session reports it.
    pub fn service_session_events(&mut self) -> io::Result<Option<DisplayMetrics>> {
        self.drain_session_events()?;
        Ok(std::mem::take(&mut self.display_changed).then_some(self.display))
    }

    fn drain_session_events(&mut self) -> io::Result<()> {
        if let Some(error) = &self.terminal_error {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, error.clone()));
        }
        while let Some(event) = self.session.take_event()? {
            match event {
                SessionEvent::PlaybackHold(hold) => {
                    let Some(surface) = self.surfaces.iter().find_map(|(key, surface)| {
                        (surface.context_id() == hold.context_id && surface.id() == hold.surface_id)
                            .then_some(*key)
                    }) else {
                        continue;
                    };
                    if self
                        .hold_serials
                        .get(&surface)
                        .is_some_and(|serial| *serial >= hold.serial)
                    {
                        continue;
                    }
                    self.hold_serials.insert(surface, hold.serial);
                    let Some(source) = hold_source(&self.active_sources, surface) else {
                        continue;
                    };
                    let position = hold.position.filter(|position| {
                        self.tracks.values().any(|track| {
                            track.surface_key == surface
                                && track.track.id() == position.track_id
                                && track.track.channel_generation().get()
                                    == position.channel_generation
                        })
                    });
                    self.hold_updates.insert(
                        source.key,
                        vivid_sdk::presenter::BridgeHoldSnapshot {
                            decoder_reset_serial: source.decoder_reset_serial,
                            held: hold.held,
                            position_pts_us: position.map(|position| position.pts_us),
                            estimated: position.is_none_or(|position| position.estimated),
                        },
                    );
                }
                SessionEvent::TargetChanged(payload) => {
                    self.session.apply_target_changed(&payload)?;
                    self.display = display_from_target(&self.session, self.display)?;
                    self.display_changed = true;
                }
                SessionEvent::TrackLost { object_id, payload } => {
                    let context_id = payload_u64(&payload, 0);
                    let surface_id = payload_u64(&payload, 1);
                    let track_id = payload_u64(&payload, 2);
                    if let Some(key) = self.tracks.iter().find_map(|(key, outer)| {
                        let configuration = outer.track.configuration().ok()?;
                        (object_id == configuration.track_id
                            && context_id == Some(configuration.context_id)
                            && surface_id == Some(configuration.surface_id)
                            && track_id == Some(configuration.track_id))
                        .then_some(*key)
                    }) {
                        self.losses.insert(key);
                    }
                }
                SessionEvent::ConnectionClosed { diagnostic } => {
                    self.terminal_error = Some(diagnostic.clone());
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, diagnostic));
                }
                SessionEvent::AnchorReady { .. }
                | SessionEvent::AnchorGone { .. }
                | SessionEvent::ContextChanged { .. }
                | SessionEvent::FileDropOffered(_)
                | SessionEvent::FileDropCancelled(_)
                | SessionEvent::Other { .. } => {}
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for OuterBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OuterBridge")
            .field("target_profile", &self.target_profile)
            .field("display", &self.display)
            .field("diagnostic_generation", &self.diagnostic_generation)
            .field("surfaces", &self.surfaces.len())
            .field("tracks", &self.tracks.len())
            .field("terminal_error", &self.terminal_error)
            .finish_non_exhaustive()
    }
}

impl Drop for OuterBridge {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.retire_overlay_jobs();
        self.position_observer.take();
        for track in self.tracks.values() {
            let _ = track.channel.close();
        }
        for retired in self.retiring_surfaces.values() {
            for track in &retired.tracks {
                let _ = track.channel.close();
            }
        }
    }
}

fn surface_key(source: &BridgeSource) -> BridgeSurfaceKey {
    BridgeSurfaceKey {
        producer: source.key.producer,
        context: source.key.context,
        surface: source.key.surface,
    }
}

/// The registered error code behind a presenter rejection, if the failure came from the presenter.
fn presenter_code(error: &io::Error) -> Option<u64> {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<vivid_sdk::PresenterError>())
        .map(|error| error.code)
}

fn payload_u64(payload: &[(u64, Value)], key: u64) -> Option<u64> {
    payload
        .iter()
        .find_map(|(candidate, value)| (*candidate == key).then(|| value.as_u64()).flatten())
}

fn validate_snapshot(
    surfaces: &[BridgeSurface],
    sources: &[BridgeSource],
    nodes: &[BridgeNode],
) -> io::Result<()> {
    let surface_keys = surfaces
        .iter()
        .map(|surface| surface.key)
        .collect::<HashSet<_>>();
    if surface_keys.len() != surfaces.len()
        || surfaces.iter().any(|surface| {
            surface.key.producer == 0
                || surface.key.context == 0
                || surface.key.surface == 0
                || surface.logical_width == 0
                || surface.logical_height == 0
        })
    {
        return Err(invalid_data(
            "bridge snapshot contains duplicate or incomplete surface identity",
        ));
    }
    let keys = sources
        .iter()
        .map(|source| source.key)
        .collect::<HashSet<_>>();
    if keys.len() != sources.len()
        || keys
            .iter()
            .any(|key| key.producer == 0 || key.context == 0 || key.surface == 0 || key.track == 0)
    {
        return Err(invalid_data(
            "bridge snapshot contains duplicate or incomplete track identity",
        ));
    }
    for source in sources {
        if !surface_keys.contains(&surface_key(source)) {
            return Err(invalid_data("bridge track references a missing surface"));
        }
        if matches!(
            source.kind,
            BridgeSourceKind::Raster { .. } | BridgeSourceKind::Image { .. }
        ) && !source.live
        {
            return Err(invalid_data("static bridge track is not live"));
        }
        if let Some(raw_gain) = source.audio_gain
            && (!matches!(source.kind, BridgeSourceKind::Audio { .. })
                || AudioGain::new(raw_gain).is_none())
        {
            return Err(invalid_data(
                "bridge audio gain is invalid or belongs to a non-audio track",
            ));
        }
        if let BridgeSourceKind::Audio {
            linked_video: Some(video),
            ..
        } = source.kind
            && (!keys.contains(&video)
                || video.producer != source.key.producer
                || video.context != source.key.context
                || video.surface != source.key.surface)
        {
            return Err(invalid_data(
                "linked audio references a missing or foreign video track",
            ));
        }
    }
    let mut node_keys = HashSet::new();
    for node in nodes {
        if !surface_keys.contains(&node.surface)
            || !node_keys.insert((node.producer, node.node, node.fragment))
            || node.width <= 0
            || node.height <= 0
            || node.clip.width <= 0
            || node.clip.height <= 0
        {
            return Err(invalid_data(
                "bridge snapshot contains an invalid scene node",
            ));
        }
    }
    Ok(())
}

/// Maps a poisoned lock on `what` to an I/O error naming it.
fn poisoned<T>(what: &'static str) -> impl FnOnce(std::sync::PoisonError<T>) -> io::Error {
    move |error| io::Error::other(format!("{what}: {error}"))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
