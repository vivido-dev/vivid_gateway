//! Slot activation, the outer surface clock, and playback position feedback.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use vivid_protocol::cbor::Value;
use vivid_protocol::messages;
use vivid_protocol::registry;
use vivid_protocol::track::{AudioGain, TrackMode};
use vivid_sdk::presenter::{
    BridgePlayRequest, BridgeSource, BridgeSourceKey, BridgeSourceKind, BridgeSurfaceKey,
};
use vivid_sdk::{RequestMetadata, SlotBinding, Track};

use super::config::{SLOT_AUDIO, SLOT_VIDEO, slot_for_kind};
use super::relay::OuterMediaCommand;
use super::{
    OUTER_TIMED_PREROLL_RECORDS, OuterBridge, OuterTrack, START_POLICY_SYNCHRONIZED, invalid_data,
    presenter_code, surface_key,
};

/// `TRACK_STATUS` playback-state map key holding the clock state (media specification §15).
const PLAYBACK_STATE_KEY: u64 = 3;
/// `TRACK_STATUS` playback-state map key holding the current clock PTS.
const PLAYBACK_CLOCK_PTS_KEY: u64 = 4;
/// Clock state code for a paused surface group.
const PLAYBACK_PAUSED: u64 = 3;
/// Interval between background `TRACK_STATUS` queries.
///
/// One outstanding query at a time, so this caps the observer at 20 queries per second across
/// every track. Shortening it makes activation and EOS feedback quicker at the cost of more
/// control-connection traffic.
const POSITION_POLL_INTERVAL: Duration = Duration::from_millis(50);
/// Hold-position workers that may run at once.
///
/// Each worker is a thread blocked on one PAUSE + query round trip for a retiring surface. The cap
/// bounds thread count when many owners quit together; later retirements wait for a free worker.
const MAX_HOLD_QUERIES: usize = 16;

/// The clock state the bridge reports for a relayed timed track.
///
/// These are the two states of the media specification's `PLAYBACK_STATE` this bridge produces;
/// `u64::from` gives the wire code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClockState {
    /// PLAY was admitted, but the clock waits for buffered or synchronized media.
    Buffering,
    /// The surface clock is running.
    Playing,
}

impl From<ClockState> for u64 {
    fn from(state: ClockState) -> Self {
        match state {
            ClockState::Buffering => 1,
            ClockState::Playing => 2,
        }
    }
}

/// How far an end of stream has progressed through the outer presenter.
///
/// Ordered by progress, so a later observation compares greater. `u64::from` gives the media
/// specification's `PLAYBACK_STATE` EOS code.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum EosState {
    /// No EOS has reached the outer presenter.
    #[default]
    NotReceived,
    /// The outer presenter accepted EOS; buffered media is still playing out.
    Accepted,
    /// Every buffered record before EOS has been presented.
    Applied,
}

impl From<EosState> for u64 {
    fn from(state: EosState) -> Self {
        match state {
            EosState::NotReceived => 0,
            EosState::Accepted => 1,
            EosState::Applied => 2,
        }
    }
}

impl OuterBridge {
    /// Applies the playback changes between two consecutive inner source snapshots.
    ///
    /// Activates surface slots whose inner slot map changed, relays audio gain, starts, rebases and
    /// pauses surface clocks, and records EOS requests for
    /// [`flush_pending_eos`](Self::flush_pending_eos). A synchronized start the outer presenter cannot
    /// honour is reported through [`take_source_errors`](Self::take_source_errors) instead.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] when a relayed audio gain is out of range or names a
    /// non-audio track, or when a playing source has no outer track. Outer session failures are
    /// returned as the session reports them.
    pub fn update_playback(
        &mut self,
        previous: &[BridgeSource],
        current: &[BridgeSource],
    ) -> io::Result<()> {
        self.incompatible_sources
            .retain(|key, _| current.iter().any(|source| source.key == *key));
        if !self.session.supports(registry::TIMED_MEDIA_SYNC) {
            for source in current.iter().filter(|source| {
                !source.live && source.play_request.start_policy == START_POLICY_SYNCHRONIZED
            }) {
                if self
                    .incompatible_sources
                    .insert(source.key, source.decoder_reset_serial)
                    != Some(source.decoder_reset_serial)
                {
                    self.source_errors
                        .push((source.key, source.decoder_reset_serial));
                    if let Some(track) = self.tracks.get(&source.key) {
                        let _ = track.channel.close();
                    }
                }
            }
        }
        let previous_sources = previous
            .iter()
            .cloned()
            .map(|source| (source.key, source))
            .collect::<HashMap<_, _>>();
        // Playback helpers must read the new authoritative request. Keeping this assignment until
        // the end made a changed PLAY either use the old PTS or disappear entirely because the
        // outer tracks were already marked playing.
        self.active_sources = current
            .iter()
            .cloned()
            .map(|source| (source.key, source))
            .collect();
        let changed_slot_surfaces = current
            .iter()
            .filter_map(|source| {
                let changed = previous
                    .iter()
                    .find(|candidate| candidate.key == source.key)
                    .is_none_or(|old| old.active != source.active);
                changed.then_some(surface_key(source))
            })
            .collect::<HashSet<_>>();
        for source in current.iter().filter(|source| !source.active) {
            if let Some(track) = self.tracks.get_mut(&source.key) {
                track.activated = false;
                track.slot_activated.store(false, Ordering::Release);
            }
        }
        for surface in changed_slot_surfaces {
            if current
                .iter()
                .any(|source| surface_key(source) == surface && source.active)
            {
                self.try_activate_surface_slots(surface)?;
            }
        }
        for source in current {
            if self.incompatible_sources.contains_key(&source.key) {
                continue;
            }
            let previous_gain = previous
                .iter()
                .find(|candidate| candidate.key == source.key)
                .and_then(|candidate| candidate.audio_gain);
            if source.audio_gain == previous_gain {
                continue;
            }
            let Some(raw_gain) = source.audio_gain else {
                continue;
            };
            let gain = AudioGain::new(raw_gain)
                .ok_or_else(|| invalid_data("relayed audio gain is out of range"))?;
            let track = self
                .tracks
                .get(&source.key)
                .filter(|track| matches!(track.kind, BridgeSourceKind::Audio { .. }))
                .map(|track| track.track.clone())
                .ok_or_else(|| invalid_data("relayed audio gain does not name an audio track"))?;
            self.session.set_audio_gain(&track, gain)?;
        }
        let mut rebased_surfaces = HashSet::new();
        let mut paused_surfaces = HashSet::new();
        for source in current {
            if self.incompatible_sources.contains_key(&source.key) {
                continue;
            }
            let old = previous
                .iter()
                .find(|candidate| candidate.key == source.key);
            if source.playing
                && old.is_none_or(|old| !old.playing || old.play_request != source.play_request)
            {
                if old.is_some_and(|old| old.playing && old.play_request != source.play_request) {
                    let surface = self
                        .tracks
                        .get(&source.key)
                        .map(|track| track.surface_key)
                        .ok_or_else(|| invalid_data("playback track is missing"))?;
                    if rebased_surfaces.insert(surface) {
                        self.rebase_surface_clock(source.key)?;
                    }
                } else {
                    self.try_start_surface(source.key)?;
                }
            } else if !source.playing && old.is_some_and(|old| old.playing) {
                let surface = self
                    .tracks
                    .get(&source.key)
                    .map(|track| track.surface_key)
                    .ok_or_else(|| invalid_data("playback track is missing"))?;
                if paused_surfaces.insert(surface) {
                    // Name the selected audio master consistently; PAUSE controls its entire
                    // owner-scoped surface group at the physical presenter.
                    let clock = preferred_surface_clock(&previous_sources, surface, source.key);
                    let track = self
                        .tracks
                        .get(&clock)
                        .ok_or_else(|| invalid_data("outer playback clock is missing"))?
                        .track
                        .clone();
                    self.session.pause(&track)?;
                    // PAUSE is surface-group state. Keep the bridge's bookkeeping equally
                    // broad so the recovery PLAY is not mistaken for a redundant PLAY on the
                    // old clock and silently skipped.
                    for track in self
                        .tracks
                        .values_mut()
                        .filter(|track| track.surface_key == surface)
                    {
                        track.playing = false;
                        track.resume_after_submission =
                            matches!(track.kind, BridgeSourceKind::Audio { .. })
                                .then_some(track.media_submitted);
                        track.preplay_ceiling = track
                            .media_submitted
                            .saturating_add(OUTER_TIMED_PREROLL_RECORDS);
                    }
                }
            }
            if source.eos_epoch.is_some()
                && old.is_none_or(|old| old.eos_epoch != source.eos_epoch)
                && let Some(track) = self.tracks.get_mut(&source.key)
                && !track.eos
            {
                // The snapshot can overtake media that was already received on the client IPC
                // connection but still sits in its per-track queue. Record the intent here; the
                // bridge worker calls `flush_pending_eos` only after that source queue is empty,
                // and the per-track writer then serializes EOS behind every accepted packet.
                track.eos_requested = true;
            }
        }
        self.reconcile_paused_clocks()
    }

    fn rebase_surface_clock(&mut self, key: BridgeSourceKey) -> io::Result<()> {
        let surface_key = self
            .tracks
            .get(&key)
            .map(|track| track.surface_key)
            .ok_or_else(|| invalid_data("playback track is missing"))?;
        // A recovery PLAY can overtake the first keyframe on the independent control connection.
        // Keep its authoritative PTS in `active_sources`, but do not send it to the outer
        // presenter before this replacement surface has active slots. Vivido correctly rejects a
        // PLAY against an inactive surface; treating that admission error as outer-session loss
        // used to rebuild the bridge again in the middle of every tab/attachment recovery.
        if !self
            .tracks
            .values()
            .any(|track| track.surface_key == surface_key && track.activated)
        {
            return Ok(());
        }
        self.play_surface_clock(key, surface_key)
    }

    fn play_surface_clock(
        &mut self,
        key: BridgeSourceKey,
        surface: BridgeSurfaceKey,
    ) -> io::Result<()> {
        let clock = preferred_surface_clock(&self.active_sources, surface, key);
        let request = self
            .active_sources
            .get(&key)
            .map(|source| source.play_request)
            .ok_or_else(|| invalid_data("authoritative playback request is missing"))?;
        let clock_track = self
            .tracks
            .get(&clock)
            .ok_or_else(|| invalid_data("outer playback clock is missing"))?
            .track
            .clone();
        self.session
            .play_with(&clock_track, bridge_play_options(request))?;
        let playing_members = self
            .active_sources
            .values()
            .filter(|source| {
                surface_key(source) == surface
                    && source_is_effectively_playing(&self.active_sources, source)
            })
            .map(|source| source.key)
            .collect::<Vec<_>>();
        for member in playing_members {
            if let Some(track) = self.tracks.get_mut(&member) {
                track.playing = true;
                track.resume_after_submission = None;
            }
        }
        self.record_published_clock(surface, request);
        self.playback.push((
            key,
            PlaybackSnapshot {
                decoder_reset_serial: self.tracks[&key].decoder_reset_serial,
                clock: if request.start_policy == START_POLICY_SYNCHRONIZED {
                    ClockState::Buffering
                } else {
                    ClockState::Playing
                },
                eos: EosState::NotReceived,
            },
        ));
        Ok(())
    }

    /// Remember the position the outer session now holds for one surface.
    ///
    /// Vivido applies PLAY to every active timed slot on the named surface, so the record is
    /// surface-wide, but only the slots it actually reached received the clock.
    fn record_published_clock(&mut self, surface: BridgeSurfaceKey, request: BridgePlayRequest) {
        self.surface_clock.insert(surface, request);
        for track in self
            .tracks
            .values_mut()
            .filter(|track| track.surface_key == surface && track.activated)
        {
            track.published_play_request = Some(request);
        }
    }

    /// The paused position one surface holds that the outer session has not been given.
    ///
    /// `playing` is an edge; the position a paused surface sits at is level state. A nested
    /// producer publishes it as PLAY followed immediately by PAUSE - Vivi does exactly that to
    /// place a seek target without starting time - and consecutive projection snapshots coalesce,
    /// so that rising edge is routinely never observed here. A seek also replaces the timed
    /// tracks, and a replacement outer track has no clock of its own. Either way the outer
    /// presenter holds every decoded picture and the pane goes blank until the producer next
    /// resumes, which is not something the user asked for by seeking while paused.
    fn pending_paused_clock(
        &self,
        surface: BridgeSurfaceKey,
    ) -> Option<(BridgeSourceKey, BridgePlayRequest)> {
        let mut members = self
            .active_sources
            .values()
            .filter(|source| {
                surface_key(source) == surface
                    && source.active
                    && matches!(
                        source.kind,
                        BridgeSourceKind::Video { .. } | BridgeSourceKind::Audio { .. }
                    )
            })
            .collect::<Vec<_>>();
        if members.is_empty() {
            return None;
        }
        members.sort_by_key(|source| source.key.track);
        if members
            .iter()
            .any(|source| source_is_effectively_playing(&self.active_sources, source))
        {
            return None;
        }
        // Vivido refuses PLAY for a surface with no active timed slot. Wait for the ordinary
        // pre-roll path to activate rather than turning that refusal into outer-session loss.
        let outer = members
            .iter()
            .map(|source| self.tracks.get(&source.key))
            .collect::<Option<Vec<_>>>()?;
        if !outer.iter().all(|track| track.activated) {
            return None;
        }
        // Name video to preserve its requested seek target. PLAY positions every active timed
        // slot, including physical audio; the immediately following PAUSE freezes the group.
        let clock = members
            .iter()
            .position(|source| matches!(source.kind, BridgeSourceKind::Video { .. }))
            .unwrap_or(0);
        let request = members[clock].play_request;
        // Projection removal destroys the old outer clock. A correlated resume PLAY is
        // nevertheless an explicit producer position, including when PLAY/PAUSE coalesced
        // before this bridge observed a playing edge. Only uncorrelated baseline state needs
        // evidence of a previously published clock.
        if !self.surface_clock.contains_key(&surface) && request.hold_serial.is_none() {
            return None;
        }
        outer
            .iter()
            .any(|track| track.published_play_request != Some(request))
            .then_some((members[clock].key, request))
    }

    /// Republish one paused surface's authoritative position to the outer session.
    fn rebase_paused_clock(
        &mut self,
        clock: BridgeSourceKey,
        surface: BridgeSurfaceKey,
        request: BridgePlayRequest,
    ) -> io::Result<()> {
        let track = self
            .tracks
            .get(&clock)
            .ok_or_else(|| invalid_data("outer paused clock is missing"))?
            .track
            .clone();
        match self.session.play_with(&track, bridge_play_options(request)) {
            Ok(()) => {}
            // The activation this republication reads is the bridge's own mirror of an
            // independently serviced control connection. A refusal means the outer surface has
            // moved on, not that the outer session was lost; the next reconcile retries.
            Err(error) if presenter_code(&error) == Some(messages::ERROR_BAD_STATE) => {
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        // PLAY is the only way to say where a flushed timed surface resumes; the PAUSE that
        // follows is what keeps it there. Publish the pair together so the outer clock is
        // positioned without ever running.
        self.session.pause(&track)?;
        for track in self
            .tracks
            .values_mut()
            .filter(|track| track.surface_key == surface)
        {
            track.playing = false;
        }
        self.record_published_clock(surface, request);
        Ok(())
    }

    /// Reconcile every paused surface against the position its producer published.
    fn reconcile_paused_clocks(&mut self) -> io::Result<()> {
        let mut surfaces = self.surfaces.keys().copied().collect::<Vec<_>>();
        surfaces.sort_by_key(|surface| (surface.producer, surface.context, surface.surface));
        for surface in surfaces {
            let Some((clock, request)) = self.pending_paused_clock(surface) else {
                continue;
            };
            self.rebase_paused_clock(clock, surface, request)?;
        }
        Ok(())
    }

    /// Enqueue requested EOS markers after all earlier client-side media for those tracks.
    ///
    /// `blocked` is the set of sources that still have a record waiting in the foreground
    /// bridge. Each track writer preserves command order, so accepting EOS here establishes the
    /// complete inner-packets-before-outer-EOS ordering without waiting on another track.
    pub fn flush_pending_eos(&mut self, blocked: &HashSet<BridgeSourceKey>) {
        let ready = self
            .tracks
            .iter()
            .filter_map(|(key, track)| {
                (track.eos_requested
                    && !track.eos
                    // Retained image replay is requested after projection acknowledgement and
                    // may not have reached the foreground queue yet. An empty queue therefore
                    // does not prove the image body preceded EOS. This writer generation must
                    // have accepted its complete IMAGE_DATA before its channel can end.
                    && (!matches!(track.kind, BridgeSourceKind::Image { .. })
                        || track.media_submitted != 0)
                    && !blocked.contains(key)
                    && !self.pending.contains_key(key))
                .then_some(*key)
            })
            .collect::<Vec<_>>();
        for key in ready {
            let Some(track) = self.tracks.get_mut(&key) else {
                continue;
            };
            match track.media_sender.try_send(OuterMediaCommand::Eos) {
                Ok(()) => {
                    track.eos = true;
                }
                Err(mpsc::TrySendError::Full(_)) => {}
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    self.losses.insert(key);
                }
            }
        }
    }

    /// Recheck output readiness for a previously accepted PLAY intent.
    ///
    /// Track media and control replies travel on independent connections, so the query immediately
    /// following a successful media write may race the presenter's readiness update. The bridge
    /// worker calls this without blocking while the intent remains authoritative.
    ///
    /// # Errors
    ///
    /// Outer session failures are returned as the session reports them.
    pub fn retry_pending_playback(&mut self) -> io::Result<()> {
        let pending = self
            .active_sources
            .values()
            .filter(|source| {
                source.playing
                    && self
                        .tracks
                        .get(&source.key)
                        .is_some_and(|track| !track.playing)
            })
            .map(|source| source.key)
            .collect::<Vec<_>>();
        for key in pending {
            self.try_start_surface(key)?;
        }
        // A paused surface's position needs the same retry: the snapshot that carries it usually
        // arrives before its replacement tracks have decoded output, so the activation this
        // republication depends on has not happened yet.
        self.reconcile_paused_clocks()
    }

    /// Recheck authoritative slot readiness without assuming that a completed socket write has
    /// already been observed by the independently serviced outer control connection.
    ///
    /// This includes live audio. A browser activates its raster and audio slots without PLAY; if
    /// the relay waits only on `playing`, the audio writer parks on its first pre-roll record and
    /// never returns the credit that lets the browser's shared raster/audio worker continue.
    ///
    /// # Errors
    ///
    /// Outer session failures, and failures to clean up retired surfaces, are returned as the session
    /// reports them.
    pub fn retry_pending_activation(&mut self) -> io::Result<()> {
        self.poll_playback_progress();
        self.poll_retiring_surfaces()?;
        let surfaces = self.desired_surfaces.clone();
        self.remove_absent_surfaces(&surfaces)?;
        let nodes = self.desired_nodes.clone();
        self.reconcile_nodes(&nodes)?;
        let pending = self
            .tracks
            .iter()
            .filter_map(|(key, track)| {
                (!track.activated
                    && self
                        .active_sources
                        .get(key)
                        .is_some_and(|source| source.active))
                .then_some(track.surface_key)
            })
            .collect::<HashSet<_>>();
        for surface in pending {
            self.try_activate_surface_slots(surface)?;
        }
        Ok(())
    }

    /// Takes the playback transitions observed since the last call.
    pub fn take_playback_states(&mut self) -> Vec<(BridgeSourceKey, PlaybackSnapshot)> {
        self.poll_playback_progress();
        std::mem::take(&mut self.playback)
    }

    /// Takes the latest observed playback position of each timed track.
    pub fn take_positions(
        &mut self,
    ) -> Vec<(
        BridgeSourceKey,
        vivid_sdk::presenter::BridgePositionSnapshot,
    )> {
        std::mem::take(&mut self.positions)
    }

    /// Takes the presentation-hold updates the outer presenter reported since the last call.
    pub fn take_playback_holds(
        &mut self,
    ) -> Vec<(BridgeSourceKey, vivid_sdk::presenter::BridgeHoldSnapshot)> {
        self.hold_updates.drain().collect()
    }

    /// Takes the sources the outer presenter cannot play as requested.
    ///
    /// Each entry carries the decoder-reset serial it applies to. A source is reported once per serial.
    pub fn take_source_errors(&mut self) -> Vec<(BridgeSourceKey, u64)> {
        std::mem::take(&mut self.source_errors)
    }

    pub(super) fn poll_retiring_surfaces(&mut self) -> io::Result<()> {
        let mut running = self
            .retiring_surfaces
            .values()
            .filter(|retired| retired.result.is_some())
            .count();
        let mut finished = Vec::new();
        for (surface, retired) in &mut self.retiring_surfaces {
            if retired.result.is_none() && running < MAX_HOLD_QUERIES {
                let query = self.session.track_pause_query_handle();
                let track = retired.clock.track.clone();
                let (send, receive) = mpsc::sync_channel(1);
                thread::Builder::new()
                    .name("vivid-hold-position".into())
                    .spawn(move || {
                        let result = query
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::Unsupported,
                                    "physical hold requires a control connection",
                                )
                            })
                            .and_then(|query| query(&track));
                        let _ = send.send(result);
                    })?;
                retired.result = Some(receive);
                running += 1;
            }
            if let Some(result) = &retired.result {
                match result.try_recv() {
                    Ok(status) => finished.push((*surface, status)),
                    Err(mpsc::TryRecvError::Disconnected) => {
                        finished.push((*surface, Err(io::Error::other("hold observer stopped"))));
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                }
            }
        }
        let mut first_error = None;
        for (surface, result) in finished {
            let retired = self
                .retiring_surfaces
                .get_mut(&surface)
                .expect("pending retirement");
            if let Ok(status) = result {
                let job = &retired.clock;
                if status.channel_generation == job.track.channel_generation() {
                    let clock_pts_us = status.playback_state.as_ref().and_then(|fields| {
                        fields.iter().find_map(|(key, value)| {
                            (*key == PLAYBACK_CLOCK_PTS_KEY)
                                .then(|| value.as_i64())
                                .flatten()
                        })
                    });
                    self.positions.retain(|(key, _)| *key != job.key);
                    self.positions.push((
                        job.key,
                        vivid_sdk::presenter::BridgePositionSnapshot {
                            decoder_reset_serial: job.decoder_reset_serial,
                            playing: job.playing,
                            start_pts_us: job.start_pts_us,
                            state: PLAYBACK_PAUSED,
                            clock_pts_us,
                            decoded_pts_us: status.last_decoded_pts_us,
                            presented_pts_us: status.last_presented_pts_us,
                            presentation_id: status.last_presentation_id,
                        },
                    ));
                }
            }
            // A track that fails to destroy keeps its retirement, so the next poll retries it; its
            // result channel is already drained, so that retry does not publish a position again.
            for track in std::mem::take(&mut retired.tracks) {
                let _ = track.channel.close();
                if let Err(error) = self
                    .session
                    .destroy_track(&track.track, &RequestMetadata::default())
                {
                    first_error.get_or_insert(error);
                    retired.tracks.push(track);
                }
            }
            if !retired.tracks.is_empty() {
                continue;
            }
            self.retiring_surfaces.remove(&surface);
            if let Some(handle) = self.surfaces.get(&surface) {
                // As in `remove_track`, a failed refresh is left for the next rebuild to retry.
                self.surface_refresh.insert(surface);
                match self.session.query_surface(handle) {
                    Ok(_) => {
                        self.surface_refresh.remove(&surface);
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn try_activate_surface_slots(&mut self, target_surface: BridgeSurfaceKey) -> io::Result<()> {
        if self.incompatible_sources.keys().any(|key| {
            key.producer == target_surface.producer
                && key.context == target_surface.context
                && key.surface == target_surface.surface
        }) {
            return Ok(());
        }
        if self.retiring_surfaces.contains_key(&target_surface) {
            return Ok(());
        }
        let active = self
            .active_sources
            .values()
            .filter(|source| surface_key(source) == target_surface && source.active)
            .map(|source| source.key)
            .collect::<Vec<_>>();
        if active.is_empty() {
            return Ok(());
        }
        let mut bindings = Vec::with_capacity(active.len());
        for key in &active {
            let track = self
                .tracks
                .get(key)
                .ok_or_else(|| invalid_data("active outer track is missing"))?;
            let output_ready = if self.position_observer.is_some() {
                track.output_ready
            } else {
                self.session.query_track(&track.track)?.milestones
                    & vivid_sdk::MILESTONE_OUTPUT_READY
                    != 0
            };
            if !output_ready {
                // The observer must not put a remote round trip between media chunks or
                // pre-roll packets. The bounded writer continues feeding the decoder meanwhile.
                return Ok(());
            }
            bindings.push(SlotBinding {
                slot: slot_for_kind(&track.kind),
                track_id: track.track.id(),
                expected_channel_generation: track.track.channel_generation(),
                required_milestone: vivid_sdk::MILESTONE_OUTPUT_READY,
            });
        }
        let surface = self
            .surfaces
            .get(&target_surface)
            .ok_or_else(|| invalid_data("outer activation surface is missing"))?;
        match self
            .session
            .activate_tracks(surface, &bindings, &RequestMetadata::default())
        {
            Ok(_) => {}
            Err(error) if presenter_code(&error) == Some(messages::ERROR_BAD_STATE) => {
                for key in &active {
                    self.tracks
                        .get_mut(key)
                        .expect("active track exists")
                        .output_ready = false;
                }
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        let active = active.into_iter().collect::<HashSet<_>>();
        for (key, track) in self
            .tracks
            .iter_mut()
            .filter(|(_, track)| track.surface_key == target_surface)
        {
            let activated = active.contains(key);
            track.activated = activated;
            // Static retained deliveries keep their ingress barrier until activation. Timed
            // pre-roll uses channel flow and its packet ceiling independently of these queries.
            track.slot_activated.store(activated, Ordering::Release);
        }
        Ok(())
    }

    fn try_start_surface(&mut self, key: BridgeSourceKey) -> io::Result<()> {
        if self.incompatible_sources.contains_key(&key) {
            return Ok(());
        }
        let surface_key = self
            .tracks
            .get(&key)
            .map(|track| track.surface_key)
            .ok_or_else(|| invalid_data("playback track is missing"))?;
        let member_keys = self
            .tracks
            .iter()
            .filter_map(|(candidate, track)| {
                (track.surface_key == surface_key).then_some(*candidate)
            })
            .collect::<Vec<_>>();
        let members = member_keys
            .into_iter()
            .filter_map(|member| {
                let track = &self.tracks[&member];
                if !matches!(
                    track.kind,
                    BridgeSourceKind::Video { .. } | BridgeSourceKind::Audio { .. }
                ) {
                    return None;
                }
                let source_playing = self
                    .active_sources
                    .get(&member)
                    .is_some_and(|source| source.playing)
                    || matches!(
                        &track.kind,
                        BridgeSourceKind::Audio {
                            linked_video: Some(video),
                            ..
                        } if self.active_sources.get(video).is_some_and(|source| source.playing)
                    );
                source_playing.then(|| {
                    (
                        member,
                        track.track.clone(),
                        matches!(track.kind, BridgeSourceKind::Audio { .. }),
                    )
                })
            })
            .collect::<Vec<_>>();
        if members.is_empty() {
            return Ok(());
        }
        let clock = preferred_surface_clock(&self.active_sources, surface_key, key);
        if self.tracks.get(&clock).is_some_and(|track| track.playing) {
            // Video and linked audio can both carry the same rising surface edge. The first call
            // starts their shared clock and marks every effective member playing; the second must
            // not emit another PLAY and reconfigure the physical audio output again.
            return Ok(());
        }
        if members
            .iter()
            .all(|(member, _, _)| self.tracks[member].activated)
        {
            // PAUSE keeps the surface's slots and decoded output active, so resume must not repeat
            // initial activation or wait for a keyframe. It does need evidence that linked audio
            // has restarted: playing its retained tail before Vivi has submitted another packet
            // drains into an underrun, after which audio jumps and video follows the wrong clock.
            // A submission is enough. Waiting for its completion could deadlock on a full paused
            // audio ring, whose capacity is returned only after PLAY starts consuming it.
            if self.tracks.get(&clock).is_some_and(|track| {
                !track.eos
                    && track
                        .resume_after_submission
                        .is_some_and(|floor| track.media_submitted <= floor)
            }) {
                return Ok(());
            }
            return self.play_surface_clock(key, surface_key);
        }
        if members.iter().any(|(member, _, _)| {
            let track = &self.tracks[member];
            track.media_completed == 0 && track.media_inflight == 0
        }) {
            // Every member needs at least one submitted pre-roll record before atomic activation.
            return Ok(());
        }
        self.poll_playback_progress();
        if self.position_observer.is_some()
            && members
                .iter()
                .any(|(member, _, _)| !self.tracks[member].output_ready)
        {
            return Ok(());
        }
        let mut bindings = Vec::new();
        for (_, outer_track, audio) in &members {
            bindings.push(SlotBinding {
                slot: if *audio { SLOT_AUDIO } else { SLOT_VIDEO },
                track_id: outer_track.id(),
                expected_channel_generation: outer_track.channel_generation(),
                required_milestone: vivid_sdk::MILESTONE_OUTPUT_READY,
            });
        }
        let already_playing = self.tracks.get(&clock).is_some_and(|track| track.playing);
        if already_playing {
            return Ok(());
        }
        let surface = self
            .surfaces
            .get(&surface_key)
            .ok_or_else(|| invalid_data("outer playback surface is missing"))?
            .clone();
        match self
            .session
            .activate_tracks(&surface, &bindings, &RequestMetadata::default())
        {
            Ok(_) => {}
            Err(error) if presenter_code(&error) == Some(messages::ERROR_BAD_STATE) => {
                // Readiness can be invalidated by recovery between observation and activation.
                // Re-observe without blocking media or widening its bounded pre-roll window.
                for (member, _, _) in &members {
                    self.tracks
                        .get_mut(member)
                        .expect("member still exists")
                        .output_ready = false;
                }
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        for binding in bindings {
            if let Some((_, track)) = self
                .tracks
                .iter_mut()
                .find(|(_, track)| track.track.id() == binding.track_id)
            {
                track.slot_activated.store(true, Ordering::Release);
                track.activated = true;
            }
        }
        self.play_surface_clock(key, surface_key)
    }

    fn poll_playback_progress(&mut self) {
        let Some(observer) = self.position_observer.as_mut() else {
            return;
        };
        if let Ok((job, result)) = observer.output.try_recv() {
            observer.busy = false;
            if let Ok(status) = result
                && let Some(track) = self.tracks.get_mut(&job.key)
                && let Some(source) = self.active_sources.get(&job.key)
                && track.writer_id == job.writer_id
                && status.channel_generation == track.track.channel_generation()
                && source.decoder_reset_serial == job.decoder_reset_serial
                && source.playing == job.playing
                && source.play_request.start_pts_us == job.start_pts_us
            {
                track.output_ready = status.milestones & vivid_sdk::MILESTONE_OUTPUT_READY != 0;
                if track.mode == TrackMode::Timed {
                    let field = |key| {
                        status.playback_state.as_ref().and_then(|map| {
                            map.iter().find(|(k, _)| *k == key).map(|(_, value)| value)
                        })
                    };
                    let position = vivid_sdk::presenter::BridgePositionSnapshot {
                        decoder_reset_serial: job.decoder_reset_serial,
                        playing: job.playing,
                        start_pts_us: job.start_pts_us,
                        state: field(PLAYBACK_STATE_KEY)
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        clock_pts_us: field(PLAYBACK_CLOCK_PTS_KEY).and_then(Value::as_i64),
                        decoded_pts_us: status.last_decoded_pts_us,
                        presented_pts_us: status.last_presented_pts_us,
                        presentation_id: status.last_presentation_id,
                    };
                    track.target_picture_ready = track.published_play_request
                        == Some(source.play_request)
                        && status.milestones & vivid_sdk::MILESTONE_OUTPUT_READY != 0
                        && status.last_decoded_pts_us >= source.play_request.start_pts_us;
                    // One latest observation per track, bounded by the session track reservation.
                    self.positions.retain(|(key, _)| *key != job.key);
                    self.positions.push((job.key, position));
                    let eos = if status.milestones & vivid_sdk::MILESTONE_BUFFERED_ENDED != 0 {
                        EosState::Applied
                    } else if status.milestones & vivid_sdk::MILESTONE_EOS_ACCEPTED != 0 {
                        EosState::Accepted
                    } else {
                        EosState::NotReceived
                    };
                    if eos > track.reported_eos_state {
                        track.reported_eos_state = eos;
                        self.playback.push((
                            job.key,
                            PlaybackSnapshot {
                                decoder_reset_serial: job.decoder_reset_serial,
                                clock: if job.playing {
                                    ClockState::Playing
                                } else {
                                    ClockState::Buffering
                                },
                                eos,
                            },
                        ));
                    }
                }
            }
        }
        if observer.busy || Instant::now() < self.position_poll_at {
            return;
        }
        self.position_poll_at = Instant::now() + POSITION_POLL_INTERVAL;
        let mut keys: Vec<_> = self
            .tracks
            .iter()
            .filter(|(_, t)| {
                (t.mode == TrackMode::Timed && t.activated) || (!t.activated && !t.output_ready)
            })
            .map(|(key, _)| *key)
            .collect();
        keys.sort_by_key(|key| (key.producer, key.context, key.surface, key.track));
        if keys.is_empty() {
            return;
        }
        self.position_cursor %= keys.len();
        let key = keys[self.position_cursor];
        self.position_cursor += 1;
        let track = &self.tracks[&key];
        if let Some(source) = self.active_sources.get(&key) {
            let job = PositionJob {
                key,
                writer_id: track.writer_id,
                track: track.track.clone(),
                decoder_reset_serial: source.decoder_reset_serial,
                playing: source.playing,
                start_pts_us: source.play_request.start_pts_us,
            };
            observer.busy = observer
                .input
                .as_ref()
                .is_some_and(|input| input.try_send(job).is_ok());
        }
    }
}

pub(super) struct PositionJob {
    pub(super) key: BridgeSourceKey,
    pub(super) writer_id: u64,
    pub(super) track: Track,
    pub(super) decoder_reset_serial: u64,
    pub(super) playing: bool,
    pub(super) start_pts_us: i64,
}

pub(super) struct RetiringSurface {
    pub(super) clock: PositionJob,
    pub(super) tracks: Vec<OuterTrack>,
    pub(super) result: Option<mpsc::Receiver<io::Result<vivid_sdk::TrackStatus>>>,
}

pub(super) struct PositionObserver {
    input: Option<mpsc::SyncSender<PositionJob>>,
    output: mpsc::Receiver<(PositionJob, io::Result<vivid_sdk::TrackStatus>)>,
    worker: Option<thread::JoinHandle<()>>,
    pub(super) busy: bool,
}

impl PositionObserver {
    pub(super) fn new(session: &vivid_sdk::Session) -> io::Result<Option<Self>> {
        let Some(query) = session.track_query_handle() else {
            return Ok(None);
        };
        let (input, jobs) = mpsc::sync_channel::<PositionJob>(1);
        let (results, output) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("vivid-position".into())
            .spawn(move || {
                while let Ok(job) = jobs.recv() {
                    let status = query(&job.track);
                    if results.send((job, status)).is_err() {
                        break;
                    }
                }
            })?;
        Ok(Some(Self {
            input: Some(input),
            output,
            worker: Some(worker),
            busy: false,
        }))
    }
}

impl Drop for PositionObserver {
    fn drop(&mut self) {
        // The owning bridge cancels the session before dropping this observer.
        self.input.take();
        if self.busy {
            let _ = self.output.recv();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// One playback transition of a relayed timed track, for the inner presenter to apply.
///
/// Pass `decoder_reset_serial` back with the snapshot: the inner presenter ignores a snapshot
/// whose serial is not current, so a late EOS cannot end a replacement generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaybackSnapshot {
    /// The inner decoder-reset serial this observation belongs to.
    pub decoder_reset_serial: u64,
    /// The outer surface clock state.
    pub clock: ClockState,
    /// How far the track's end of stream has progressed.
    pub eos: EosState,
}

fn bridge_play_options(request: vivid_sdk::presenter::BridgePlayRequest) -> vivid_sdk::PlayOptions {
    vivid_sdk::PlayOptions {
        start_pts_us: request.start_pts_us,
        minimum_buffer_us: request.minimum_buffer_us.max(1),
        maximum_latency_us: request.maximum_latency_us.max(1),
        start_policy: if request.start_policy == START_POLICY_SYNCHRONIZED {
            vivid_sdk::StartPolicy::Synchronized
        } else {
            vivid_sdk::StartPolicy::AfterMinimumBuffer
        },
        // A terminating gateway owns a distinct surface and hold serial domain.
        hold_serial: None,
    }
}

#[cfg(test)]
pub(super) fn default_play_request() -> vivid_sdk::presenter::BridgePlayRequest {
    vivid_sdk::presenter::BridgePlayRequest {
        start_pts_us: 0,
        minimum_buffer_us: 1,
        maximum_latency_us: 1_000_000,
        rate_32_32: 1_i64 << 32,
        late_policy: 1,
        loop_count: 0,
        start_policy: 1,
        hold_serial: None,
    }
}

/// Choose the clock track for one timed surface.
///
/// Linked audio is an active member whenever its video is playing, even when the authoritative
/// snapshot leaves the audio track's own `playing` bit false. PLAY must target that audio track:
/// Vivido propagates the surface clock to every active slot, but only the track named by PLAY has
/// its physical audio output configured and started. Targeting the video on a recovery rebase lets
/// frames resume while the audio clock remains at the original PTS until its stall fallback fires.
///
/// When several audio tracks qualify, the lowest track ID wins, so PLAY and the PAUSE that later
/// stops the same group always name the same clock.
pub(super) fn preferred_surface_clock(
    sources: &HashMap<BridgeSourceKey, BridgeSource>,
    surface: BridgeSurfaceKey,
    fallback: BridgeSourceKey,
) -> BridgeSourceKey {
    sources
        .values()
        .filter(|source| {
            surface_key(source) == surface
                && matches!(source.kind, BridgeSourceKind::Audio { .. })
                && source_is_effectively_playing(sources, source)
        })
        .map(|source| source.key)
        .min_by_key(|key| key.track)
        .unwrap_or(fallback)
}

/// The source a presentation hold on `surface` is reported against.
///
/// Audio is preferred because it is the surface clock; among equals the lowest track ID wins, so
/// consecutive holds on one surface always land on the same source.
pub(super) fn hold_source(
    sources: &HashMap<BridgeSourceKey, BridgeSource>,
    surface: BridgeSurfaceKey,
) -> Option<&BridgeSource> {
    sources
        .values()
        .filter(|source| surface_key(source) == surface)
        .min_by_key(|source| {
            (
                !matches!(source.kind, BridgeSourceKind::Audio { .. }),
                source.key.track,
            )
        })
}

fn source_is_effectively_playing(
    sources: &HashMap<BridgeSourceKey, BridgeSource>,
    source: &BridgeSource,
) -> bool {
    source.playing
        || matches!(
            source.kind,
            BridgeSourceKind::Audio {
                linked_video: Some(video),
                ..
            } if sources.get(&video).is_some_and(|video| video.playing)
        )
}
