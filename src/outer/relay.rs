//! Media assembly, per-track outer writers, and the recovery requests they raise.

use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use vivid_protocol::media::{self, AudioPacket, RasterDeltaOperation, VideoPacket};
use vivid_protocol::messages;
use vivid_protocol::track::TrackMode;
use vivid_sdk::presenter::KEYFRAME_REASON_DECODER_ERROR;
use vivid_sdk::presenter::{
    BridgeKeyframeRequest, BridgeSource, BridgeSourceKey, BridgeSourceKind, BridgeSurfaceKey,
};
use vivid_sdk::{ChannelEvent, RequestMetadata, TrackChannel};

use super::config::track_configuration;
use super::overlay::{OverlayLayouts, OverlayRevisions, RELAYED_REVISIONS};
use super::playback::EosState;
use super::{
    OUTER_TIMED_PREROLL_RECORDS, OuterBridge, OuterTrack, invalid_data, payload_u64, poisoned,
    surface_key,
};

impl OuterBridge {
    /// Whether the foreground worker may hand another media chunk to this source.
    ///
    /// Timed tracks forward one bounded pre-roll window before outer PLAY. This avoids filling the
    /// video socket while still supplying enough reordered video and linked audio to become
    /// output-ready. Completed socket writes replenish the handoff; the authenticated channel
    /// still enforces the presenter's byte/record credit without a per-packet ACK barrier.
    ///
    /// The window sizes that handshake and nothing more. Once the outer slot holds this track
    /// there is no readiness check left for media to run ahead of, the writer has already stopped
    /// pacing one record at a time, and the outer channel's own byte and record flow is the bound
    /// that belongs there. Keeping the cumulative wall past activation is what strands a seek
    /// taken while paused: bringing a replacement generation up to the target its producer
    /// published takes a whole key-frame interval of decoder references, nothing raises
    /// pre-roll ceiling for an activated track, and the pane holds the wrong picture until the
    /// producer resumes.
    ///
    /// This does not admit the tail of a stream that merely stopped. Whether a paused producer's
    /// records may move at all is its own question, answered by the caller that owns the
    /// producer's playback state, because the answer there is to leave them queued rather than to
    /// drop them.
    pub fn can_accept_media(&self, key: BridgeSourceKey) -> bool {
        if self.pending.contains_key(&key) {
            return true;
        }
        let Some(track) = self.tracks.get(&key) else {
            return true;
        };
        if track.eos || track.media_inflight >= OUTER_MEDIA_WRITER_QUEUE {
            return false;
        }
        track.mode == TrackMode::Live
            || track.playing
            || track.activated
            || (track.media_inflight == 0 && track.media_submitted < track.preplay_ceiling)
    }

    /// Accepts one chunk of an inner media record, relaying the record once it is complete.
    ///
    /// Returns `true` when this chunk completed the record and it was queued to the source's outer
    /// writer, and `false` while more chunks are expected. Partial records share a bounded budget, and
    /// one older than 30 seconds is discarded when the next chunk arrives. An invalid chunk discards
    /// only its own source's partial record.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotFound`] when the source has no outer track, and
    /// [`io::ErrorKind::InvalidData`] for a record type the track does not carry, chunk bounds or
    /// ordering that do not continue the partial record, an exhausted assembly budget, or media after
    /// EOS. Returns [`io::ErrorKind::WouldBlock`] when the outer writer's queue is full and
    /// [`io::ErrorKind::BrokenPipe`] when the writer has stopped.
    pub fn media_chunk(&mut self, chunk: MediaChunk) -> io::Result<bool> {
        let MediaChunk {
            delivery_id,
            source: key,
            record_type,
            offset,
            total,
            last,
            bytes,
        } = chunk;
        let total = usize::try_from(total).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("media body length does not fit usize: {error}"),
            )
        })?;
        self.pending
            .retain(|_, body| body.started.elapsed() < PENDING_TIMEOUT);
        let validation = (|| {
            let track = self
                .tracks
                .get(&key)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "outer track is missing"))?;
            let expected = match track.kind {
                BridgeSourceKind::Raster { .. } => record_type == messages::RASTER_FRAME,
                BridgeSourceKind::Image { .. } => record_type == messages::IMAGE_DATA,
                BridgeSourceKind::Video { .. } => record_type == messages::VIDEO_PACKET,
                BridgeSourceKind::Audio { .. } => record_type == messages::AUDIO_PACKET,
                BridgeSourceKind::VectorScene { .. } => matches!(
                    record_type,
                    messages::VECTOR_FRAME
                        | messages::VECTOR_ASSET
                        | messages::VECTOR_ASSET_RELEASE
                ),
            };
            let end = (offset as usize)
                .checked_add(bytes.len())
                .ok_or_else(|| invalid_data("media chunk length overflow"))?;
            if track.eos
                || !expected
                || total == 0
                || total > MAX_PENDING_BYTES
                || total > track.track.configuration()?.maximum_record_body as usize
                || bytes.is_empty()
                || end > total
                || last != (end == total)
            {
                return Err(invalid_data("invalid media chunk bounds or track state"));
            }
            Ok(())
        })();
        if let Err(error) = validation {
            self.pending.remove(&key);
            return Err(error);
        }
        let reserved: usize = self.pending.values().map(|body| body.total).sum();
        let assemblies = self.pending.len();
        let pending = match self.pending.entry(key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if offset != 0
                    || assemblies >= MAX_PENDING_RECORDS
                    || total > MAX_PENDING_BYTES.saturating_sub(reserved)
                {
                    return Err(invalid_data("media assembly sequence or budget exceeded"));
                }
                let mut body = Vec::new();
                body.try_reserve_exact(total).map_err(io::Error::other)?;
                entry.insert(PendingBody {
                    delivery_id,
                    started: Instant::now(),
                    record_type,
                    total,
                    received: 0,
                    bytes: body,
                })
            }
        };
        if pending.delivery_id != delivery_id
            || pending.record_type != record_type
            || pending.total != total
            || pending.received != offset as usize
        {
            self.pending.remove(&key);
            return Err(invalid_data("media chunk sequence gap"));
        }
        pending.received = pending
            .received
            .checked_add(bytes.len())
            .ok_or_else(|| invalid_data("media chunk length overflow"))?;
        if pending.received > pending.total {
            self.pending.remove(&key);
            return Err(invalid_data("media chunks exceed declared body length"));
        }
        pending.bytes.extend_from_slice(&bytes);
        if !last {
            return Ok(false);
        }
        let pending = self
            .pending
            .remove(&key)
            .ok_or_else(|| invalid_data("missing pending media body"))?;
        if pending.received != pending.total {
            return Err(invalid_data("incomplete media body"));
        }
        let track = self
            .tracks
            .get_mut(&key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "outer track is missing"))?;
        if track.eos {
            return Err(invalid_data("media arrived after outer channel EOS"));
        }
        track
            .media_sender
            .try_send(OuterMediaCommand::Write {
                delivery_id,
                record_type: pending.record_type,
                body: pending.bytes,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "outer source media writer queue is full",
                ),
                mpsc::TrySendError::Disconnected(_) => io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "outer source media writer stopped",
                ),
            })?;
        track.media_inflight = track.media_inflight.saturating_add(1);
        track.media_submitted = track.media_submitted.saturating_add(1);
        Ok(true)
    }

    /// Takes the outcomes of the records the outer writers finished since the last call.
    ///
    /// A failed delivery also queues the source for [`take_source_losses`](Self::take_source_losses),
    /// or for [`take_full_frame_requests`](Self::take_full_frame_requests) when a full frame recovers it.
    pub fn take_media_completions(&mut self) -> Vec<DeliveryOutcome> {
        self.completions
            .extend(self.writer_completions_rx.try_iter());
        for completion in &mut self.completions {
            let current = self
                .tracks
                .get_mut(&completion.source)
                .filter(|track| track.writer_id == completion.writer_id);
            let Some(track) = current else {
                // A late retained hydration from a replaced writer must not be attributed to a
                // new track that happens to reuse the same SDK object ID.
                if completion.delivery_id == 0 {
                    completion.delivered = false;
                }
                continue;
            };
            if !completion.eos {
                track.media_inflight = track.media_inflight.saturating_sub(1);
                track.media_completed = track.media_completed.saturating_add(1);
            }
            if completion.needs_full_frame {
                self.full_frames.insert(completion.source);
            } else if !completion.delivered {
                self.losses.insert(completion.source);
            }
        }
        // EOS admission is not playback completion. The bounded position observer publishes
        // completion later; synchronously draining here deadlocks the bridge when audio is paused.
        self.completions
            .drain(..)
            .filter(|value| !value.eos)
            .map(|value| DeliveryOutcome {
                delivery_id: value.delivery_id,
                delivered: value.delivered,
                sequence: value.sequence,
                object_id: value.object_id,
            })
            .collect()
    }

    /// The inner source whose outer track has object ID `object_id`.
    pub fn source_for_outer_object(&self, object_id: u64) -> Option<BridgeSourceKey> {
        self.tracks
            .iter()
            .find_map(|(key, track)| (track.track.id() == object_id).then_some(*key))
    }

    /// Takes the keyframe requests the outer tracks raised since the last call.
    pub fn take_keyframe_requests(&mut self) -> Vec<BridgeKeyframeRequest> {
        let keys = self.tracks.keys().copied().collect::<Vec<_>>();
        for key in keys {
            let _ = self.poll_channel_events(key);
        }
        std::mem::take(&mut self.keyframes)
    }

    /// Takes the sources that need a full raster frame, ordered by inner identity.
    pub fn take_full_frame_requests(&mut self) -> Vec<BridgeSourceKey> {
        let keys = self.tracks.keys().copied().collect::<Vec<_>>();
        for key in keys {
            let _ = self.poll_channel_events(key);
        }
        let mut values = self.full_frames.drain().collect::<Vec<_>>();
        values.sort_by_key(|key| (key.producer, key.context, key.surface, key.track));
        values
    }

    /// Takes the sources whose outer track was lost or failed a delivery, ordered by inner identity.
    pub fn take_source_losses(&mut self) -> Vec<BridgeSourceKey> {
        let mut values = self.losses.drain().collect::<Vec<_>>();
        values.sort_by_key(|key| (key.producer, key.context, key.surface, key.track));
        values
    }

    pub(super) fn create_outer_track(&mut self, source: &BridgeSource) -> io::Result<()> {
        if let Some(track) = self.unfinished_tracks.get(&source.key).cloned() {
            self.session
                .destroy_track(&track, &RequestMetadata::default())?;
            self.unfinished_tracks.remove(&source.key);
        }
        let surface_key = surface_key(source);
        let surface = self
            .surfaces
            .get(&surface_key)
            .ok_or_else(|| invalid_data("outer surface was not created"))?
            .clone();
        let configuration = track_configuration(&self.session, &surface, source)?;
        let mode = configuration.mode;
        let mut probe = configuration.clone();
        probe.track_id = 0;
        if !self.session.probe_track(&probe)?.supported {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "outer presenter rejected relayed track configuration",
            ));
        }
        let track = self
            .session
            .create_track(configuration, &RequestMetadata::default())?;
        self.unfinished_tracks.insert(source.key, track.clone());
        let mut opened_channel = None;
        let setup = (|| {
            let channel = Arc::new(self.session.open_track_channel(&track)?);
            opened_channel = Some(Arc::clone(&channel));
            self.next_writer_id = self
                .next_writer_id
                .checked_add(1)
                .ok_or_else(|| invalid_data("outer media writer identity exhausted"))?;
            let writer_id = self.next_writer_id;
            let (media_sender, media_receiver) = mpsc::sync_channel(OUTER_MEDIA_WRITER_QUEUE);
            let slot_activated = Arc::new(AtomicBool::new(false));
            let writer = OuterMediaWriter {
                overlay_revisions: Arc::clone(&self.overlay_revisions),
                overlay_layouts: Arc::clone(&self.overlay_layouts),
                writer_id,
                key: source.key,
                object_id: track.id(),
                channel: Arc::clone(&channel),
                slot_activated: Arc::clone(&slot_activated),
                kind: source.kind.clone(),
                outer_delta_operations: track.delta_operation_limit()?,
                next_media_id: 0,
                outer_epoch: 0,
                inner_epoch: 0,
                last_raster_id: 0,
                last_inner_raster_id: 0,
                awaiting_full_frame: false,
                needs_full_frame: false,
            };
            let completions = self.writer_completions_tx.clone();
            thread::Builder::new()
                .name(format!("vivid-outer-media-{}", track.id()))
                .spawn(move || run_outer_media_writer(writer, &media_receiver, &completions))?;
            Ok((channel, media_sender, slot_activated, writer_id))
        })();
        let (channel, media_sender, slot_activated, writer_id) = match setup {
            Ok(value) => value,
            Err(error) => {
                if let Some(channel) = opened_channel {
                    let _ = channel.close();
                }
                if self
                    .session
                    .destroy_track(&track, &RequestMetadata::default())
                    .is_ok()
                {
                    self.unfinished_tracks.remove(&source.key);
                }
                return Err(error);
            }
        };
        self.unfinished_tracks.remove(&source.key);
        self.tracks.insert(
            source.key,
            OuterTrack {
                writer_id,
                surface_key,
                track,
                channel,
                media_sender,
                slot_activated,
                kind: source.kind.clone(),
                decoder_reset_serial: source.decoder_reset_serial,
                mode,
                activated: false,
                output_ready: false,
                media_inflight: 0,
                media_submitted: 0,
                media_completed: 0,
                preplay_ceiling: OUTER_TIMED_PREROLL_RECORDS,
                resume_after_submission: None,
                published_play_request: None,
                playing: false,
                eos_requested: false,
                eos: false,
                reported_eos_state: EosState::NotReceived,
                target_picture_ready: false,
            },
        );
        Ok(())
    }

    pub(super) fn remove_track(&mut self, key: BridgeSourceKey) -> io::Result<()> {
        self.pending.remove(&key);
        if let Some(track) = self.tracks.get(&key) {
            let _ = track.channel.close();
            self.session
                .destroy_track(&track.track, &RequestMetadata::default())?;
            let surface_key = track.surface_key;
            self.tracks.remove(&key);
            self.surface_refresh.insert(surface_key);
            if let Some(surface) = self.surfaces.get(&surface_key) {
                // Destroying a track that occupied a slot advances the presenter's surface
                // revision. Refresh the SDK handle before a replacement ACTIVATE_TRACK; keeping
                // the old revision makes every otherwise-ready atomic activation fail forever.
                self.session.query_surface(surface)?;
            }
            self.surface_refresh.remove(&surface_key);
        }
        Ok(())
    }

    fn poll_channel_events(&mut self, key: BridgeSourceKey) -> io::Result<()> {
        let Some(track) = self.tracks.get(&key) else {
            return Ok(());
        };
        while let Some(event) = track.channel.take_event()? {
            match event {
                ChannelEvent::NeedKeyframe(payload) => {
                    self.keyframes.push(BridgeKeyframeRequest {
                        source: key,
                        minimum_epoch: payload_u64(&payload, 4)
                            .and_then(|value| u32::try_from(value).ok()),
                        reason: payload_u64(&payload, 5).unwrap_or(KEYFRAME_REASON_DECODER_ERROR),
                    });
                }
                ChannelEvent::NeedFullFrame(_) => {
                    self.full_frames.insert(key);
                }
                ChannelEvent::Error(_) => {
                    self.losses.insert(key);
                }
            }
        }
        Ok(())
    }
}

/// Per-track handoff between the foreground bridge and the SDK's blocking flow/rate admission.
///
/// The virtual presenter exposes at most eight unacknowledged records per track, so this remains
/// bounded above that protocol window without becoming another large media reservoir.
pub(super) const OUTER_MEDIA_WRITER_QUEUE: usize = 32;

pub(super) const MAX_PENDING_BYTES: usize = vivid_protocol::HARD_MAX_RECORD_BODY as usize;

const MAX_PENDING_RECORDS: usize = 256;

pub(super) const PENDING_TIMEOUT: Duration = Duration::from_secs(30);

/// One chunk of an inner media record, as the foreground worker hands it to the bridge.
///
/// A record arrives as consecutive chunks of one delivery. The first starts at `offset` zero, each
/// continues where the previous ended, and the one with `last` set ends exactly at `total`.
pub struct MediaChunk {
    /// Identity of the record delivery this chunk belongs to.
    pub delivery_id: u64,
    /// The inner track the record was written to.
    pub source: BridgeSourceKey,
    /// The record type, which must match the track's kind.
    pub record_type: u16,
    /// Byte offset of this chunk within the record body.
    pub offset: u32,
    /// Length of the complete record body.
    pub total: u32,
    /// Whether this chunk completes the record.
    pub last: bool,
    /// The chunk's bytes.
    pub bytes: Vec<u8>,
}

impl std::fmt::Debug for MediaChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaChunk")
            .field("delivery_id", &self.delivery_id)
            .field("source", &self.source)
            .field("record_type", &self.record_type)
            .field("offset", &self.offset)
            .field("total", &self.total)
            .field("last", &self.last)
            .field("bytes", &self.bytes.len())
            .finish()
    }
}

/// How the outer writer finished one complete relayed record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryOutcome {
    /// The delivery the record arrived in; zero for a retained replay with no inner delivery.
    pub delivery_id: u64,
    /// Whether the outer track accepted the record.
    pub delivered: bool,
    /// The outer media sequence the record was sent as, or zero when it was not delivered.
    pub sequence: u64,
    /// The outer track object the record was written to.
    pub object_id: u64,
}

pub(super) struct OuterMediaWriter {
    pub(super) overlay_revisions: OverlayRevisions,
    pub(super) overlay_layouts: Arc<Mutex<OverlayLayouts>>,
    pub(super) writer_id: u64,
    pub(super) key: BridgeSourceKey,
    pub(super) object_id: u64,
    pub(super) channel: Arc<TrackChannel>,
    /// Whether the outer surface slot now holds this track.
    ///
    /// Until it does, the writer keeps at most one record outstanding so the independently
    /// serviced control connection cannot run its readiness checks ahead of media the outer
    /// presenter has not consumed. Afterwards that pacing costs one presenter round trip per
    /// record and buys nothing, so the channel's own byte and record flow becomes the only bound.
    pub(super) slot_activated: Arc<AtomicBool>,
    pub(super) kind: BridgeSourceKind,
    /// Raster delta operations the outer presenter granted this track, zero when it granted none.
    ///
    /// The inner grant says only what the nested producer was allowed to send. Forwarding is
    /// governed by what the outer track will accept.
    pub(super) outer_delta_operations: u32,
    pub(super) next_media_id: u64,
    pub(super) outer_epoch: u32,
    pub(super) inner_epoch: u32,
    pub(super) last_raster_id: u64,
    pub(super) last_inner_raster_id: u64,
    pub(super) awaiting_full_frame: bool,
    pub(super) needs_full_frame: bool,
}

pub(super) enum OuterMediaCommand {
    Write {
        delivery_id: u64,
        record_type: u16,
        body: Vec<u8>,
    },
    Eos,
}

pub(super) struct PendingBody {
    pub(super) delivery_id: u64,
    pub(super) started: Instant,
    record_type: u16,
    pub(super) total: usize,
    pub(super) received: usize,
    pub(super) bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MediaCompletion {
    pub(super) writer_id: u64,
    pub(super) source: BridgeSourceKey,
    pub(super) delivery_id: u64,
    pub(super) delivered: bool,
    pub(super) sequence: u64,
    pub(super) object_id: u64,
    pub(super) needs_full_frame: bool,
    pub(super) eos: bool,
}

impl OuterMediaWriter {
    pub(super) fn forward_media(&mut self, record_type: u16, body: &[u8]) -> io::Result<u64> {
        match record_type {
            messages::VECTOR_ASSET => {
                let asset = vivid_protocol::vector::ImageAsset::decode(body)
                    .map_err(|error| invalid_data(error.0))?;
                // Each outer track is dedicated to this complete inner source identity, so its
                // freshly established asset namespace can use the validated ascending IDs.
                self.channel.send_vector_asset(&asset)
            }
            messages::VECTOR_ASSET_RELEASE => {
                let release = vivid_protocol::vector::AssetRelease::decode(body)
                    .map_err(|error| invalid_data(error.0))?;
                self.channel.release_vector_asset(release.id)
            }
            messages::VECTOR_FRAME => {
                let mut frame = vivid_protocol::vector::Frame::decode(body)
                    .map_err(|error| invalid_data(error.0))?;
                let mut canvas = vivid_protocol::vector::Canvas::new();
                let key = BridgeSurfaceKey {
                    producer: self.key.producer,
                    context: self.key.context,
                    surface: self.key.surface,
                };
                {
                    let layouts = self
                        .overlay_layouts
                        .lock()
                        .map_err(poisoned("overlay layouts"))?;
                    for command in frame.canvas.commands() {
                        let mut command = command.clone();
                        if let vivid_protocol::vector::Command::TextLayout { layout, .. } =
                            &mut command
                        {
                            *layout = *layouts
                                .ids
                                .get(&(key, *layout))
                                .ok_or_else(|| invalid_data("missing outer text layout"))?;
                        }
                        canvas
                            .push(command)
                            .map_err(|error| invalid_data(error.0))?;
                    }
                }
                frame.canvas = canvas;
                let inner_revision = frame.revision;
                let (epoch, revision) = next_outer_identity(self, frame.epoch)?;
                frame.epoch = epoch;
                frame.revision = revision;
                let mut mappings = self.overlay_revisions.lock()?;
                let revisions = &mut mappings.entry(key).or_default().revisions;
                if revisions.len() == RELAYED_REVISIONS {
                    revisions.pop_front();
                }
                revisions.push_back((revision, inner_revision));
                drop(mappings);
                self.channel.send_vector(&frame)
            }
            messages::IMAGE_DATA => self.channel.send_image(body),
            messages::VIDEO_PACKET => {
                let packet = media::parse_video_packet(body)?;
                let (epoch, id) = next_outer_identity(self, packet.epoch)?;
                self.channel.send_video(VideoPacket {
                    epoch,
                    packet_id: id,
                    pts_us: packet.pts_us,
                    dts_us: packet.dts_us,
                    duration_us: packet.duration_us,
                    key: packet.flags & media::VIDEO_PACKET_KEY != 0,
                    data: packet.data,
                })
            }
            messages::AUDIO_PACKET => {
                let packet = media::parse_audio_packet(body)?;
                let (epoch, id) = next_outer_identity(self, packet.epoch)?;
                self.channel.send_audio(AudioPacket {
                    epoch,
                    packet_id: id,
                    pts_us: packet.pts_us,
                    dts_us: packet.dts_us,
                    duration_us: packet.duration_us,
                    trim_start_samples: packet.trim_start_samples,
                    trim_end_samples: packet.trim_end_samples,
                    data: packet.data,
                })
            }
            messages::RASTER_FRAME => {
                let flags = body
                    .get(4..8)
                    .and_then(|value| value.try_into().ok())
                    .map(u32::from_be_bytes)
                    .ok_or_else(|| invalid_data("raster header is truncated"))?;
                if flags & media::RASTER_FRAME_DELTA == 0 {
                    let frame = media::parse_full_raster_frame(body)?;
                    let pixels = media::decode_raster_pixels(frame)?;
                    let (epoch, id) = next_outer_identity(self, frame.epoch)?;
                    // The outer track mirrors the nested track's compression, so a frame the
                    // producer compressed must not leave here raw. Relaying the decoded pixels
                    // uncompressed put a full framebuffer on the outer link for every document
                    // page turn, which is invisible over a local socket and dominates a forwarded
                    // one. The adaptive send keeps the raw form whenever it is the smaller of the
                    // two, so the record can never exceed the raw-framebuffer body claim.
                    let sequence = self.channel.send_raster_adaptive(epoch, id, &pixels)?;
                    self.last_raster_id = id;
                    self.last_inner_raster_id = frame.frame_id;
                    self.awaiting_full_frame = false;
                    Ok(sequence)
                } else {
                    let (width, height, limit) = if let BridgeSourceKind::Raster {
                        width,
                        height,
                        delta_operation_limit: Some(limit),
                        ..
                    } = &self.kind
                    {
                        (*width, *height, *limit)
                    } else {
                        self.needs_full_frame = true;
                        self.awaiting_full_frame = true;
                        return Err(invalid_data(
                            "raster delta arrived for a non-delta outer track",
                        ));
                    };
                    let frame = media::parse_delta_raster_frame(body, width, height, limit)?;
                    if self.awaiting_full_frame
                        || self.last_raster_id == 0
                        || frame.epoch != self.inner_epoch
                        || frame.base_frame_id != self.last_inner_raster_id
                    {
                        self.needs_full_frame = true;
                        self.awaiting_full_frame = true;
                        return Err(invalid_data("outer raster delta has no reusable base"));
                    }
                    // An outer presenter that granted fewer delta operations than this frame uses
                    // cannot receive it at all. Ask the nested producer for a full frame instead:
                    // sending the delta anyway fails the record, and a failure that is not a
                    // full-frame request retires the writer and strands the source for good.
                    if frame.operations.len() > self.outer_delta_operations as usize {
                        self.needs_full_frame = true;
                        self.awaiting_full_frame = true;
                        return Err(invalid_data(
                            "outer track granted too few raster delta operations",
                        ));
                    }
                    let (epoch, id) = next_outer_identity(self, frame.epoch)?;
                    let operations = frame
                        .operations
                        .iter()
                        .map(parsed_delta_operation)
                        .collect::<Vec<_>>();
                    let sequence = self.channel.send_raster_delta_adaptive(
                        epoch,
                        id,
                        self.last_raster_id,
                        frame.pts_us,
                        frame.duration_us,
                        &operations,
                    )?;
                    self.last_raster_id = id;
                    self.last_inner_raster_id = frame.frame_id;
                    Ok(sequence)
                }
            }
            _ => Err(invalid_data("unsupported relayed media record type")),
        }
    }
}

pub(super) fn run_outer_media_writer(
    mut writer: OuterMediaWriter,
    receiver: &mpsc::Receiver<OuterMediaCommand>,
    completions: &mpsc::Sender<MediaCompletion>,
) {
    while let Ok(command) = receiver.recv() {
        let (delivery_id, eos, result) = match command {
            OuterMediaCommand::Write {
                delivery_id,
                record_type,
                body,
            } => {
                writer.needs_full_frame = false;
                let result = writer
                    .forward_media(record_type, &body)
                    .and_then(|sequence| {
                        if !matches!(
                            writer.kind,
                            BridgeSourceKind::Video { .. } | BridgeSourceKind::Audio { .. }
                        ) && !writer.slot_activated.load(Ordering::Acquire)
                        {
                            writer.channel.wait_for_reusable_media_capacity()?;
                        }
                        Ok(sequence)
                    });
                (delivery_id, false, result)
            }
            OuterMediaCommand::Eos => (0, true, writer.channel.eos()),
        };
        if writer.needs_full_frame {
            writer.awaiting_full_frame = true;
        }
        let delivered = result.is_ok();
        let completion = MediaCompletion {
            writer_id: writer.writer_id,
            source: writer.key,
            delivery_id,
            delivered,
            sequence: result.unwrap_or(0),
            object_id: writer.object_id,
            needs_full_frame: writer.needs_full_frame,
            eos,
        };
        if completions.send(completion).is_err() || eos || (!delivered && !writer.needs_full_frame)
        {
            break;
        }
    }
}

fn next_outer_identity(track: &mut OuterMediaWriter, inner_epoch: u32) -> io::Result<(u32, u64)> {
    if track.outer_epoch == 0 {
        track.outer_epoch = 1;
        track.inner_epoch = inner_epoch;
    } else if inner_epoch > track.inner_epoch {
        track.outer_epoch = track
            .outer_epoch
            .checked_add(1)
            .ok_or_else(|| io::Error::other("outer media epoch exhausted"))?;
        track.inner_epoch = inner_epoch;
        track.last_raster_id = 0;
    } else if inner_epoch < track.inner_epoch {
        return Err(invalid_data("inner media epoch moved backward"));
    }
    track.next_media_id = track
        .next_media_id
        .checked_add(1)
        .ok_or_else(|| io::Error::other("outer media ID exhausted"))?;
    Ok((track.outer_epoch, track.next_media_id))
}

fn parsed_delta_operation<'a>(
    operation: &'a media::ParsedRasterDeltaOperation<'a>,
) -> RasterDeltaOperation<'a> {
    match operation {
        media::ParsedRasterDeltaOperation::Overwrite {
            x,
            y,
            width,
            height,
            rgba,
        } => RasterDeltaOperation::Overwrite {
            x: *x,
            y: *y,
            width: *width,
            height: *height,
            rgba: match rgba {
                Cow::Borrowed(value) => value,
                Cow::Owned(value) => value.as_slice(),
            },
        },
        media::ParsedRasterDeltaOperation::Copy {
            destination_x,
            destination_y,
            width,
            height,
            source_x,
            source_y,
        } => RasterDeltaOperation::Copy {
            destination_x: *destination_x,
            destination_y: *destination_y,
            width: *width,
            height: *height,
            source_x: *source_x,
            source_y: *source_y,
        },
    }
}
