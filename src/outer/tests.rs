use super::config::{SLOT_RASTER, intersect_surface_policy, track_configuration};
use super::overlay::RelayedScenes;
use super::playback::default_play_request;
use super::relay::{OUTER_MEDIA_WRITER_QUEUE, OuterMediaWriter, run_outer_media_writer};
use super::*;
use std::time::Duration;
use vivid_protocol::registry;

use vivid_protocol::media::{self, AudioPacket, RasterDeltaOperation};
use vivid_protocol::messages::{self, LaneClass};
use vivid_protocol::track::{KindConfiguration, RasterConfiguration, TrackConfiguration};
use vivid_sdk::{
    CoordinateModel, ProducerConfig, SurfaceDefinition, SurfaceDescriptor, SurfaceRole,
};

/// One playing source on surface `(1, 1, 1)`; audio unless `video` is set.
fn playing_source(track: u64, video: bool) -> BridgeSource {
    BridgeSource {
        decoder_reset_serial: 1,
        key: BridgeSourceKey {
            producer: 1,
            context: 1,
            surface: 1,
            track,
        },
        kind: if video {
            BridgeSourceKind::Video {
                codec: "h264".into(),
                packetization: "annexb".into(),
                extradata: Vec::new(),
                width: 16,
                height: 16,
                profile: 0,
                level: 0,
                bitrate: 0,
                color_primaries: 0,
                transfer: 0,
                matrix: 0,
                range: 0,
                sar_num: 1,
                sar_den: 1,
                max_access_unit_bytes: 256,
                codec_string: None,
                decoder_config: None,
            }
        } else {
            BridgeSourceKind::Audio {
                linked_video: None,
                codec: "pcm_s16le".into(),
                packetization: "pcm-packet-v1".into(),
                extradata: Vec::new(),
                sample_rate: 48_000,
                channels: 2,
                channel_mask: 3,
                bitrate: 1_536_000,
                max_access_unit_bytes: 256,
                codec_string: None,
            }
        },
        live: false,
        active: true,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: true,
        play_request: default_play_request(),
        eos_epoch: None,
        causation_id: None,
    }
}

/// PLAY and the PAUSE that later stops the same surface group must name the same clock, and holds
/// on one surface must land on one source, however the source map happens to iterate.
#[test]
fn clock_and_hold_choices_do_not_depend_on_map_order() {
    let surface = BridgeSurfaceKey {
        producer: 1,
        context: 1,
        surface: 1,
    };
    let video = playing_source(1, true);
    for _ in 0..64 {
        // Each map gets a fresh random hasher, so iteration order differs between rounds.
        let sources = [
            playing_source(9, false),
            playing_source(5, false),
            video.clone(),
        ]
        .into_iter()
        .map(|source| (source.key, source))
        .collect::<HashMap<_, _>>();
        assert_eq!(
            preferred_surface_clock(&sources, surface, video.key).track,
            5
        );
        assert_eq!(
            hold_source(&sources, surface).map(|source| source.key.track),
            Some(5)
        );
    }
}

#[test]
fn a_scene_bound_request_waits_for_its_own_owners_presentation() {
    // Two producers reusing the same local context and surface IDs.
    let first = BridgeSurfaceKey {
        producer: 1,
        context: 1,
        surface: 1,
    };
    let second = BridgeSurfaceKey {
        producer: 2,
        ..first
    };
    let scenes = RelayedScenes::default();
    for key in [first, second] {
        scenes
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .revisions
            .extend([(1, 10), (2, 11)]);
    }
    let short = Duration::from_millis(20);

    // Another owner's presentation of the same numbers releases nothing.
    scenes.record_presented(second, 1).unwrap();
    let timed_out = scenes.wait_presented(first, 10, short).unwrap_err();
    assert_eq!(timed_out.kind(), io::ErrorKind::TimedOut);
    assert_eq!(scenes.wait_presented(second, 10, short).unwrap(), 1);

    // A waiter is released by its own presentation, not by polling.
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| scenes.wait_presented(first, 11, Duration::from_secs(5)));
        std::thread::sleep(short);
        scenes.record_presented(first, 2).unwrap();
        assert_eq!(waiter.join().unwrap().unwrap(), 2);
    });

    // Once a later scene is shown, an earlier one never will be, and an inner revision the
    // relay skipped never will be either.
    let replaced = scenes.wait_presented(first, 10, Duration::from_secs(5));
    assert_eq!(replaced.unwrap_err().kind(), io::ErrorKind::InvalidData);
    let skipped = scenes.wait_presented(first, 9, Duration::from_secs(5));
    assert_eq!(skipped.unwrap_err().kind(), io::ErrorKind::InvalidData);

    // Retiring one owner's window releases its waiter and leaves the other owner's intact.
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| scenes.wait_presented(first, 12, Duration::from_secs(5)));
        std::thread::sleep(short);
        scenes.retire(Some(first));
        assert_eq!(
            waiter.join().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    });
    assert_eq!(scenes.wait_presented(second, 10, short).unwrap(), 1);
}

#[test]
fn native_outer_config_pins_lane_fallbacks_to_its_control_presenter() {
    let endpoint = "tcp:127.0.0.1:12345".to_owned();
    let config = producer_config(
        Some(endpoint.clone()),
        None,
        None,
        &Secret32::new([7; 32]),
        registry::TERMINAL_SURFACE,
    )
    .unwrap();

    assert_eq!(config.endpoint_control.as_deref(), Some(endpoint.as_str()));
    assert_eq!(
        config.endpoint_interactive.as_deref(),
        Some(endpoint.as_str())
    );
    assert_eq!(config.endpoint_realtime.as_deref(), Some(endpoint.as_str()));
    assert_eq!(config.endpoint_bulk.as_deref(), Some(endpoint.as_str()));
}

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
impl vivid_sdk::presenter::PresenterListener for TestSocketListener {
    fn endpoint(&self) -> String {
        format!("unix:{}", self.endpoint.display())
    }

    fn accept(&self) -> io::Result<vivid_sdk::presenter::Transport> {
        let (stream, _) = self.inner.accept()?;
        stream.set_nonblocking(false)?;
        let reader = stream.try_clone()?;
        let cancel_reader = reader.try_clone()?;
        let cancel_writer = stream.try_clone()?;
        let cancel = vivid_sdk::presenter::ConnectionCancel::new(move || {
            let _ = cancel_reader.shutdown(std::net::Shutdown::Both);
            let _ = cancel_writer.shutdown(std::net::Shutdown::Both);
        });
        Ok(vivid_sdk::presenter::Transport::new(
            Box::new(reader),
            Box::new(stream),
            cancel,
            Arc::new(|_: Option<Duration>| Ok(())),
        ))
    }
}

#[test]
fn recovery_reset_is_scoped_to_the_complete_surface_owner() {
    let session = vivid_sdk::Session::connect(ProducerConfig::offline()).unwrap();
    let mut bridge = OuterBridge::from_session(
        session,
        Secret32::new([7; 32]),
        Route::Native {
            control: String::new(),
            realtime: None,
            bulk: None,
        },
        DisplayMetrics::default(),
    )
    .unwrap();
    let surface = |producer| BridgeSurface {
        overlay_window: None,
        overlay_layouts: Vec::new(),
        key: BridgeSurfaceKey {
            producer,
            context: 7,
            surface: 5,
        },
        logical_width: 16,
        logical_height: 16,
        capture_policy: 0,
        descriptor: vivid_sdk::presenter::BridgeSourceDescriptor {
            role: 1,
            title: format!("owner {producer}"),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let video = |producer| BridgeSource {
        decoder_reset_serial: 1,
        key: BridgeSourceKey {
            producer,
            context: 7,
            surface: 5,
            track: 3,
        },
        kind: BridgeSourceKind::Video {
            codec_string: None,
            decoder_config: None,
            codec: "h264".into(),
            packetization: "h264-annexb-au-v1".into(),
            extradata: Vec::new(),
            width: 16,
            height: 16,
            profile: 0,
            level: 0,
            bitrate: 0,
            color_primaries: 1,
            transfer: 1,
            matrix: 1,
            range: 1,
            sar_num: 1,
            sar_den: 1,
            max_access_unit_bytes: 1024,
        },
        live: false,
        active: true,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: false,
        eos_epoch: None,
        causation_id: None,
        play_request: default_play_request(),
    };
    let node = |producer, x| BridgeNode {
        producer,
        node: 9,
        fragment: 0,
        surface: BridgeSurfaceKey {
            producer,
            context: 7,
            surface: 5,
        },
        x,
        y: 0,
        width: 16,
        height: 16,
        z_index: 0,
        visible: true,
        clip: vivid_sdk::presenter::BridgeClipRect {
            x: 0,
            y: 0,
            width: 16,
            height: 16,
        },
    };
    let surfaces = vec![surface(11), surface(12)];
    let mut sources = vec![video(11), video(12)];
    let nodes = vec![node(11, 0), node(12, 20)];
    bridge.rebuild(&surfaces, &sources, &nodes).unwrap();

    let recovering_key = sources[0].key;
    let unrelated_key = sources[1].key;
    let recovering_track = bridge.outer_track_id(recovering_key).unwrap();
    let unrelated_track = bridge.outer_track_id(unrelated_key).unwrap();
    let unrelated_surface = bridge.outer_surface_id(surfaces[1].key).unwrap();
    let recovering_node = bridge.nodes.get(&(11, 9, 0)).unwrap().clone();
    let unrelated_node = bridge.nodes.get(&(12, 9, 0)).unwrap().clone();

    sources[1].decoder_reset_serial += 1;
    bridge
        .acknowledge_outer_requested_decoder_reset(unrelated_key, sources[1].decoder_reset_serial)
        .unwrap();
    assert!(
        bridge
            .rebuild(&surfaces, &sources, &nodes)
            .unwrap()
            .is_empty(),
        "an acknowledged outer-requested reset recreated its existing decoder"
    );
    assert_eq!(bridge.outer_track_id(unrelated_key), Some(unrelated_track));

    bridge
        .rebuild_resetting(
            &surfaces,
            &sources,
            &nodes,
            &HashSet::from([recovering_key]),
        )
        .unwrap();

    assert_ne!(
        bridge.outer_track_id(recovering_key),
        Some(recovering_track),
        "the recovering owner's saturated timed track was not replaced"
    );
    assert_eq!(bridge.outer_track_id(unrelated_key), Some(unrelated_track));
    assert_eq!(
        bridge.outer_surface_id(surfaces[1].key),
        Some(unrelated_surface)
    );
    assert_eq!(bridge.nodes.get(&(11, 9, 0)), Some(&recovering_node));
    assert_eq!(bridge.nodes.get(&(12, 9, 0)), Some(&unrelated_node));

    let mut updated_nodes = nodes;
    updated_nodes[1].x = 24;
    bridge.rebuild(&surfaces, &sources, &updated_nodes).unwrap();
    assert_eq!(bridge.outer_track_id(unrelated_key), Some(unrelated_track));
    assert_eq!(bridge.nodes.get(&(11, 9, 0)), Some(&recovering_node));
    assert_eq!(
        bridge.nodes.get(&(12, 9, 0)).unwrap().0,
        unrelated_node.0,
        "the unrelated owner's next valid update replaced its outer node identity"
    );
    assert_ne!(bridge.nodes.get(&(12, 9, 0)).unwrap().1, unrelated_node.1);
}

#[test]
fn recovery_rebase_uses_effectively_playing_linked_audio_as_surface_clock() {
    let video_key = BridgeSourceKey {
        producer: 11,
        context: 7,
        surface: 5,
        track: 3,
    };
    let audio_key = BridgeSourceKey {
        track: 4,
        ..video_key
    };
    let request = BridgePlayRequest {
        start_pts_us: 8_033_333,
        minimum_buffer_us: 33_000,
        maximum_latency_us: 500_000,
        rate_32_32: 1_i64 << 32,
        late_policy: 1,
        loop_count: 0,
        start_policy: 1,
        hold_serial: None,
    };
    let video = BridgeSource {
        decoder_reset_serial: 1,
        key: video_key,
        kind: BridgeSourceKind::Video {
            codec_string: None,
            decoder_config: None,
            codec: "h264".into(),
            packetization: "h264-annexb-au-v1".into(),
            extradata: Vec::new(),
            width: 16,
            height: 16,
            profile: 0,
            level: 0,
            bitrate: 0,
            color_primaries: 1,
            transfer: 1,
            matrix: 1,
            range: 1,
            sar_num: 1,
            sar_den: 1,
            max_access_unit_bytes: 1024,
        },
        live: false,
        active: true,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: true,
        eos_epoch: None,
        causation_id: None,
        play_request: request,
    };
    let audio = BridgeSource {
        decoder_reset_serial: 1,
        key: audio_key,
        kind: BridgeSourceKind::Audio {
            codec_string: None,
            linked_video: Some(video_key),
            codec: "pcm_s16le".into(),
            packetization: "pcm-packet-v1".into(),
            extradata: Vec::new(),
            sample_rate: 48_000,
            channels: 2,
            channel_mask: 3,
            bitrate: 0,
            max_access_unit_bytes: 4096,
        },
        live: false,
        active: true,
        audio_gain: Some(AudioGain::UNITY.raw()),
        capture_policy: 0,
        descriptor: None,
        // This is the real nested recovery shape: the video owns authoritative PLAY while
        // linked audio participates through the active surface slot.
        playing: false,
        eos_epoch: None,
        causation_id: None,
        play_request: BridgePlayRequest {
            minimum_buffer_us: 0,
            ..request
        },
    };
    let sources = HashMap::from([(video_key, video), (audio_key, audio)]);

    assert_eq!(
        preferred_surface_clock(&sources, surface_key(&sources[&video_key]), video_key),
        audio_key,
        "recovery PLAY must configure and restart the physical audio output"
    );
}

#[test]
#[cfg(unix)]
fn paused_active_surface_waits_for_one_fresh_audio_submission() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("pause-resume.sock");
    let listener = match TestSocketListener::bind(socket.clone()) {
        Ok(listener) => listener,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping pause/resume socket test: {error}");
            return;
        }
        Err(error) => panic!("pause/resume listener failed: {error}"),
    };
    let presenter = vivid_sdk::presenter::VirtualVivid::start_eventless(
        listener,
        vivid_sdk::presenter::MediaConfig::default(),
    )
    .unwrap();
    presenter.update_metrics(7, 80, 24, (8, 16));
    let secret = presenter.issue_pane_capability(7).unwrap();
    let mut bridge = OuterBridge::builder(
        Secret32::from_hex(&secret).unwrap(),
        DisplayMetrics::default(),
    )
    .control_endpoint(format!("unix:{}", socket.display()))
    .build()
    .unwrap();

    let audio_key = BridgeSourceKey {
        producer: 3,
        context: 1,
        surface: 7,
        track: 11,
    };
    let surface = BridgeSurface {
        overlay_window: None,
        overlay_layouts: Vec::new(),
        key: BridgeSurfaceKey {
            producer: audio_key.producer,
            context: audio_key.context,
            surface: audio_key.surface,
        },
        logical_width: 16,
        logical_height: 16,
        capture_policy: 0,
        descriptor: vivid_sdk::presenter::BridgeSourceDescriptor {
            role: 1,
            title: "pause resume".into(),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let request = BridgePlayRequest {
        start_pts_us: 0,
        minimum_buffer_us: 1,
        maximum_latency_us: 1_000_000,
        rate_32_32: 1_i64 << 32,
        late_policy: 1,
        loop_count: 0,
        start_policy: 1,
        hold_serial: None,
    };
    let paused = BridgeSource {
        decoder_reset_serial: 1,
        key: audio_key,
        kind: BridgeSourceKind::Audio {
            linked_video: None,
            codec: "pcm_s16le".into(),
            packetization: "pcm-packet-v1".into(),
            extradata: Vec::new(),
            sample_rate: 48_000,
            channels: 2,
            channel_mask: 3,
            bitrate: 1_536_000,
            max_access_unit_bytes: 256,
            codec_string: None,
        },
        live: false,
        active: true,
        audio_gain: Some(AudioGain::UNITY.raw()),
        capture_policy: 0,
        descriptor: None,
        playing: false,
        play_request: request,
        eos_epoch: None,
        causation_id: None,
    };
    bridge
        .rebuild(&[surface], std::slice::from_ref(&paused), &[])
        .unwrap();
    presenter.projection_snapshot(&HashSet::from([7]));

    let body = media::audio_packet_body(AudioPacket {
        epoch: 1,
        packet_id: 1,
        pts_us: 0,
        dts_us: 0,
        duration_us: 10_000,
        trim_start_samples: 0,
        trim_end_samples: 0,
        data: &[0; 32],
    })
    .unwrap();
    assert!(
        bridge
            .media_chunk(MediaChunk {
                delivery_id: 1,
                source: audio_key,
                record_type: messages::AUDIO_PACKET,
                offset: 0,
                total: u32::try_from(body.len()).unwrap(),
                last: true,
                bytes: body
            })
            .unwrap()
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while !bridge.tracks[&audio_key].activated {
        let _ = bridge.take_media_completions();
        bridge.retry_pending_activation().unwrap();
        assert!(
            Instant::now() < deadline,
            "the timed audio slot never became active"
        );
        thread::sleep(Duration::from_millis(2));
    }

    let mut playing = paused.clone();
    playing.playing = true;
    bridge
        .update_playback(
            std::slice::from_ref(&paused),
            std::slice::from_ref(&playing),
        )
        .unwrap();
    assert!(bridge.tracks[&audio_key].playing);

    bridge
        .update_playback(
            std::slice::from_ref(&playing),
            std::slice::from_ref(&paused),
        )
        .unwrap();
    assert!(!bridge.tracks[&audio_key].playing);
    let submitted_before_resume = bridge.tracks[&audio_key].media_submitted;

    let mut resumed = playing;
    resumed.play_request.start_pts_us = 4_000_000;
    bridge
        .update_playback(
            std::slice::from_ref(&paused),
            std::slice::from_ref(&resumed),
        )
        .unwrap();

    assert!(
        !bridge.tracks[&audio_key].playing,
        "resume played the retained tail before the nested audio feed restarted"
    );
    assert_eq!(
        bridge.tracks[&audio_key].media_submitted, submitted_before_resume,
        "the pending resume changed media accounting"
    );

    let body = media::audio_packet_body(AudioPacket {
        epoch: 1,
        packet_id: 2,
        pts_us: 4_000_000,
        dts_us: 4_000_000,
        duration_us: 10_000,
        trim_start_samples: 0,
        trim_end_samples: 0,
        data: &[0; 32],
    })
    .unwrap();
    assert!(
        bridge
            .media_chunk(MediaChunk {
                delivery_id: 2,
                source: audio_key,
                record_type: messages::AUDIO_PACKET,
                offset: 0,
                total: u32::try_from(body.len()).unwrap(),
                last: true,
                bytes: body
            })
            .unwrap(),
        "PAUSE did not admit one bounded resume packet"
    );
    bridge.retry_pending_playback().unwrap();

    assert!(
        bridge.tracks[&audio_key].playing,
        "one fresh audio submission did not release the pending PLAY"
    );
    assert_eq!(
        bridge.tracks[&audio_key].media_submitted,
        submitted_before_resume + 1,
        "resume admitted more than the one record needed to restart the feed"
    );
    assert!(
        presenter
            .projection_snapshot(&HashSet::from([7]))
            .sources
            .iter()
            .find(|source| source.key.track == bridge.tracks[&audio_key].track.id())
            .is_some_and(|source| source.playing),
        "the physical presenter did not observe resumed playback"
    );
}

/// One paused, active, audio-only surface attached to a live virtual presenter.
///
/// This is the shape a nested Vivi seek leaves behind: the timed tracks are replaced while
/// the producer stays paused, so no `playing` edge ever reaches the bridge.
#[cfg(unix)]
struct PausedSurfaceFixture {
    _directory: tempfile::TempDir,
    presenter: vivid_sdk::presenter::VirtualVivid,
    bridge: OuterBridge,
    surface: BridgeSurface,
    source: BridgeSource,
    packet_id: u64,
}

#[cfg(unix)]
impl PausedSurfaceFixture {
    fn start(name: &str) -> Option<Self> {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join(name);
        let listener = match TestSocketListener::bind(socket.clone()) {
            Ok(listener) => listener,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                eprintln!("skipping {name} socket test: {error}");
                return None;
            }
            Err(error) => panic!("{name} listener failed: {error}"),
        };
        let presenter = vivid_sdk::presenter::VirtualVivid::start_eventless(
            listener,
            vivid_sdk::presenter::MediaConfig::default(),
        )
        .unwrap();
        presenter.update_metrics(7, 80, 24, (8, 16));
        let secret = presenter.issue_pane_capability(7).unwrap();
        let bridge = OuterBridge::builder(
            Secret32::from_hex(&secret).unwrap(),
            DisplayMetrics::default(),
        )
        .control_endpoint(format!("unix:{}", socket.display()))
        .build()
        .unwrap();
        let key = BridgeSourceKey {
            producer: 3,
            context: 1,
            surface: 7,
            track: 11,
        };
        let surface = BridgeSurface {
            overlay_window: None,
            overlay_layouts: Vec::new(),
            key: BridgeSurfaceKey {
                producer: key.producer,
                context: key.context,
                surface: key.surface,
            },
            logical_width: 16,
            logical_height: 16,
            capture_policy: 0,
            descriptor: vivid_sdk::presenter::BridgeSourceDescriptor {
                role: 1,
                title: "paused seek".into(),
                content_revision: 1,
                semantic_availability: 0,
                locator: String::new(),
            },
        };
        let source = BridgeSource {
            decoder_reset_serial: 1,
            key,
            kind: BridgeSourceKind::Audio {
                linked_video: None,
                codec: "pcm_s16le".into(),
                packetization: "pcm-packet-v1".into(),
                extradata: Vec::new(),
                sample_rate: 48_000,
                channels: 2,
                channel_mask: 3,
                bitrate: 1_536_000,
                max_access_unit_bytes: 256,
                codec_string: None,
            },
            live: false,
            active: true,
            audio_gain: Some(AudioGain::UNITY.raw()),
            capture_policy: 0,
            descriptor: None,
            playing: false,
            play_request: BridgePlayRequest {
                start_pts_us: 0,
                minimum_buffer_us: 1,
                maximum_latency_us: 1_000_000,
                rate_32_32: 1_i64 << 32,
                late_policy: 1,
                loop_count: 0,
                start_policy: 1,
                hold_serial: None,
            },
            eos_epoch: None,
            causation_id: None,
        };
        Some(Self {
            _directory: directory,
            presenter,
            bridge,
            surface,
            source,
            packet_id: 0,
        })
    }

    fn key(&self) -> BridgeSourceKey {
        self.source.key
    }

    fn submit_packet(&mut self, pts_us: i64) -> bool {
        self.packet_id += 1;
        let body = media::audio_packet_body(AudioPacket {
            epoch: 1,
            packet_id: self.packet_id,
            pts_us,
            dts_us: pts_us,
            duration_us: 10_000,
            trim_start_samples: 0,
            trim_end_samples: 0,
            data: &[0; 32],
        })
        .unwrap();
        self.bridge
            .media_chunk(MediaChunk {
                delivery_id: self.packet_id,
                source: self.source.key,
                record_type: messages::AUDIO_PACKET,
                offset: 0,
                total: u32::try_from(body.len()).unwrap(),
                last: true,
                bytes: body,
            })
            .unwrap()
    }

    /// Publish the bridge's own outer session as a projected producer of the test presenter.
    fn project(&self) {
        self.presenter.projection_snapshot(&HashSet::from([7]));
    }

    /// Service the bridge until the outer slot holds this source.
    fn settle_activation(&mut self, pts_us: i64) {
        let key = self.source.key;
        let deadline = Instant::now() + Duration::from_secs(2);
        while !self.bridge.tracks[&key].activated {
            self.project();
            let _ = self.bridge.take_media_completions();
            self.bridge.retry_pending_activation().unwrap();
            self.bridge.retry_pending_playback().unwrap();
            if self.bridge.can_accept_media(key) {
                self.submit_packet(pts_us);
            }
            assert!(
                Instant::now() < deadline,
                "the timed audio slot never became active"
            );
            thread::sleep(Duration::from_millis(2));
        }
        let _ = self.bridge.take_media_completions();
    }

    fn outer_playback(&self) -> (i64, bool) {
        let outer_track = self.bridge.tracks[&self.source.key].track.id();
        let snapshot = self.presenter.projection_snapshot(&HashSet::from([7]));
        let source = snapshot
            .sources
            .iter()
            .find(|source| source.key.track == outer_track)
            .expect("the outer presenter lost the relayed track");
        (source.play_request.start_pts_us, source.playing)
    }
}

#[cfg(unix)]
#[test]
fn quitting_a_paused_owner_does_not_block_its_next_launch() {
    paused_owner_replacement(false);
}

#[cfg(unix)]
#[test]
fn correlated_paused_recovery_restores_a_clock_without_outer_history() {
    paused_owner_replacement(true);
}

#[cfg(unix)]
fn paused_owner_replacement(paused_recovery: bool) {
    let Some(mut fixture) = PausedSurfaceFixture::start("pause-quit-restart.sock") else {
        return;
    };
    let mut other_surface = fixture.surface.clone();
    other_surface.key.producer += 1;
    let mut other = fixture.source.clone();
    other.key.producer += 1;
    let node = |surface: BridgeSurfaceKey| BridgeNode {
        producer: surface.producer,
        node: 9,
        fragment: 0,
        surface,
        x: 0,
        y: 0,
        width: 16,
        height: 16,
        z_index: 0,
        visible: true,
        clip: vivid_sdk::presenter::BridgeClipRect {
            x: 0,
            y: 0,
            width: 16,
            height: 16,
        },
    };
    let other_node = node(other_surface.key);
    fixture
        .bridge
        .rebuild(
            &[fixture.surface.clone(), other_surface.clone()],
            &[fixture.source.clone(), other.clone()],
            &[node(fixture.surface.key), other_node.clone()],
        )
        .unwrap();
    fixture.settle_activation(0);
    let survivor = fixture
        .bridge
        .session
        .query_surface(&fixture.bridge.surfaces[&other_surface.key])
        .unwrap();
    let survivor_track = fixture.bridge.outer_track_id(other.key).unwrap();
    let survivor_node = fixture.bridge.nodes[&(other.key.producer, 9, 0)].clone();

    // Quit while PAUSE is authoritative, then launch a new owner using the same local IDs.
    fixture
        .bridge
        .rebuild(
            std::slice::from_ref(&other_surface),
            std::slice::from_ref(&other),
            std::slice::from_ref(&other_node),
        )
        .unwrap();
    let retired = fixture.surface.key;
    fixture.surface.key.producer += 2;
    fixture.source.key.producer += 2;
    fixture.source.playing = !paused_recovery;
    if paused_recovery {
        fixture.source.play_request.start_pts_us = 2_000_000;
        fixture.source.play_request.start_policy = 2;
        fixture.source.play_request.hold_serial = Some(7);
    }
    fixture.packet_id = 0;
    fixture
        .bridge
        .rebuild(
            &[fixture.surface.clone(), other_surface.clone()],
            &[fixture.source.clone(), other.clone()],
            &[node(fixture.surface.key), other_node.clone()],
        )
        .unwrap();
    fixture.settle_activation(fixture.source.play_request.start_pts_us);
    let deadline = Instant::now() + Duration::from_secs(2);
    while fixture.bridge.retiring_surfaces.contains_key(&retired)
        || if paused_recovery {
            fixture.bridge.tracks[&fixture.key()].published_play_request
                != Some(fixture.source.play_request)
        } else {
            !fixture.bridge.tracks[&fixture.key()].playing
        }
    {
        fixture.project();
        fixture.bridge.take_media_completions();
        fixture.bridge.retry_pending_activation().unwrap();
        fixture.bridge.retry_pending_playback().unwrap();
        assert!(
            Instant::now() < deadline,
            "paused quit stranded the new playback owner"
        );
        thread::sleep(Duration::from_millis(2));
    }
    if paused_recovery {
        assert_eq!(fixture.outer_playback(), (2_000_000, false));
    }
    assert_eq!(
        fixture.bridge.outer_track_id(other.key),
        Some(survivor_track)
    );
    assert_eq!(
        fixture.bridge.nodes[&(other.key.producer, 9, 0)],
        survivor_node
    );
    assert_eq!(
        fixture
            .bridge
            .session
            .query_surface(&fixture.bridge.surfaces[&other_surface.key])
            .unwrap()
            .revision,
        survivor.revision
    );
    let mut moved = other_node;
    moved.x = 1;
    fixture
        .bridge
        .update_nodes(&[node(fixture.surface.key), moved])
        .unwrap();
    assert_eq!(
        fixture.bridge.nodes[&(other.key.producer, 9, 0)].0,
        survivor_node.0
    );
    assert_ne!(
        fixture.bridge.nodes[&(other.key.producer, 9, 0)].1,
        survivor_node.1
    );
}

/// A retired surface whose first track cannot be destroyed must still destroy the rest, and
/// must keep the failed track until it can retry. Dropping the retirement on the first error
/// left every later outer track alive with nothing left to destroy it.
#[cfg(unix)]
#[test]
fn a_failed_retirement_destroy_keeps_the_owners_remaining_cleanup() {
    let Some(mut fixture) = PausedSurfaceFixture::start("retire-destroy.sock") else {
        return;
    };
    fixture.source.active = false;
    let mut second = fixture.source.clone();
    second.key.track += 1;
    // Another owner reusing the same local context, surface and track IDs.
    let mut other_surface = fixture.surface.clone();
    other_surface.key.producer += 1;
    let mut other = fixture.source.clone();
    other.key.producer += 1;
    fixture
        .bridge
        .rebuild(
            &[fixture.surface.clone(), other_surface.clone()],
            &[fixture.source.clone(), second.clone(), other.clone()],
            &[],
        )
        .unwrap();
    let retired = fixture.surface.key;
    let poisoned = fixture.bridge.tracks[&fixture.key()].track.clone();
    let healthy = fixture.bridge.tracks[&second.key].track.clone();
    let survivor = fixture.bridge.tracks[&other.key].track.clone();
    // Destroying this handle elsewhere makes the bridge's own destroy of it fail.
    fixture
        .bridge
        .session
        .destroy_track(&poisoned, &RequestMetadata::default())
        .unwrap();
    let tracks = [fixture.key(), second.key]
        .map(|key| fixture.bridge.tracks.remove(&key).unwrap())
        .into();
    fixture.bridge.retiring_surfaces.insert(
        retired,
        RetiringSurface {
            clock: PositionJob {
                key: second.key,
                writer_id: 0,
                track: healthy.clone(),
                decoder_reset_serial: second.decoder_reset_serial,
                playing: false,
                start_pts_us: 0,
            },
            tracks,
            result: None,
        },
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    while fixture.bridge.poll_retiring_surfaces().is_ok() {
        assert!(Instant::now() < deadline, "the retirement never finished");
        thread::sleep(Duration::from_millis(2));
    }

    let remaining = fixture
        .bridge
        .retiring_surfaces
        .get(&retired)
        .map(|retiring| {
            retiring
                .tracks
                .iter()
                .map(|track| track.track.id())
                .collect::<Vec<_>>()
        });
    assert_eq!(remaining, Some(vec![poisoned.id()]));
    assert!(
        fixture.bridge.session.query_track(&healthy).is_err(),
        "the destroyable track of the retired surface was leaked"
    );
    fixture.bridge.session.query_track(&survivor).unwrap();
    assert_eq!(
        fixture.bridge.outer_track_id(other.key),
        Some(survivor.id())
    );
    assert!(fixture.bridge.surfaces.contains_key(&other_surface.key));
}

/// Seeking while paused replaces the timed tracks and never publishes a `playing` edge the
/// bridge can observe: consecutive projection snapshots coalesce the producer's PLAY-then-PAUSE
/// pair into one paused snapshot. Without republishing the position from level state, the
/// replacement outer track has no clock at all, the physical presenter holds every decoded
/// picture, and the pane stays blank until the user happens to resume.
#[test]
#[cfg(unix)]
fn a_paused_seek_positions_the_replacement_outer_track() {
    let Some(mut fixture) = PausedSurfaceFixture::start("paused-seek.sock") else {
        return;
    };
    let key = fixture.key();
    let surface = fixture.surface.clone();
    let paused = fixture.source.clone();
    fixture
        .bridge
        .rebuild(
            std::slice::from_ref(&surface),
            std::slice::from_ref(&paused),
            &[],
        )
        .unwrap();
    fixture.project();
    assert!(fixture.submit_packet(0));
    fixture.settle_activation(0);

    let mut playing = paused.clone();
    playing.playing = true;
    fixture
        .bridge
        .update_playback(
            std::slice::from_ref(&paused),
            std::slice::from_ref(&playing),
        )
        .unwrap();
    fixture
        .bridge
        .update_playback(
            std::slice::from_ref(&playing),
            std::slice::from_ref(&paused),
        )
        .unwrap();
    assert_eq!(
        fixture.outer_playback().0,
        0,
        "the initial position did not reach the outer presenter"
    );

    // The seek: a replacement decoder, a new authoritative target, and no playing edge.
    let target_pts_us: i64 = 9_000_000;
    let mut sought = paused.clone();
    sought.decoder_reset_serial = 2;
    sought.play_request.start_pts_us = target_pts_us;
    fixture
        .bridge
        .rebuild(
            std::slice::from_ref(&surface),
            std::slice::from_ref(&sought),
            &[],
        )
        .unwrap();
    fixture.project();
    fixture.source = sought;
    assert!(
        fixture.bridge.tracks[&key].published_play_request.is_none(),
        "the replacement outer track was created already positioned"
    );

    assert!(fixture.submit_packet(target_pts_us));
    fixture.settle_activation(target_pts_us);
    fixture.bridge.retry_pending_playback().unwrap();

    assert_eq!(
        fixture.outer_playback(),
        (target_pts_us, false),
        "the paused seek target never reached the outer presenter"
    );
    fixture.bridge.retry_pending_playback().unwrap();
    assert_eq!(
        fixture.outer_playback(),
        (target_pts_us, false),
        "an already published paused position was republished"
    );
}

/// The bounded pre-roll window sizes an activation handshake, and nothing raises it once the
/// slot is activated. A seek taken while paused needs a whole key-frame interval of decoder
/// references to reach the target its producer published, so the window becomes a wall that
/// replacement generation can never get past and the pane holds the wrong picture until the
/// producer resumes.
#[test]
#[cfg(unix)]
fn an_activated_paused_source_keeps_accepting_pre_roll() {
    let Some(mut fixture) = PausedSurfaceFixture::start("paused-preroll.sock") else {
        return;
    };
    let key = fixture.key();
    let surface = fixture.surface.clone();
    let paused = fixture.source.clone();
    fixture
        .bridge
        .rebuild(
            std::slice::from_ref(&surface),
            std::slice::from_ref(&paused),
            &[],
        )
        .unwrap();
    fixture.project();
    assert!(fixture.submit_packet(0));
    fixture.settle_activation(0);

    let track = fixture.bridge.tracks.get_mut(&key).unwrap();
    assert!(!track.playing, "the fixture source is not paused");
    track.preplay_ceiling = track.media_submitted;
    assert!(
        fixture.bridge.can_accept_media(key),
        "an exhausted pre-roll window walled off an activated paused source"
    );

    // Exercise the socket writers and acknowledgements, rather than only the admission
    // predicate: an activated generation must carry more than the old 32-record ceiling.
    for _ in 0..64 {
        let deadline = Instant::now() + Duration::from_secs(2);
        while !fixture.bridge.can_accept_media(key) {
            let _ = fixture.bridge.take_media_completions();
            assert!(
                Instant::now() < deadline,
                "activated pre-roll stopped making progress"
            );
            thread::sleep(Duration::from_millis(2));
        }
        assert!(fixture.submit_packet(0));
        let _ = fixture.bridge.take_media_completions();
    }
    assert!(fixture.bridge.tracks[&key].media_submitted > 32);

    let track = fixture.bridge.tracks.get_mut(&key).unwrap();
    track.activated = false;
    assert!(
        !fixture.bridge.can_accept_media(key),
        "pre-PLAY pre-roll stopped being bounded before activation"
    );
}

#[test]
fn image_eos_waits_for_delayed_retained_body_without_blocking_another_owner() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let mut bridge = OuterBridge::builder(
        Secret32::from_hex(vivid_sdk::testing::ROOT_SECRET_HEX).unwrap(),
        DisplayMetrics::default(),
    )
    .control_endpoint(presenter.endpoint().to_owned())
    .build()
    .unwrap();
    let image = [0_u8; 4];
    let surface = |producer| BridgeSurface {
        overlay_window: None,
        overlay_layouts: Vec::new(),
        key: BridgeSurfaceKey {
            producer,
            context: 1,
            surface: 1,
        },
        logical_width: 1,
        logical_height: 1,
        capture_policy: 0,
        descriptor: vivid_sdk::presenter::BridgeSourceDescriptor {
            role: 1,
            title: String::new(),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let source = |producer| BridgeSource {
        key: BridgeSourceKey {
            producer,
            context: 1,
            surface: 1,
            track: 1,
        },
        kind: BridgeSourceKind::Image {
            encoding: 1,
            width: 1,
            height: 1,
            encoded_length: 4,
            sha256: None,
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
    let sources = vec![source(11), source(12)];
    bridge
        .rebuild(&[surface(11), surface(12)], &sources, &[])
        .unwrap();
    let mut ended = sources.clone();
    for source in &mut ended {
        source.eos_epoch = Some(0);
    }
    bridge.update_playback(&sources, &ended).unwrap();

    // Projection/EOS can arrive before the requested retained body enters any client queue.
    bridge.flush_pending_eos(&HashSet::new());
    for source in &sources {
        assert!(
            bridge.can_accept_media(source.key),
            "EOS closed an unhydrated image"
        );
    }
    let delayed = sources[0].key;
    let other = sources[1].key;
    assert!(
        bridge
            .media_chunk(MediaChunk {
                delivery_id: 2,
                source: other,
                record_type: messages::IMAGE_DATA,
                offset: 0,
                total: 4,
                last: true,
                bytes: image.to_vec()
            })
            .unwrap()
    );
    bridge.flush_pending_eos(&HashSet::new());
    assert!(bridge.can_accept_media(delayed));
    assert!(bridge.tracks[&other].eos);

    // A partial body is still not evidence that this channel can end.
    assert!(
        !bridge
            .media_chunk(MediaChunk {
                delivery_id: 1,
                source: delayed,
                record_type: messages::IMAGE_DATA,
                offset: 0,
                total: 4,
                last: false,
                bytes: image[..2].to_vec()
            })
            .unwrap()
    );
    bridge.flush_pending_eos(&HashSet::new());
    assert!(bridge.can_accept_media(delayed));
    assert!(
        bridge
            .media_chunk(MediaChunk {
                delivery_id: 1,
                source: delayed,
                record_type: messages::IMAGE_DATA,
                offset: 2,
                total: 4,
                last: true,
                bytes: image[2..].to_vec()
            })
            .unwrap()
    );
    bridge.flush_pending_eos(&HashSet::new());

    // Observe the actual independent channels, not just the bridge's EOS bookkeeping.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let channels = presenter.channels();
        if channels.len() == 2 && channels.iter().all(|channel| channel.media_records == 1) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "EOS overtook an owner's IMAGE_DATA: {channels:?}"
        );
        thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn gateway_capture_policy_is_monotonic_and_cannot_be_loosened_by_either_hop() {
    let inner_deny = 0b00001;
    let route_deny = 0b10100;
    let effective = intersect_surface_policy(inner_deny, route_deny).unwrap();
    assert_eq!(effective, inner_deny | route_deny);
    assert_eq!(effective & inner_deny, inner_deny);
    assert_eq!(effective & route_deny, route_deny);
    assert_eq!(
        intersect_surface_policy(effective, 0).unwrap(),
        effective,
        "an outer hop cannot remove an inner denial"
    );
    intersect_surface_policy(1_u64 << 63, 0).unwrap_err();
}

/// Vrowser uses one live raster track and one live PCM track on the same surface. It activates
/// both slots without PLAY and sends both kinds from one worker. Treating the audio as timed
/// leaves the pre-roll allowance exhausted; the next audio write then blocks
/// that worker before it can send another browser frame.
#[test]
#[cfg(unix)]
fn active_live_raster_and_audio_keep_advancing_without_play() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("live-browser.sock");
    let listener = match TestSocketListener::bind(socket.clone()) {
        Ok(listener) => listener,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            eprintln!("skipping live browser socket test: {error}");
            return;
        }
        Err(error) => panic!("live browser listener failed: {error}"),
    };
    let presenter = vivid_sdk::presenter::VirtualVivid::start_eventless(
        listener,
        vivid_sdk::presenter::MediaConfig::default(),
    )
    .unwrap();
    presenter.update_metrics(7, 80, 24, (8, 16));
    let secret = presenter.issue_pane_capability(7).unwrap();
    let mut bridge = OuterBridge::builder(
        Secret32::from_hex(&secret).unwrap(),
        DisplayMetrics::default(),
    )
    .control_endpoint(format!("unix:{}", socket.display()))
    .build()
    .unwrap();

    let raster_key = BridgeSourceKey {
        producer: 3,
        context: 1,
        surface: 7,
        track: 11,
    };
    let audio_key = BridgeSourceKey {
        track: 12,
        ..raster_key
    };
    let surface = BridgeSurface {
        overlay_window: None,
        overlay_layouts: Vec::new(),
        key: BridgeSurfaceKey {
            producer: raster_key.producer,
            context: raster_key.context,
            surface: raster_key.surface,
        },
        logical_width: 2,
        logical_height: 1,
        capture_policy: 0,
        descriptor: vivid_sdk::presenter::BridgeSourceDescriptor {
            role: 1,
            title: "live browser".into(),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let request = default_play_request();
    let raster = BridgeSource {
        decoder_reset_serial: 1,
        key: raster_key,
        kind: BridgeSourceKind::Raster {
            width: 2,
            height: 1,
            alpha_mode: 1,
            compression_mode: 1,
            delta_operation_limit: Some(4),
        },
        live: true,
        active: true,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: false,
        play_request: request,
        eos_epoch: None,
        causation_id: None,
    };
    let audio = BridgeSource {
        decoder_reset_serial: 1,
        key: audio_key,
        kind: BridgeSourceKind::Audio {
            linked_video: None,
            codec: "pcm_f32le".into(),
            packetization: "pcm-packet-v1".into(),
            extradata: Vec::new(),
            sample_rate: 48_000,
            channels: 2,
            channel_mask: 3,
            bitrate: 3_072_000,
            max_access_unit_bytes: 1024,
            codec_string: Some("pcm-f32".into()),
        },
        live: true,
        active: true,
        audio_gain: Some(AudioGain::UNITY.raw()),
        capture_policy: 0,
        descriptor: None,
        playing: false,
        play_request: request,
        eos_epoch: None,
        causation_id: None,
    };
    let node = BridgeNode {
        producer: raster_key.producer,
        node: 1,
        fragment: 0,
        surface: surface.key,
        x: 0,
        y: 0,
        width: 2_i64 << 32,
        height: 1_i64 << 32,
        z_index: 0,
        visible: true,
        clip: vivid_sdk::presenter::BridgeClipRect {
            x: 0,
            y: 0,
            width: 2_i64 << 32,
            height: 1_i64 << 32,
        },
    };
    bridge
        .rebuild(&[surface], &[raster, audio], &[node])
        .unwrap();

    let full = media::raster_frame_body(1, 1, 2, 1, &[0xff, 0, 0, 0xff, 0, 0, 0, 0xff]).unwrap();
    let audio_packet = |packet_id| {
        media::audio_packet_body(AudioPacket {
            epoch: 1,
            packet_id,
            pts_us: i64::try_from(packet_id).unwrap() * 10_000,
            dts_us: i64::try_from(packet_id).unwrap() * 10_000,
            duration_us: 10_000,
            trim_start_samples: 0,
            trim_end_samples: 0,
            data: &[0; 32],
        })
        .unwrap()
    };
    for (delivery, key, record_type, body) in [
        (1, raster_key, messages::RASTER_FRAME, full),
        (2, audio_key, messages::AUDIO_PACKET, audio_packet(1)),
    ] {
        assert!(
            bridge
                .media_chunk(MediaChunk {
                    delivery_id: delivery,
                    source: key,
                    record_type,
                    offset: 0,
                    total: u32::try_from(body.len()).unwrap(),
                    last: true,
                    bytes: body
                })
                .unwrap()
        );
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        bridge.retry_pending_activation().unwrap();
        let snapshot = presenter.projection_snapshot(&HashSet::from([7]));
        if snapshot.sources.len() == 2
            && snapshot
                .sources
                .iter()
                .all(|source| source.live && source.active)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the outer surface never activated both live browser slots"
        );
        thread::sleep(Duration::from_millis(2));
    }

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut completed = HashSet::new();
    while completed.len() < 2 {
        completed.extend(bridge.take_media_completions().into_iter().filter_map(
            |DeliveryOutcome {
                 delivery_id: delivery,
                 delivered,
                 ..
             }| delivered.then_some(delivery),
        ));
        assert!(
            Instant::now() < deadline,
            "initial live browser records never returned their ingress allowance"
        );
        thread::sleep(Duration::from_millis(2));
    }
    assert!(
        bridge.can_accept_media(audio_key),
        "live audio was incorrectly capped at one timed pre-roll record"
    );

    let next_frame =
        media::raster_frame_body(1, 2, 2, 1, &[0, 0xff, 0, 0xff, 0, 0, 0, 0xff]).unwrap();
    for (delivery, key, record_type, body) in [
        (3, raster_key, messages::RASTER_FRAME, next_frame),
        (4, audio_key, messages::AUDIO_PACKET, audio_packet(2)),
    ] {
        assert!(
            bridge
                .media_chunk(MediaChunk {
                    delivery_id: delivery,
                    source: key,
                    record_type,
                    offset: 0,
                    total: u32::try_from(body.len()).unwrap(),
                    last: true,
                    bytes: body
                })
                .unwrap()
        );
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while completed.len() < 4 {
        for DeliveryOutcome {
            delivery_id: delivery,
            delivered,
            ..
        } in bridge.take_media_completions()
        {
            assert!(delivered, "live browser delivery {delivery} failed");
            completed.insert(delivery);
        }
        assert!(
            Instant::now() < deadline,
            "live raster/audio stopped after the initial browser frame: completed={completed:?}, raster_inflight={}, audio_inflight={}, raster_active={}, audio_active={}",
            bridge.tracks[&raster_key].media_inflight,
            bridge.tracks[&audio_key].media_inflight,
            bridge.tracks[&raster_key]
                .slot_activated
                .load(Ordering::Acquire),
            bridge.tracks[&audio_key]
                .slot_activated
                .load(Ordering::Acquire),
        );
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(completed, HashSet::from([1, 2, 3, 4]));
}

/// Decoder pre-roll must use granted credit without a control or returned-credit round trip
/// per packet, but it must still stop at the finite bootstrap ceiling before activation.
#[test]
fn decoder_pre_roll_uses_a_bounded_window_without_readiness_round_trips() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let mut bridge = OuterBridge::builder(
        Secret32::from_hex(vivid_sdk::testing::ROOT_SECRET_HEX).unwrap(),
        DisplayMetrics::default(),
    )
    .control_endpoint(presenter.endpoint())
    .build()
    .unwrap();

    let video_key = BridgeSourceKey {
        producer: 3,
        context: 1,
        surface: 7,
        track: 11,
    };
    let surface = BridgeSurface {
        overlay_window: None,
        overlay_layouts: Vec::new(),
        key: BridgeSurfaceKey {
            producer: video_key.producer,
            context: video_key.context,
            surface: video_key.surface,
        },
        logical_width: 16,
        logical_height: 16,
        capture_policy: 0,
        descriptor: vivid_sdk::presenter::BridgeSourceDescriptor {
            role: 1,
            title: "seek pre-roll".into(),
            content_revision: 1,
            semantic_availability: 0,
            locator: String::new(),
        },
    };
    let video = BridgeSource {
        decoder_reset_serial: 1,
        key: video_key,
        kind: BridgeSourceKind::Video {
            codec: "h264".into(),
            packetization: "h264-annexb-au-v1".into(),
            extradata: Vec::new(),
            width: 16,
            height: 16,
            profile: 0,
            level: 0,
            bitrate: 8_000_000,
            color_primaries: 1,
            transfer: 1,
            matrix: 1,
            range: 1,
            sar_num: 1,
            sar_den: 1,
            max_access_unit_bytes: 1024,
            codec_string: None,
            decoder_config: None,
        },
        live: false,
        active: true,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: false,
        play_request: default_play_request(),
        eos_epoch: None,
        causation_id: None,
    };
    let node = BridgeNode {
        producer: video_key.producer,
        node: 1,
        fragment: 0,
        surface: surface.key,
        x: 0,
        y: 0,
        width: 16_i64 << 32,
        height: 16_i64 << 32,
        z_index: 0,
        visible: true,
        clip: vivid_sdk::presenter::BridgeClipRect {
            x: 0,
            y: 0,
            width: 16_i64 << 32,
            height: 16_i64 << 32,
        },
    };
    bridge.rebuild(&[surface], &[video], &[node]).unwrap();

    // The scripted peer never returns credit. Its initial grant is enough for this window;
    // neither returned capacity nor a readiness query should gate the next packet.

    let body = media::video_packet_body(media::VideoPacket {
        epoch: 1,
        packet_id: 1,
        pts_us: 0,
        dts_us: 0,
        duration_us: 40_000,
        key: true,
        data: &[0, 0, 0, 1, 0x65, 0x88],
    })
    .unwrap();
    assert!(
        bridge
            .media_chunk(MediaChunk {
                delivery_id: 1,
                source: video_key,
                record_type: messages::VIDEO_PACKET,
                offset: 0,
                total: u32::try_from(body.len()).unwrap(),
                last: true,
                bytes: body
            })
            .unwrap()
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while bridge.tracks[&video_key].media_inflight != 0 {
        let _ = bridge.take_media_completions();
        assert!(
            Instant::now() < deadline,
            "the first pre-roll record never returned its ingress allowance"
        );
        thread::sleep(Duration::from_millis(2));
    }
    assert!(
        bridge.can_accept_media(video_key),
        "pre-roll must not wait for a readiness query"
    );
    for packet_id in 2..=OUTER_TIMED_PREROLL_RECORDS as u64 {
        assert!(bridge.can_accept_media(video_key));
        let body = media::video_packet_body(media::VideoPacket {
            epoch: 1,
            packet_id,
            pts_us: i64::try_from(packet_id).unwrap() * 40_000,
            dts_us: i64::try_from(packet_id).unwrap() * 40_000,
            duration_us: 40_000,
            key: true,
            data: &[0, 0, 0, 1, 0x65, 0x88],
        })
        .unwrap();
        assert!(
            bridge
                .media_chunk(MediaChunk {
                    delivery_id: packet_id,
                    source: video_key,
                    record_type: messages::VIDEO_PACKET,
                    offset: 0,
                    total: u32::try_from(body.len()).unwrap(),
                    last: true,
                    bytes: body
                })
                .unwrap()
        );
        while bridge.tracks[&video_key].media_inflight != 0 {
            let _ = bridge.take_media_completions();
            assert!(
                Instant::now() < deadline,
                "pre-roll stalled awaiting readiness"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }
    assert!(
        !bridge.tracks[&video_key].activated,
        "no readiness observation has authorized activation"
    );
    assert!(
        !bridge.can_accept_media(video_key),
        "pre-roll must stop at its finite ceiling"
    );
}

const RASTER_WIDTH: u32 = 4;
const RASTER_HEIGHT: u32 = 2;
const INNER_DELTA_OPERATIONS: u32 = 4;

/// One raster writer against an offline outer track, granted `outer_delta_operations`.
fn raster_writer(outer_delta_operations: u32) -> (vivid_sdk::Session, OuterMediaWriter) {
    let mut session = vivid_sdk::Session::connect(ProducerConfig::offline()).unwrap();
    let context_id = session.info().root_context_id;
    let surface = session
        .create_surface(
            SurfaceDefinition {
                context_id,
                surface_id: 1,
                semantic_profile: registry::TERMINAL_CONTENT.into(),
                coordinate_model: CoordinateModel::TerminalContentCells,
                logical_width: 1,
                logical_height: 1,
                scale_numerator: 1,
                scale_denominator: 1,
                rotation: 0,
                descriptor: SurfaceDescriptor {
                    role: SurfaceRole::Figure,
                    title: "relayed raster".into(),
                    semantic_content_revision: 1,
                    semantic_availability: 0,
                    locator_hint: String::new(),
                },
                policy: 0,
                profile_parameters: vec![],
            },
            &RequestMetadata::default(),
        )
        .unwrap();
    let body = media::rgba8_raw_frame_body_len(RASTER_WIDTH, RASTER_HEIGHT).unwrap();
    let track = session
        .create_track(
            TrackConfiguration {
                direction: vivid_protocol::track::TrackDirection::default(),
                context_id,
                surface_id: surface.id(),
                track_id: 2,
                slot: SLOT_RASTER,
                mode: TrackMode::Live,
                lane: LaneClass::Bulk,
                maximum_record_body: body,
                maximum_rate_millihertz: 60_000,
                maximum_encoded_bits_per_second: u64::from(body) * 8 * 120,
                maximum_records_per_second: 120,
                maximum_inflight_body_bytes: u64::from(body) * 2,
                kind: KindConfiguration::Raster(RasterConfiguration {
                    width: RASTER_WIDTH,
                    height: RASTER_HEIGHT,
                    alpha_mode: 1,
                    delta_enabled: true,
                    maximum_delta_operations: u8::try_from(INNER_DELTA_OPERATIONS).unwrap(),
                    zstd_enabled: false,
                }),
                target_latency_us: 0,
                maximum_latency_us: 100_000,
                retained_pixel_charge: u64::from(RASTER_WIDTH) * u64::from(RASTER_HEIGHT),
            },
            &RequestMetadata::default(),
        )
        .unwrap();
    let channel = Arc::new(session.open_track_channel(&track).unwrap());
    let writer = OuterMediaWriter {
        overlay_layouts: Arc::default(),
        overlay_revisions: Arc::default(),
        writer_id: 1,
        key: BridgeSourceKey {
            producer: 1,
            context: 1,
            surface: 1,
            track: 3,
        },
        object_id: track.id(),
        channel,
        slot_activated: Arc::new(AtomicBool::new(true)),
        kind: BridgeSourceKind::Raster {
            width: RASTER_WIDTH,
            height: RASTER_HEIGHT,
            alpha_mode: 1,
            compression_mode: 0,
            delta_operation_limit: Some(INNER_DELTA_OPERATIONS),
        },
        outer_delta_operations,
        next_media_id: 0,
        outer_epoch: 0,
        inner_epoch: 0,
        last_raster_id: 0,
        last_inner_raster_id: 0,
        awaiting_full_frame: false,
        needs_full_frame: false,
    };
    (session, writer)
}

fn full_frame_body(frame_id: u64) -> Vec<u8> {
    media::raster_frame_body(
        1,
        frame_id,
        RASTER_WIDTH,
        RASTER_HEIGHT,
        &[0x10, 0x20, 0x30, 0xff].repeat((RASTER_WIDTH * RASTER_HEIGHT) as usize),
    )
    .unwrap()
}

fn delta_body(frame_id: u64, base_frame_id: u64) -> Vec<u8> {
    media::raster_delta_frame_body(
        1,
        frame_id,
        base_frame_id,
        0,
        0,
        RASTER_WIDTH,
        RASTER_HEIGHT,
        INNER_DELTA_OPERATIONS,
        &[RasterDeltaOperation::Overwrite {
            x: 0,
            y: 0,
            width: 1,
            height: 1,
            rgba: &[0xaa, 0xbb, 0xcc, 0xff],
        }],
        false,
    )
    .unwrap()
}

/// An outer presenter that grants no delta operations must not be handed a delta.
///
/// The inner grant only says what the nested producer was allowed to send. Relaying the delta
/// regardless fails the record without asking for anything, which retires the writer and
/// leaves the pane frozen on its last full frame for the life of the source.
#[test]
fn a_delta_the_outer_track_cannot_accept_asks_for_a_full_frame() {
    let (_session, mut writer) = raster_writer(0);
    writer
        .forward_media(messages::RASTER_FRAME, &full_frame_body(1))
        .expect("a full frame is always relayable");
    assert!(!writer.needs_full_frame);

    let error = writer
        .forward_media(messages::RASTER_FRAME, &delta_body(2, 1))
        .expect_err("an ungranted delta cannot be relayed");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        writer.needs_full_frame,
        "the failure has to ask the nested producer for a full frame"
    );
}

/// The same relay stays intact when the outer track did grant the operations.
#[test]
fn a_delta_within_the_outer_grant_is_relayed() {
    let (_session, mut writer) = raster_writer(INNER_DELTA_OPERATIONS);
    writer
        .forward_media(messages::RASTER_FRAME, &full_frame_body(1))
        .expect("a full frame is always relayable");
    writer
        .forward_media(messages::RASTER_FRAME, &delta_body(2, 1))
        .expect("a granted delta is relayable");
    assert!(!writer.needs_full_frame);
    assert_eq!(writer.last_raster_id, 2, "the outer base has to advance");
}

/// One raster writer thread against a live outer presenter, started at `slot_activated`.
///
/// The channel it writes on is real, so the flow window and the presenter's return of it are
/// the ones a relayed track actually meets.
fn live_raster_writer(
    presenter: &vivid_sdk::testing::TestPresenter,
    slot_activated: bool,
) -> (
    vivid_sdk::Session,
    mpsc::SyncSender<OuterMediaCommand>,
    mpsc::Receiver<MediaCompletion>,
) {
    let config = vivid_sdk::ProducerConfig {
        endpoint_control: Some(presenter.endpoint().to_owned()),
        endpoint_bulk: Some(presenter.endpoint().to_owned()),
        authentication: vivid_sdk::ProducerAuthentication::root_hex(
            vivid_sdk::testing::ROOT_SECRET_HEX,
        )
        .unwrap(),
        producer_name: "vivid-gateway-test".to_owned(),
        target_profile: vivid_sdk::TERMINAL_SURFACE.to_owned(),
        required_profiles: vec![
            vivid_sdk::LIVE_MEDIA.to_owned(),
            vivid_sdk::TERMINAL_SURFACE.to_owned(),
            vivid_sdk::CORE_CONTROL.to_owned(),
        ],
        optional_profiles: Vec::new(),
        ..vivid_sdk::ProducerConfig::default()
    };
    let mut session = vivid_sdk::Session::connect(config).expect("live outer session");
    let context_id = session.info().root_context_id;
    let surface = session
        .create_surface(
            SurfaceDefinition {
                context_id,
                surface_id: 1,
                semantic_profile: registry::TERMINAL_CONTENT.into(),
                coordinate_model: CoordinateModel::TerminalContentCells,
                logical_width: 1,
                logical_height: 1,
                scale_numerator: 1,
                scale_denominator: 1,
                rotation: 0,
                descriptor: SurfaceDescriptor {
                    role: SurfaceRole::Figure,
                    title: "relayed raster".into(),
                    semantic_content_revision: 1,
                    semantic_availability: 0,
                    locator_hint: String::new(),
                },
                policy: 0,
                profile_parameters: vec![],
            },
            &RequestMetadata::default(),
        )
        .unwrap();
    let source = BridgeSource {
        decoder_reset_serial: 1,
        key: BridgeSourceKey {
            producer: 1,
            context: 1,
            surface: 1,
            track: 3,
        },
        kind: BridgeSourceKind::Raster {
            width: RASTER_WIDTH,
            height: RASTER_HEIGHT,
            alpha_mode: 1,
            compression_mode: 0,
            delta_operation_limit: Some(INNER_DELTA_OPERATIONS),
        },
        live: true,
        active: true,
        audio_gain: None,
        capture_policy: 0,
        descriptor: None,
        playing: false,
        eos_epoch: None,
        causation_id: None,
        play_request: BridgePlayRequest {
            start_pts_us: 0,
            minimum_buffer_us: 0,
            maximum_latency_us: 500_000,
            rate_32_32: 1_i64 << 32,
            late_policy: 1,
            loop_count: 0,
            start_policy: 1,
            hold_serial: None,
        },
    };
    let configuration = track_configuration(&session, &surface, &source).unwrap();
    let track = session
        .create_track(configuration, &RequestMetadata::default())
        .unwrap();
    let channel = Arc::new(session.open_track_channel(&track).unwrap());
    let writer = OuterMediaWriter {
        overlay_layouts: Arc::default(),
        overlay_revisions: Arc::default(),
        writer_id: 1,
        key: source.key,
        object_id: track.id(),
        channel,
        slot_activated: Arc::new(AtomicBool::new(slot_activated)),
        kind: source.kind.clone(),
        outer_delta_operations: track.delta_operation_limit().unwrap(),
        next_media_id: 0,
        outer_epoch: 0,
        inner_epoch: 0,
        last_raster_id: 0,
        last_inner_raster_id: 0,
        awaiting_full_frame: false,
        needs_full_frame: false,
    };
    let (commands, command_receiver) = mpsc::sync_channel(OUTER_MEDIA_WRITER_QUEUE);
    let (completions, completion_receiver) = mpsc::channel();
    thread::Builder::new()
        .name("gateway-test-outer-media".to_owned())
        .spawn(move || run_outer_media_writer(writer, &command_receiver, &completions))
        .unwrap();
    (session, commands, completion_receiver)
}

fn queue_full_frame(commands: &mpsc::SyncSender<OuterMediaCommand>, frame_id: u64) {
    commands
        .send(OuterMediaCommand::Write {
            delivery_id: frame_id,
            record_type: messages::RASTER_FRAME,
            body: full_frame_body(frame_id),
        })
        .unwrap();
}

/// A static track's writer paces one record at a time only until its outer slot is active.
///
/// The pacing waits for the presenter to return the whole ingress window, which a presenter
/// that returns exactly what it consumed only does once nothing is outstanding. A raster track
/// never reaches PLAY, so a writer that keeps pacing after activation spends a presenter round
/// trip on every frame for the life of the source - free over a local socket, and the reason a
/// nested document reader turned pages slowly over a forwarded one.
#[test]
fn an_activated_static_writer_does_not_wait_out_the_presenter_between_records() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let (_session, commands, completions) = live_raster_writer(&presenter, true);
    for frame_id in 1..=3 {
        queue_full_frame(&commands, frame_id);
    }
    for frame_id in 1..=3 {
        let completion = completions
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|error| panic!("frame {frame_id} was never written: {error}"));
        assert!(completion.delivered, "frame {frame_id} was rejected");
    }
}

/// The same three frames against a writer whose slot is not active yet stop after the first.
///
/// This presenter never returns ingress capacity, which is what the pre-activation pacing
/// waits for. It pins the pacing itself: without it the writer would run all three through,
/// and the pre-roll ordering that timed tracks depend on before `ACTIVATE_TRACK` would be gone.
#[test]
fn a_writer_whose_slot_is_not_active_yet_still_paces_one_record_at_a_time() {
    let presenter = vivid_sdk::testing::TestPresenter::start(80, 24).unwrap();
    let (_session, commands, completions) = live_raster_writer(&presenter, false);
    for frame_id in 1..=3 {
        queue_full_frame(&commands, frame_id);
    }
    assert!(
        completions
            .recv_timeout(Duration::from_millis(500))
            .is_err(),
        "a paced writer must not complete a record before the presenter returns its window"
    );
}

mod audit;
