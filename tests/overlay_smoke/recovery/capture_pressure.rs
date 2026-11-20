//! Authenticated channel advance while original presentation capture is blocked.

// Rust guideline compliant 2026-10-07
use super::*;
use std::collections::BTreeMap;
use vivid_protocol::overlay::wire::{PresentationOutcome, SubmissionOutcome};
use vivid_sdk::OverlayLaneEvent;
use vivid_sdk::presenter::VectorFrame;

const RED: [u8; 4] = [224, 32, 48, 255];
const BLUE: [u8; 4] = [32, 64, 224, 255];
const PURPLE: [u8; 4] = [176, 48, 192, 255];
const GREEN: [u8; 4] = [32, 192, 80, 255];
const AUX: [[u8; 4]; 3] = [
    [192, 128, 32, 255],
    [32, 160, 160, 255],
    [128, 96, 192, 255],
];
const WAIT: Duration = Duration::from_secs(3);
type Outcomes = BTreeMap<(u64, u64), SubmissionOutcome>;

fn source(session: &Session, surface: &Surface, track: &Track) -> SourceKey {
    SourceKey {
        producer: session.info().session_id,
        context: surface.context_id(),
        surface: surface.id(),
        track: track.id(),
    }
}
fn queue(
    inner: &VirtualVivid,
    relay: &mut OuterBridge,
    session: &Session,
    surface: &Surface,
    track: &Track,
    revision: u64,
    delivery_id: u64,
) -> io::Result<()> {
    let end = Instant::now() + WAIT;
    loop {
        let status = session.query_overlay_window(Query {
            context_id: surface.context_id(),
            surface_id: surface.id(),
        })?;
        if status.accepted_revision == revision {
            break;
        }
        assert!(Instant::now() < end, "source acceptance stalled");
        std::thread::sleep(Duration::from_millis(1));
    }
    project(inner, relay)?;
    let key = source(session, surface, track);
    let current = snapshot(inner, &[1, 2]);
    let retained = &current
        .sources
        .iter()
        .find(|s| s.key == key)
        .unwrap()
        .retained_vector;
    assert_eq!(retained.len(), 1, "fixture contains only a vector frame");
    let (record_type, bytes) = &retained[0];
    assert_eq!(*record_type, messages::VECTOR_FRAME);
    assert!(relay.media_chunk(MediaChunk {
        delivery_id,
        source: key,
        record_type: *record_type,
        offset: 0,
        total: u32::try_from(bytes.len()).unwrap(),
        last: true,
        bytes: bytes.to_vec(),
    })?);
    Ok(())
}
fn finish(relay: &mut OuterBridge, delivery_id: u64) -> io::Result<DeliveryOutcome> {
    let end = Instant::now() + WAIT;
    loop {
        let completions = relay.take_media_completions();
        assert!(
            completions.len() <= 1,
            "unrelated delivery completed while waiting"
        );
        if let Some(completion) = completions.into_iter().next() {
            assert_eq!(
                completion.delivery_id, delivery_id,
                "unexpected delivery completed"
            );
            return Ok(completion);
        }
        assert!(Instant::now() < end, "queued writer did not finish");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn visible(session: &mut Session, surface: &Surface, visible: bool) -> io::Result<()> {
    let mut request = session
        .query_overlay_window(Query {
            context_id: surface.context_id(),
            surface_id: surface.id(),
        })?
        .window;
    request.options.visible = visible;
    session.set_overlay_window(&request)?;
    Ok(())
}
fn frame(
    host: &mut NativeVectorHost,
    relay: &mut OuterBridge,
    count: usize,
    primary: u64,
    revision: u64,
    peer: u64,
    peer_revision: u64,
) -> io::Result<VectorFrame> {
    let end = Instant::now() + WAIT;
    loop {
        relay.retry_pending_activation()?;
        let frame = host.prepare(9, 1.)?;
        assert_eq!(frame.dimensions(), (320, 64));
        let matches = |id, rev| {
            frame
                .drawings()
                .iter()
                .any(|d| d.window.surface_id() == id && d.submission.revision == rev)
        };
        if frame.drawings().len() == count
            && matches(peer, peer_revision)
            && (count == 1 || matches(primary, revision))
        {
            return Ok(frame);
        }
        assert!(
            Instant::now() < end,
            "channel-pressure physical frame stalled: {frame:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn collect(producer: &Producer, outcomes: &mut Outcomes) -> io::Result<()> {
    for _ in 0..256 {
        match producer.lane.wait_event(Duration::ZERO)? {
            None => return Ok(()),
            Some(OverlayLaneEvent::Outcome(outcome)) => {
                assert!(
                    outcomes
                        .insert(
                            (
                                outcome.submission.address.surface_id,
                                outcome.submission.revision
                            ),
                            outcome
                        )
                        .is_none(),
                    "duplicate original outcome"
                );
            }
            Some(OverlayLaneEvent::ConnectionLost { .. }) => {
                panic!("original lane closed during channel advance")
            }
            Some(_) => (),
        }
    }
    panic!("fixture input event queue did not drain");
}
fn expect(
    outcomes: &Outcomes,
    surface: &Surface,
    track: &Track,
    revision: u64,
    channel: u64,
    expected: PresentationOutcome,
) {
    let result = outcomes
        .get(&(surface.id(), revision))
        .expect("original outcome arrived");
    assert_eq!(result.outcome, expected);
    assert_eq!(
        result.submission.address,
        WindowAddress {
            context_id: surface.context_id(),
            surface_id: surface.id(),
            generation: surface.generation().get(),
        }
    );
    assert_eq!(result.submission.track_id, track.id());
    assert_eq!(result.submission.epoch, 1);
    assert_eq!(result.submission.channel_generation, channel);
}

#[test]
fn authenticated_channel_advance_retires_blocked_capture_and_preserves_same_id_peer()
-> io::Result<()> {
    exercise(|_, _, _| Ok(()))
}
fn exercise(
    mut render: impl FnMut(&str, &VectorFrame, [[u8; 4]; 5]) -> io::Result<()>,
) -> io::Result<()> {
    let (inner, outer, mut host, mut relay) = fixture_geometry(true, 40, 4)?;
    let mut a = Producer::new(&inner, 1, false)?;
    let mut b = Producer::new(&inner, 2, false)?;
    let owner = a.session.info().session_id;
    let resume = a.session.info().resume_generation;
    let root = relay.outer_session_id();
    let generation = a.track.channel_generation().get();
    let lane_generation = a.lane.generation();
    let address = a.address();
    let key = source(&a.session, &a.surface, &a.track);
    let peer_key = source(&b.session, &b.surface, &b.track);
    assert_ne!(key.producer, peer_key.producer);
    assert_eq!(
        (key.context, key.surface, key.track),
        (peer_key.context, peer_key.surface, peer_key.track)
    );
    let mut auxiliary = Vec::new();
    for (i, color) in AUX.iter().enumerate() {
        let (surface, track, channel) = Producer::create_window(
            &mut a.session,
            f64::from(u32::try_from(i + 2).unwrap()) * 64.,
        )?;
        Producer::paint_window(
            &mut a.session,
            &surface,
            &track,
            &channel,
            1,
            u32::from_be_bytes(*color),
        )?;
        queue(
            &inner,
            &mut relay,
            &a.session,
            &surface,
            &track,
            1,
            100 + i as u64,
        )?;
        assert!(finish(&mut relay, 100 + i as u64)?.delivered);
        auxiliary.push((surface, track, channel));
    }
    a.paint(1, u32::from_be_bytes(RED))?;
    queue(&inner, &mut relay, &a.session, &a.surface, &a.track, 1, 1)?;
    assert!(finish(&mut relay, 1)?.delivered);
    b.paint(1, u32::from_be_bytes(BLUE))?;
    forward_unshown(&inner, &mut relay, &b, 1)?;
    let all_keys = snapshot(&inner, &[1, 2])
        .sources
        .iter()
        .map(|s| s.key)
        .collect::<HashSet<_>>();
    assert_eq!(all_keys.len(), 5);
    let primary = relay.outer_surface_id(a.key()).unwrap();
    let peer = relay.outer_surface_id(b.key()).unwrap();
    let original = frame(&mut host, &mut relay, 5, primary, 1, peer, 1)?;
    render("initial", &original, [RED, BLUE, AUX[0], AUX[1], AUX[2]])?;
    assert_eq!(a.status()?.scene_revision, 0);
    assert_eq!(b.status()?.scene_revision, 0);
    let blocked_before = inner.overlay_presentation_blocked_captures();
    a.paint(2, 0xff80_20ff)?;
    queue(&inner, &mut relay, &a.session, &a.surface, &a.track, 2, 2)?;
    let end = Instant::now() + WAIT;
    while inner.overlay_presentation_blocked_captures() == blocked_before {
        assert!(relay.take_media_completions().is_empty());
        assert!(
            Instant::now() < end,
            "fifth original scene never encountered capture pressure"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    eprintln!("channel pressure stage: pressure-witnessed");
    visible(&mut a.session, &a.surface, false)?;
    for (surface, _, _) in &auxiliary {
        visible(&mut a.session, surface, false)?;
    }
    b.paint(2, u32::from_be_bytes(PURPLE))?;
    queue(&inner, &mut relay, &b.session, &b.surface, &b.track, 2, 200)?;
    assert!(finish(&mut relay, 200)?.delivered);
    let peer_only = frame(&mut host, &mut relay, 1, primary, 1, peer, 2)?;
    render("peer", &peer_only, [[0; 4], PURPLE, [0; 4], [0; 4], [0; 4]])?;
    assert!(host.presented(&peer_only)?);
    wait(&inner, &mut relay, || {
        b.status().is_ok_and(|s| s.scene_revision == 2)
    })?;
    drop(peer_only);
    let mut a_outcomes = Outcomes::new();
    let mut b_outcomes = Outcomes::new();
    collect(&a, &mut a_outcomes)?;
    assert!(
        a_outcomes.is_empty(),
        "peer progress released the blocked owner's original scenes"
    );
    assert!(
        relay.take_media_completions().is_empty(),
        "fifth source escaped the full quota"
    );
    assert_eq!(a.status()?.scene_revision, 0);
    assert_eq!(a.status()?.accepted_revision, 2);
    visible(&mut a.session, &a.surface, true)?;
    for (surface, _, _) in &auxiliary {
        visible(&mut a.session, surface, true)?;
    }
    project(&inner, &mut relay)?;
    let stale = frame(&mut host, &mut relay, 5, primary, 1, peer, 2)?;
    let before = [RED, PURPLE, AUX[0], AUX[1], AUX[2]];
    render("before_advance", &stale, before)?;
    drop(original);
    a.session
        .advance_channel(&a.track, 1, &RequestMetadata::default())?;
    eprintln!("channel pressure stage: channel-advanced");
    assert_eq!(a.track.channel_generation().get(), generation + 1);
    let replacement = a.session.open_track_channel(&a.track)?;
    let old = std::mem::replace(&mut a.channel, replacement);
    let _ = old
        .send_vector(&Frame {
            epoch: 1,
            revision: 3,
            canvas: Canvas::new(),
        })
        .unwrap_err();
    let _ = old.close();
    project(&inner, &mut relay)?;
    assert!(
        !finish(&mut relay, 2)?.delivered,
        "retired blocked scene reached the outer writer"
    );
    assert!(
        !host.presented(&stale)?,
        "channel advance revived an old physical receipt"
    );
    assert_eq!(a.session.info().session_id, owner);
    assert_eq!(a.session.info().resume_generation, resume);
    assert_eq!(inner.desktop_session_generation(key), Some(resume));
    assert_eq!(a.lane.generation(), lane_generation);
    assert_eq!(a.address(), address);
    assert_eq!(relay.outer_session_id(), root);
    assert_eq!(relay.outer_surface_id(a.key()), Some(primary));
    assert_eq!(relay.outer_surface_id(b.key()), Some(peer));
    assert_eq!(
        snapshot(&inner, &[1, 2])
            .sources
            .iter()
            .map(|s| s.key)
            .collect::<HashSet<_>>(),
        all_keys
    );
    assert_eq!(b.status()?.scene_revision, 2);
    for (_, track, _) in &auxiliary {
        assert_eq!(track.channel_generation().get(), generation);
    }
    a.paint(3, u32::from_be_bytes(GREEN))?;
    queue(&inner, &mut relay, &a.session, &a.surface, &a.track, 3, 3)?;
    assert!(finish(&mut relay, 3)?.delivered);
    let fresh = frame(&mut host, &mut relay, 5, primary, 2, peer, 2)?;
    render("fresh", &fresh, [GREEN, PURPLE, AUX[0], AUX[1], AUX[2]])?;
    assert_eq!(
        a.status()?.scene_revision,
        0,
        "readback granted original presentation"
    );
    assert!(host.presented(&fresh)?);
    eprintln!("channel pressure stage: fresh-admitted");
    let end = Instant::now() + WAIT;
    loop {
        input(&inner, &mut relay)?;
        collect(&a, &mut a_outcomes)?;
        collect(&b, &mut b_outcomes)?;
        if a_outcomes.len() == 6 && b_outcomes.len() == 2 {
            break;
        }
        assert!(
            Instant::now() < end,
            "original outcomes did not settle: a={a_outcomes:?}, b={b_outcomes:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    expect(
        &a_outcomes,
        &a.surface,
        &a.track,
        1,
        generation,
        PresentationOutcome::Superseded,
    );
    expect(
        &a_outcomes,
        &a.surface,
        &a.track,
        2,
        generation,
        PresentationOutcome::Superseded,
    );
    expect(
        &a_outcomes,
        &a.surface,
        &a.track,
        3,
        generation + 1,
        PresentationOutcome::Presented,
    );
    for (surface, track, _) in &auxiliary {
        expect(
            &a_outcomes,
            surface,
            track,
            1,
            generation,
            PresentationOutcome::Presented,
        );
    }
    expect(
        &b_outcomes,
        &b.surface,
        &b.track,
        1,
        generation,
        PresentationOutcome::Superseded,
    );
    expect(
        &b_outcomes,
        &b.surface,
        &b.track,
        2,
        generation,
        PresentationOutcome::Presented,
    );
    assert_eq!(a.status()?.scene_revision, 3);
    render("retired", &stale, before)?;
    assert!(!host.presented(&stale)?);
    assert!(!host.presented(&fresh)?);
    input(&inner, &mut relay)?;
    collect(&a, &mut a_outcomes)?;
    collect(&b, &mut b_outcomes)?;
    assert_eq!((a_outcomes.len(), b_outcomes.len()), (6, 2));
    assert!(relay.take_source_losses().is_empty());
    eprintln!(
        "Verified authenticated channel-only pressure recovery: blocked capture refusals={}; all five source keys, root, sessions, resume/input-lane generations and surface IDs unchanged; peer revision 2 Presented, retired revisions 1/2 Superseded, new channel revision 3 and three auxiliary originals Presented; no duplicate outcomes.",
        inner.overlay_presentation_blocked_captures() - blocked_before
    );
    drop((stale, fresh));
    for (surface, _, channel) in auxiliary {
        let _ = channel.close();
        a.session
            .destroy_surface(&surface, &RequestMetadata::default())?;
    }
    a.lane.close()?;
    b.lane.close()?;
    a.session.close()?;
    b.session.close()?;
    drop(relay);
    outer.shutdown();
    inner.shutdown();
    let end = Instant::now() + WAIT;
    while inner.active_worker_count() != 0 || outer.active_worker_count() != 0 {
        assert!(
            Instant::now() < end,
            "native presenter workers did not retire"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    eprintln!("Verified channel-pressure cleanup: inner workers=0, outer workers=0.");
    Ok(())
}
#[cfg(feature = "native-gpu-testing")]
#[test]
#[ignore = "requires a native GPU; channel-only advance during witnessed receipt pressure"]
fn native_gpu_channel_advance_retires_blocked_capture_and_preserves_same_id_peer() -> io::Result<()>
{
    use vivid_sdk::vector_renderer::compositor::offscreen::Offscreen;
    let mut gpu = pollster::block_on(Offscreen::new())?;
    let mut cached = None;
    let mut original_pixels = None;
    exercise(|stage, frame, expected| {
        assert_eq!(frame.dimensions(), (320, 64));
        if gpu.render(frame.drawings(), 320, 64)? {
            let end = Instant::now() + Duration::from_secs(10);
            cached = Some(loop {
                if let Some(pixels) = gpu.poll()? {
                    break pixels;
                }
                assert!(
                    Instant::now() < end,
                    "channel-pressure GPU readback stalled"
                );
                std::thread::sleep(Duration::from_millis(1));
            });
        }
        let pixels = cached
            .as_ref()
            .expect("completed or cached immutable GPU pixels");
        assert_eq!((pixels.width, pixels.height), (320, 64));
        for (i, color) in expected.iter().enumerate() {
            for (dx, dy) in [(1, 1), (20, 20), (46, 46)] {
                let offset = (dy * 320 + i * 64 + dx) * 4;
                assert_eq!(
                    &pixels.rgba[offset..offset + 4],
                    color,
                    "wrong source/channel color at stage {stage}, window {i}"
                );
            }
            for (dx, dy) in [(60, 20), (20, 60)] {
                let offset = (dy * 320 + i * 64 + dx) * 4;
                assert_eq!(
                    &pixels.rgba[offset..offset + 4],
                    &[0; 4],
                    "window escaped its bounds"
                );
            }
        }
        if stage == "before_advance" {
            original_pixels = Some(Arc::clone(&pixels.rgba));
        } else if stage == "retired" {
            assert!(
                pixels.rgba == *original_pixels.as_ref().unwrap(),
                "retired image changed after channel replacement"
            );
        }
        if let Some(directory) = std::env::var_os("VIVID_CHANNEL_PRESSURE_OUTPUT_DIR") {
            let path = std::path::Path::new(&directory);
            std::fs::create_dir_all(path)?;
            std::fs::write(path.join(format!("{stage}.rgba")), &pixels.rgba)?;
        }
        Ok(())
    })?;
    eprintln!(
        "Verified native GPU channel pressure: five 320x64 images, 25 exact RGBA probes per stage and 81,920 identical bytes before advance and after retirement."
    );
    Ok(())
}
