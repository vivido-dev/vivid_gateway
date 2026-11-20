//! External correlations preserve original receipts across independent authenticated sockets.

// Rust guideline compliant 2026-10-07
use super::*;
use std::{collections::HashMap, sync::Arc};
use vivid_protocol::{messages, overlay::wire::PresentationOutcome};
use vivid_sdk::presenter::{
    NativeVectorHost, OverlayPresentationCapture, OverlayPresentationReceipt, VectorFrame,
};

fn ready(
    host: &mut NativeVectorHost,
    relay: &mut OuterBridge,
    count: usize,
    revision: u64,
) -> io::Result<VectorFrame> {
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        relay.retry_pending_activation()?;
        relay.take_overlay_input()?;
        let frame = host.prepare(9, 1.)?;
        if frame.drawings().len() == count
            && frame
                .drawings()
                .iter()
                .all(|d| d.submission.revision == revision)
        {
            return Ok(frame);
        }
        assert!(
            Instant::now() < until,
            "external-report native frame stalled"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn external_outcomes_keep_originals_pending_and_retire_only_the_selected_owner() -> io::Result<()> {
    let (inner, endpoint) = overlay_presenter()?;
    inner.enable_overlay_host_relay();
    let originals = inner.attach_overlay_presentation_relay()?;
    let (outer, outer_endpoint) = overlay_presenter()?;
    let outer = Arc::new(outer);
    let mut host = NativeVectorHost::attach(Arc::clone(&outer), "")?;
    let mut relay = bridge(&outer_endpoint, &pane(&outer, 9)?)?;
    let first = overlay_producer(&endpoint, &pane(&inner, 1)?, "external-a")?;
    let second = overlay_producer(&endpoint, &pane(&inner, 2)?, "external-b")?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    let pending = [a.submit(scene(0x00ff_00ff))?, b.submit(scene(0x0000_ffff))?];
    let until = Instant::now() + Duration::from_secs(5);
    let initial = loop {
        let snapshot = snapshot(&inner, &[1, 2]);
        if snapshot.sources.len() == 2
            && snapshot.sources.iter().all(|s| {
                s.retained_vector
                    .iter()
                    .any(|(kind, _)| *kind == messages::VECTOR_FRAME)
            })
        {
            break snapshot;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    };
    let x = initial.sources[0].key;
    let y = initial.sources[1].key;
    assert_ne!(x.producer, y.producer);
    assert_eq!(
        (x.context, x.surface, x.track),
        (y.context, y.surface, y.track)
    );
    let projection = initial.bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    let mut receipts: HashMap<u64, OverlayPresentationReceipt> = HashMap::new();
    let mut owners = HashMap::new();
    for (i, source) in initial.sources.iter().enumerate() {
        let correlation = u64::try_from(i).unwrap() + 1;
        owners.insert(source.key, correlation);
        for (kind, body) in &source.retained_vector {
            assert_eq!(*kind, messages::VECTOR_FRAME);
            let frame = vivid_protocol::vector::Frame::decode(body).unwrap();
            let OverlayPresentationCapture::Forwarded(receipt) =
                originals.capture(source.key, frame.epoch, frame.revision)?
            else {
                panic!("original copy was not reserved")
            };
            receipts.insert(correlation, receipt);
            relay.media_chunk_with_presentation(
                MediaChunk {
                    delivery_id: correlation,
                    source: source.key,
                    record_type: *kind,
                    offset: 0,
                    total: u32::try_from(body.len()).unwrap(),
                    last: true,
                    bytes: body.to_vec(),
                },
                correlation,
            )?;
        }
    }
    let retired = ready(&mut host, &mut relay, 2, 1)?;
    let until = Instant::now() + Duration::from_secs(5);
    let mut writes = 0;
    while writes < 2 {
        for write in relay.take_media_completions() {
            assert!(write.delivered);
            writes += 1;
        }
        assert!(relay.take_presentation_reports().is_empty());
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    for receipt in &pending {
        assert_eq!(receipt.wait(Duration::ZERO)?, None);
    }
    first.close()?;
    let projection = snapshot(&inner, &[2]).bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    let frame = ready(&mut host, &mut relay, 1, 1)?;
    assert!(!host.presented(&retired)?);
    drop(retired);
    let abandoned = relay.take_presentation_reports();
    assert_eq!(abandoned.len(), 1);
    assert_eq!(abandoned[0].outcome, None);
    assert_ne!(abandoned[0].source, projection.sources[0].key);
    assert_eq!(owners[&abandoned[0].source], abandoned[0].correlation);
    receipts
        .remove(&abandoned[0].correlation)
        .unwrap()
        .abandon();
    assert_eq!(pending[1].wait(Duration::ZERO)?, None);
    assert!(host.presented(&frame)?);
    let until = Instant::now() + Duration::from_secs(5);
    let report = loop {
        relay.take_overlay_input()?;
        if let Some(report) = relay.take_presentation_reports().pop() {
            break report;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(report.source, projection.sources[0].key);
    assert_eq!(report.outcome, Some(PresentationOutcome::Presented));
    assert!(
        receipts
            .remove(&report.correlation)
            .unwrap()
            .complete(report.outcome.unwrap())
    );
    assert_eq!(
        pending[1].wait(Duration::from_secs(2))?,
        Some(PresentationOutcome::Presented)
    );
    drop(frame);
    // A subsequent window/node update still traverses the real sockets after
    // the same-ID owner's original copy and physical writer have retired.
    b.set_bounds(Rect::new(280., 0., 240., 120.).unwrap())?;
    let next = b.submit(scene(0xff00_00ff))?;
    let until = Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        let snapshot = snapshot(&inner, &[2]);
        if snapshot.sources.iter().any(|s| {
            s.retained_vector.iter().any(|(kind, body)| {
                *kind == messages::VECTOR_FRAME
                    && vivid_protocol::vector::Frame::decode(body)
                        .is_ok_and(|f| f.revision == next.revision())
            })
        }) {
            break snapshot;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    };
    let projection = snapshot.bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    for source in snapshot.sources {
        for (kind, body) in source.retained_vector {
            let original = vivid_protocol::vector::Frame::decode(&body).unwrap();
            let OverlayPresentationCapture::Forwarded(receipt) =
                originals.capture(source.key, original.epoch, original.revision)?
            else {
                panic!("peer successor was not reserved")
            };
            relay.media_chunk_with_presentation(
                MediaChunk {
                    delivery_id: 3,
                    source: source.key,
                    record_type: kind,
                    offset: 0,
                    total: u32::try_from(body.len()).unwrap(),
                    last: true,
                    bytes: body.to_vec(),
                },
                3,
            )?;
            let frame = ready(&mut host, &mut relay, 1, 2)?;
            assert!((frame.drawings()[0].bounds.origin.x.get() - 280.).abs() < 0.001);
            assert_eq!(next.wait(Duration::ZERO)?, None);
            assert!(host.presented(&frame)?);
            let until = Instant::now() + Duration::from_secs(5);
            loop {
                relay.take_overlay_input()?;
                if let Some(report) = relay.take_presentation_reports().pop() {
                    assert_eq!(
                        (report.correlation, report.source, report.outcome),
                        (3, source.key, Some(PresentationOutcome::Presented))
                    );
                    assert!(receipt.complete(report.outcome.unwrap()));
                    break;
                }
                assert!(Instant::now() < until);
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
    assert_eq!(
        next.wait(Duration::from_secs(2))?,
        Some(PresentationOutcome::Presented)
    );
    Ok(())
}

#[test]
fn changed_partial_correlation_abandons_only_that_owner_and_the_peer_still_presents()
-> io::Result<()> {
    let (inner, endpoint) = overlay_presenter()?;
    inner.enable_overlay_host_relay();
    let originals = inner.attach_overlay_presentation_relay()?;
    let (outer, outer_endpoint) = overlay_presenter()?;
    let outer = Arc::new(outer);
    let mut host = NativeVectorHost::attach(Arc::clone(&outer), "")?;
    let mut relay = bridge(&outer_endpoint, &pane(&outer, 9)?)?;
    let a = overlay_producer(&endpoint, &pane(&inner, 1)?, "partial-a")?;
    let b = overlay_producer(&endpoint, &pane(&inner, 2)?, "partial-b")?;
    let windows = [window_of(&a, 0., 0.)?, window_of(&b, 260., 0.)?];
    let pending = [
        windows[0].submit(scene(0x00ff_00ff))?,
        windows[1].submit(scene(0x0000_ffff))?,
    ];
    let until = Instant::now() + Duration::from_secs(5);
    let snapshot = loop {
        let snapshot = snapshot(&inner, &[1, 2]);
        if snapshot.sources.len() == 2
            && snapshot.sources.iter().all(|s| {
                s.retained_vector
                    .iter()
                    .any(|(kind, _)| *kind == messages::VECTOR_FRAME)
            })
        {
            break snapshot;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    };
    let projection = snapshot.bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    let mut copies = HashMap::new();
    let mut chunks = Vec::new();
    for (i, source) in snapshot.sources.into_iter().enumerate() {
        let correlation = u64::try_from(i).unwrap() + 1;
        let (kind, body) = source
            .retained_vector
            .into_iter()
            .find(|(kind, _)| *kind == messages::VECTOR_FRAME)
            .unwrap();
        let frame = vivid_protocol::vector::Frame::decode(&body).unwrap();
        let OverlayPresentationCapture::Forwarded(receipt) =
            originals.capture(source.key, frame.epoch, frame.revision)?
        else {
            panic!("partial original unavailable")
        };
        copies.insert(source.key, receipt);
        let half = body.len() / 2;
        assert!(!relay.media_chunk_with_presentation(
            MediaChunk {
                delivery_id: correlation,
                source: source.key,
                record_type: kind,
                offset: 0,
                total: u32::try_from(body.len()).unwrap(),
                last: false,
                bytes: body[..half].to_vec()
            },
            correlation
        )?);
        chunks.push(MediaChunk {
            delivery_id: correlation,
            source: source.key,
            record_type: kind,
            offset: u32::try_from(half).unwrap(),
            total: u32::try_from(body.len()).unwrap(),
            last: true,
            bytes: body[half..].to_vec(),
        });
    }
    let first = chunks.remove(0);
    let first_key = first.source;
    assert_eq!(
        relay
            .media_chunk_with_presentation(first, 3)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    let reports = relay.take_presentation_reports();
    assert_eq!(reports.len(), 1);
    assert_eq!(
        (
            reports[0].correlation,
            reports[0].source,
            reports[0].outcome
        ),
        (1, first_key, None)
    );
    copies.remove(&first_key).unwrap().abandon();
    let peer = chunks.remove(0);
    let peer_key = peer.source;
    assert!(relay.media_chunk_with_presentation(peer, 2)?);
    let frame = ready(&mut host, &mut relay, 1, 1)?;
    for receipt in &pending {
        assert_eq!(receipt.wait(Duration::ZERO)?, None);
    }
    assert!(host.presented(&frame)?);
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        relay.take_overlay_input()?;
        if let Some(report) = relay.take_presentation_reports().pop() {
            assert_eq!(
                (report.correlation, report.source, report.outcome),
                (2, peer_key, Some(PresentationOutcome::Presented))
            );
            assert!(
                copies
                    .remove(&peer_key)
                    .unwrap()
                    .complete(report.outcome.unwrap())
            );
            break;
        }
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    let status = [
        pending[0].wait(Duration::from_secs(1))?,
        pending[1].wait(Duration::from_secs(1))?,
    ];
    assert_eq!(
        status
            .iter()
            .filter(|v| **v == Some(PresentationOutcome::Presented))
            .count(),
        1
    );
    assert_eq!(status.iter().filter(|v| v.is_none()).count(), 1);
    // Cancellation must reject the last chunk of an already reserved assembly,
    // not merely prevent a new reservation at another source.
    let source = super::snapshot(&inner, &[1, 2])
        .sources
        .into_iter()
        .find(|s| s.key == first_key)
        .unwrap();
    let (kind, body) = source
        .retained_vector
        .into_iter()
        .find(|(kind, _)| *kind == messages::VECTOR_FRAME)
        .unwrap();
    let original = vivid_protocol::vector::Frame::decode(&body).unwrap();
    let OverlayPresentationCapture::Forwarded(copy) =
        originals.capture(first_key, original.epoch, original.revision)?
    else {
        panic!("canceled original unavailable")
    };
    let half = body.len() / 2;
    assert!(!relay.media_chunk_with_presentation(
        MediaChunk {
            delivery_id: 4,
            source: first_key,
            record_type: kind,
            offset: 0,
            total: u32::try_from(body.len()).unwrap(),
            last: false,
            bytes: body[..half].to_vec()
        },
        4
    )?);
    relay.cancel_handle().cancel();
    let reports = relay.take_presentation_reports();
    assert_eq!(reports.len(), 1);
    assert_eq!((reports[0].correlation, reports[0].outcome), (4, None));
    assert_eq!(
        relay
            .media_chunk_with_presentation(
                MediaChunk {
                    delivery_id: 4,
                    source: first_key,
                    record_type: kind,
                    offset: u32::try_from(half).unwrap(),
                    total: u32::try_from(body.len()).unwrap(),
                    last: true,
                    bytes: body[half..].to_vec()
                },
                4
            )
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    copy.abandon();
    assert!(relay.take_presentation_reports().is_empty());
    Ok(())
}
