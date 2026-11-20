//! First overlay lane after serviced target changes, followed by a live resize.

// Rust guideline compliant 2026-10-07
use super::*;
use vivid_sdk::presenter::VectorFrame;

fn service_geometry(relay: &mut OuterBridge, columns: u16, rows: u16) -> io::Result<()> {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        relay.service_session_events()?;
        let current = relay.display_metrics();
        if current.columns == columns && current.rows == rows {
            return Ok(());
        }
        assert!(Instant::now() < end, "target change was not serviced");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn current_frame(
    host: &mut NativeVectorHost,
    relay: &mut OuterBridge,
    dimensions: (u32, u32),
) -> io::Result<VectorFrame> {
    let end = Instant::now() + Duration::from_secs(3);
    loop {
        relay.retry_pending_activation()?;
        let frame = host.prepare(9, 1.)?;
        if frame.drawings().len() == 2 && frame.dimensions() == dimensions {
            return Ok(frame);
        }
        assert!(
            Instant::now() < end,
            "late-lane composition stalled: {frame:?}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
#[test]
fn first_overlay_lane_after_serviced_target_changes_keeps_current_snapshots_and_peer()
-> io::Result<()> {
    exercise(|_, _| Ok(()))
}
fn exercise(mut render: impl FnMut(&str, &VectorFrame) -> io::Result<()>) -> io::Result<()> {
    let (inner, outer, mut host, mut relay) = fixture_mode(true)?;
    let root = relay.outer_session_id();
    // The gateway is already authenticated but has no overlay windows or lane.
    // Service each native display change before projecting the first window.
    for (columns, rows) in [(48, 6), (42, 5), (40, 4)] {
        outer.update_metrics(9, columns, rows, CELL);
        service_geometry(&mut relay, columns, rows)?;
    }
    let mut a = Producer::new(&inner, 1, false)?;
    let mut b = Producer::new(&inner, 2, false)?;
    assert_eq!(a.surface.id(), b.surface.id());
    assert_eq!(a.track.id(), b.track.id());
    assert_ne!(a.session.info().session_id, b.session.info().session_id);
    a.paint(1, 0xff00_00ff)?;
    forward_unshown(&inner, &mut relay, &a, 1)?;
    b.paint(1, 0x0000_ffff)?;
    forward_unshown(&inner, &mut relay, &b, 1)?;
    let before_keys = snapshot(&inner, &[1, 2])
        .sources
        .iter()
        .map(|s| s.key)
        .collect::<HashSet<_>>();
    let physical = [
        relay.outer_surface_id(a.key()),
        relay.outer_surface_id(b.key()),
    ];
    let initial = current_frame(&mut host, &mut relay, (320, 64))?;
    render("initial", &initial)?;
    assert_eq!(
        (a.status()?.scene_revision, b.status()?.scene_revision),
        (0, 0)
    );
    assert!(host.presented(&initial)?);
    wait(&inner, &mut relay, || {
        a.status().is_ok_and(|s| s.scene_revision == 1)
            && b.status().is_ok_and(|s| s.scene_revision == 1)
    })?;
    let old_input = host.input_target(&initial).unwrap();
    outer.update_metrics(9, 44, 5, CELL);
    service_geometry(&mut relay, 44, 5)?;
    assert!(!host.presented(&initial)?);
    assert_eq!(
        pointer(&mut host, &old_input, 20.)?,
        VectorInputResult::Rejected
    );
    project(&inner, &mut relay)?;
    let resized = current_frame(&mut host, &mut relay, (352, 80))?;
    render("resized", &resized)?;
    assert!(host.presented(&resized)?);
    input(&inner, &mut relay)?;
    let fresh_input = host.input_target(&resized).unwrap();
    for down in [true, false] {
        assert!(matches!(
            host.dispatch_input(
                &fresh_input,
                VectorInputEvent::Pointer {
                    position: [20., 10.],
                    button: Some((0, down)),
                    modifiers: 0,
                }
            )?,
            VectorInputResult::Delivered { .. } | VectorInputResult::Deferred,
        ));
    }
    // A click may raise/focus its window. Admit the corresponding image before
    // retrying any queued held report; never resubmit a Deferred transition.
    drop(resized);
    let resized = current_frame(&mut host, &mut relay, (352, 80))?;
    render("focused", &resized)?;
    assert!(host.presented(&resized)?);
    host.flush_input();
    let end = Instant::now() + Duration::from_secs(3);
    let mut buttons = Vec::new();
    while buttons.len() < 2 {
        input(&inner, &mut relay)?;
        if let Some(vivid_sdk::OverlayLaneEvent::Input(event)) =
            a.lane.wait_event(Duration::from_millis(1))?
            && let vivid_protocol::overlay::Event::Pointer {
                position,
                region,
                button: Some(button),
                ..
            } = event.event
        {
            assert_eq!(event.address, a.address());
            assert_eq!(event.scene_revision, 1);
            assert_eq!((position.x.get(), position.y.get(), region), (20., 10., 7));
            buttons.push(button);
        }
        assert!(
            Instant::now() < end,
            "fresh original pointer transitions did not arrive"
        );
    }
    assert_eq!(buttons, [(0, true), (0, false)]);
    while let Some(event) = b.lane.wait_event(Duration::ZERO)? {
        if let vivid_sdk::OverlayLaneEvent::Input(event) = event {
            assert!(
                !matches!(
                    event.event,
                    vivid_protocol::overlay::Event::Pointer {
                        button: Some(_),
                        ..
                    }
                ),
                "target-owner click reached the same-ID peer"
            );
        }
    }
    render("retired", &initial)?;
    assert!(!host.presented(&initial)?);
    assert_eq!(relay.outer_session_id(), root);
    assert_eq!(
        [
            relay.outer_surface_id(a.key()),
            relay.outer_surface_id(b.key())
        ],
        physical
    );
    assert_eq!(
        snapshot(&inner, &[1, 2])
            .sources
            .iter()
            .map(|s| s.key)
            .collect::<HashSet<_>>(),
        before_keys
    );
    assert_eq!(
        (a.status()?.scene_revision, b.status()?.scene_revision),
        (1, 1)
    );
    assert!(relay.take_source_losses().is_empty());
    drop((initial, resized));
    a.lane.close()?;
    b.lane.close()?;
    a.session.close()?;
    b.session.close()?;
    drop(relay);
    outer.shutdown();
    inner.shutdown();
    let end = Instant::now() + Duration::from_secs(3);
    while inner.active_worker_count() != 0 || outer.active_worker_count() != 0 {
        assert!(Instant::now() < end, "late-lane presenter workers remained");
        std::thread::sleep(Duration::from_millis(1));
    }
    eprintln!(
        "Verified first overlay lane after three serviced target changes, live resize, original outcomes, same-ID peer/root/source isolation and zero presenter workers."
    );
    Ok(())
}

#[cfg(feature = "native-gpu-testing")]
#[test]
#[ignore = "requires native GPU; first overlay lane after serviced target changes"]
fn native_gpu_first_overlay_lane_after_serviced_target_changes_preserves_peer() -> io::Result<()> {
    use vivid_sdk::vector_renderer::compositor::offscreen::Offscreen;
    let mut gpu = pollster::block_on(Offscreen::new())?;
    let mut initial = None;
    let mut cached = None;
    exercise(|stage, frame| {
        let (width, height) = frame.dimensions();
        if gpu.render(frame.drawings(), width, height)? {
            let end = Instant::now() + Duration::from_secs(10);
            cached = Some(loop {
                if let Some(pixels) = gpu.poll()? {
                    break pixels;
                }
                assert!(Instant::now() < end, "late-lane readback stalled");
                std::thread::sleep(Duration::from_millis(1));
            });
        }
        let pixels = cached
            .as_ref()
            .expect("completed or cached immutable readback");
        assert_eq!((pixels.width, pixels.height), (width, height));
        for (x, y, expected) in [
            (20, 20, [255, 0, 0, 255]),
            (84, 20, [0, 0, 255, 255]),
            (60, 20, [0; 4]),
            (120, 20, [0; 4]),
            (20, 60, [0; 4]),
        ] {
            let at = (y * width as usize + x) * 4;
            assert_eq!(
                &pixels.rgba[at..at + 4],
                &expected,
                "late-lane owner pixels changed"
            );
        }
        if stage == "initial" {
            initial = Some(Arc::clone(&pixels.rgba));
        }
        if stage == "retired" {
            assert!(
                pixels.rgba == *initial.as_ref().unwrap(),
                "retired immutable image changed"
            );
        }
        if let Some(directory) = std::env::var_os("VIVID_LATE_LANE_OUTPUT_DIR") {
            let directory = std::path::Path::new(&directory);
            std::fs::create_dir_all(directory)?;
            std::fs::write(
                directory.join(format!("{stage}-{width}x{height}.rgba")),
                &pixels.rgba,
            )?;
        }
        Ok(())
    })
}
