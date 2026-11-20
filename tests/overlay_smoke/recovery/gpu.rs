//! Native GPU readback through source recovery and outer root replacement.

// Rust guideline compliant 2026-10-07
use std::{
    io,
    time::{Duration, Instant},
};
use vivid_sdk::vector_renderer::compositor::offscreen::Offscreen;

#[test]
#[ignore = "requires a native GPU; explicit source recovery readback qualification"]
fn native_gpu_fences_retired_source_frames_and_preserves_same_id_peer() -> io::Result<()> {
    let mut gpu = pollster::block_on(Offscreen::new())?;
    super::unresolved_scenes(|frame, left| readback(&mut gpu, frame, left))
}

#[test]
#[ignore = "requires a native GPU; explicit outer root replacement readback qualification"]
fn native_gpu_replays_retained_scenes_after_outer_tcp_loss() -> io::Result<()> {
    let mut gpu = pollster::block_on(Offscreen::new())?;
    super::outer::retained_replacements(|frame, left| readback(&mut gpu, frame, left))
}

fn readback(
    gpu: &mut Offscreen,
    frame: &vivid_sdk::presenter::VectorFrame,
    left: [u8; 4],
) -> io::Result<()> {
    assert!(gpu.render(frame.drawings(), 128, 128)?);
    let end = Instant::now() + Duration::from_secs(10);
    let pixels = loop {
        if let Some(pixels) = gpu.poll()? {
            break pixels;
        }
        assert!(Instant::now() < end, "recovery GPU readback stalled");
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!((pixels.width, pixels.height), (128, 128));
    assert_eq!(pixels.rgba.len(), 128 * 128 * 4);
    let pixel = |x: usize| {
        let offset = (20 * 128 + x) * 4;
        &pixels.rgba[offset..offset + 4]
    };
    assert_eq!(pixel(20), left, "wrong source incarnation rendered");
    assert_eq!(pixel(80), [0, 0, 255, 255], "same-ID peer pixels changed");
    assert_eq!(pixel(60), [0; 4], "recovery scene escaped window bounds");
    Ok(())
}
