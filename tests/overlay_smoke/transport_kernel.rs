//! A non-reading TCP peer must not strand a native socket writer or another owner.

// Rust guideline compliant 2026-10-07
use super::*;
use std::{
    net::TcpListener,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    thread::JoinHandle,
};

// Bound the fixture independently of the gateway's own source/queue budgets.
const MAX_SOCKETS: usize = 32;
// Include proxy prefetch in both setup and live socket observations. A held
// native syscall, not the setup buffer sizes, proves kernel backpressure.
const COPY_BYTES: usize = 4 * 1024;
// Request buffers below one bounded 256-KiB record. Verify live capacity
// again while the same native write is held; setup values alone are insufficient.
const SOCKET_BYTES: usize = 16 * 1024;
// macOS loopback's large segments can grow receiver storage after SO_RCVBUF
// is set. Bound this private fixture's segment size on connected sockets.
#[cfg(target_os = "macos")]
const SEGMENT_BYTES: u32 = 1024;
// Outlast short native buffering/ACK pauses without extending the five-second
// witness deadline or the independent two-second cancellation bound.
const WRITE_HOLD: Duration = Duration::from_millis(500);

#[derive(Default)]
struct Writes {
    active: AtomicBool,
    completed_bytes: AtomicU64,
    calls: AtomicU64,
    errors: AtomicU64,
    send_capacity: AtomicU64,
    recv_capacity: AtomicU64,
}
struct NativeWriter {
    stream: TcpStream,
    writes: Arc<Writes>,
}
impl Write for NativeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writes.calls.fetch_add(1, Ordering::AcqRel);
        self.writes.send_capacity.fetch_max(
            u64::try_from(socket2::SockRef::from(&self.stream).send_buffer_size()?).unwrap(),
            Ordering::AcqRel,
        );
        self.writes.active.store(true, Ordering::Release);
        // No condition variable, credit wait or injected error here: only the
        // operating system's blocking write can keep this flag set.
        let result = self.stream.write(buf);
        if let Ok(count) = result {
            self.writes
                .completed_bytes
                .fetch_add(u64::try_from(count).unwrap(), Ordering::AcqRel);
        }
        self.writes.active.store(false, Ordering::Release);
        if result.is_err() {
            self.writes.errors.fetch_add(1, Ordering::AcqRel);
        }
        self.writes.send_capacity.fetch_max(
            u64::try_from(
                socket2::SockRef::from(&self.stream)
                    .send_buffer_size()
                    .unwrap_or(0),
            )
            .unwrap(),
            Ordering::AcqRel,
        );
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}
struct KernelCarrier {
    endpoint: String,
    gate: Arc<Gate>,
    writes: Arc<Writes>,
    native_pair: Mutex<Option<(TcpStream, TcpStream)>>,
    sockets: Mutex<Vec<TcpStream>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}
impl KernelCarrier {
    fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.strip_prefix("tcp:").unwrap().into(),
            gate: Arc::new(Gate {
                state: Mutex::new(State {
                    mode: Stall::None,
                    entered: false,
                    cancelled: false,
                }),
                changed: Condvar::new(),
            }),
            writes: Arc::default(),
            native_pair: Mutex::default(),
            sockets: Mutex::default(),
            workers: Mutex::default(),
        }
    }
    fn retain(&self, streams: &[&TcpStream]) -> io::Result<()> {
        let mut sockets = self.sockets.lock().unwrap();
        if self.gate.state.lock().unwrap().cancelled
            || sockets.len().saturating_add(streams.len()) > MAX_SOCKETS
        {
            return Err(io::ErrorKind::ConnectionAborted.into());
        }
        for stream in streams {
            sockets.push(stream.try_clone()?);
        }
        Ok(())
    }
    fn live_buffers(&self) -> io::Result<(usize, usize)> {
        let pair = self.native_pair.lock().unwrap();
        let (sender, receiver) = pair.as_ref().expect("one observed native track");
        Ok((
            socket2::SockRef::from(sender).send_buffer_size()?,
            socket2::SockRef::from(receiver).recv_buffer_size()?,
        ))
    }
    fn observation(&self) -> String {
        format!(
            "active={}, completed={}, calls={}, errors={}, send_buffer={}, receive_buffer={}",
            self.writes.active.load(Ordering::Acquire),
            self.writes.completed_bytes.load(Ordering::Acquire),
            self.writes.calls.load(Ordering::Acquire),
            self.writes.errors.load(Ordering::Acquire),
            self.writes.send_capacity.load(Ordering::Acquire),
            self.writes.recv_capacity.load(Ordering::Acquire)
        )
    }
    fn blocked_write(&self) -> (u64, u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.writes.active.load(Ordering::Acquire)
                && deadline.saturating_duration_since(Instant::now()) >= WRITE_HOLD
            {
                let completed = self.writes.completed_bytes.load(Ordering::Acquire);
                let call = self.writes.calls.load(Ordering::Acquire);
                // Observe an actual native write, not just a full gateway queue
                // or a sender parked on authenticated protocol credit.
                std::thread::sleep(WRITE_HOLD);
                if self.writes.active.load(Ordering::Acquire)
                    && self.writes.completed_bytes.load(Ordering::Acquire) == completed
                    && self.writes.calls.load(Ordering::Acquire) == call
                    && Instant::now() < deadline
                {
                    return (call, completed);
                }
            }
            assert!(
                Instant::now() < deadline,
                "native TCP writer never blocked: {}",
                self.observation()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}
impl ConnectionFactory for KernelCarrier {
    fn open(&self, kind: ConnectionKind, _: Option<LaneClass>) -> io::Result<Connection> {
        let upstream = TcpStream::connect(&self.endpoint)?;
        upstream.set_nodelay(true)?;
        if kind != ConnectionKind::Track {
            self.retain(&[&upstream])?;
            return Connection::from_streams(upstream.try_clone()?, upstream, kind);
        }
        let listener = TcpListener::bind("127.0.0.1:0")?;
        // Request a small initial receive window before SYN. Connected-socket
        // tuning and the live observation account for subsequent buffer growth.
        socket2::SockRef::from(&listener).set_recv_buffer_size(SOCKET_BYTES)?;
        let client = TcpStream::connect(listener.local_addr()?)?;
        let (mut receiver, _) = listener.accept()?;
        client.set_nodelay(true)?;
        receiver.set_nodelay(true)?;
        #[cfg(target_os = "macos")]
        for stream in [&client, &receiver] {
            let socket = socket2::SockRef::from(stream);
            let segment = socket.tcp_mss()?.min(SEGMENT_BYTES);
            socket.set_tcp_mss(segment)?;
            assert_eq!(socket.tcp_mss()?, segment);
        }
        socket2::SockRef::from(&client).set_send_buffer_size(SOCKET_BYTES)?;
        socket2::SockRef::from(&receiver).set_recv_buffer_size(SOCKET_BYTES)?;
        self.retain(&[&client, &receiver, &upstream])?;
        let mut pair = self.native_pair.lock().unwrap();
        assert!(
            pair.is_none(),
            "fixture must observe only one native track writer"
        );
        *pair = Some((client.try_clone()?, receiver.try_clone()?));
        drop(pair);
        let mut remote_reader = upstream.try_clone()?;
        let mut response = receiver.try_clone()?;
        let gate = Arc::clone(&self.gate);
        let writes = Arc::clone(&self.writes);
        let mut remote_writer = upstream;
        let mut workers = self.workers.lock().unwrap();
        workers.push(std::thread::spawn(move || {
            let mut buffer = vec![0u8; COPY_BYTES].into_boxed_slice();
            loop {
                let Ok(count) = receiver.read(&mut buffer) else {
                    return;
                };
                // Reapply the requested size as the initial record drains.
                // macOS may still grow storage after this read; the live
                // observation checks capacity while the native write is held.
                if socket2::SockRef::from(&receiver)
                    .set_recv_buffer_size(SOCKET_BYTES)
                    .is_err()
                {
                    return;
                }
                writes.recv_capacity.store(
                    u64::try_from(
                        socket2::SockRef::from(&receiver)
                            .recv_buffer_size()
                            .unwrap_or(0),
                    )
                    .unwrap(),
                    Ordering::Release,
                );
                if count == 0
                    || gate.wait(Stall::TrackWrite).is_err()
                    || remote_writer.write_all(&buffer[..count]).is_err()
                {
                    return;
                }
            }
        }));
        workers.push(std::thread::spawn(move || {
            let _ = io::copy(&mut remote_reader, &mut response);
        }));
        drop(workers);
        Connection::from_streams(
            client.try_clone()?,
            NativeWriter {
                stream: client,
                writes: Arc::clone(&self.writes),
            },
            kind,
        )
    }
    fn cancel(&self) {
        let mut state = self.gate.state.lock().unwrap();
        state.cancelled = true;
        self.gate.changed.notify_all();
        drop(state);
        for stream in self.sockets.lock().unwrap().iter() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}
impl Drop for KernelCarrier {
    fn drop(&mut self) {
        self.cancel();
        for worker in self.workers.lock().unwrap().drain(..) {
            worker.join().unwrap();
        }
    }
}
fn large_scene() -> Canvas {
    let mut path = Path::builder().move_to(0.5, 0.5);
    // Two paths stay within the producer's 256-KiB record grant. Fractional
    // coordinates keep each encoded scalar wide enough to exceed the fixture's
    // native socket buffers with one valid authenticated vector record.
    for index in 0..4094 {
        path = path.line_to(f64::from(index % 180) + 0.25, f64::from(index % 90) + 0.75);
    }
    let path = path.close().build().unwrap();
    let mut canvas = Canvas::new();
    for _ in 0..2 {
        canvas
            .fill(path.clone(), Brush::Solid(Color(0xff33_5577)))
            .unwrap();
    }
    canvas
}
#[test]
fn native_tcp_backpressure_cancels_while_control_and_a_same_id_peer_progress() -> io::Result<()> {
    let (inner, endpoint) = overlay_presenter()?;
    let first = overlay_producer(&endpoint, &pane(&inner, 1)?, "native-stall")?;
    let second = overlay_producer(&endpoint, &pane(&inner, 2)?, "peer")?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    assert_eq!(
        a.submit(large_scene())?.wait(Duration::from_secs(3))?,
        Some(vivid_sdk::overlay::PresentationOutcome::Presented)
    );
    assert_eq!(
        b.submit(scene(1))?.wait(Duration::from_secs(3))?,
        Some(vivid_sdk::overlay::PresentationOutcome::Presented)
    );
    let (outer, endpoint) = overlay_presenter()?;
    let carrier = Arc::new(KernelCarrier::new(&endpoint));
    let mut blocked = OuterBridge::builder(
        Secret32::from_hex(&pane(&outer, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .connection_factory(Arc::<KernelCarrier>::clone(&carrier))
    .build()?;
    let mut peer = bridge(&endpoint, &pane(&outer, 10)?)?;
    let keys = [1, 2].map(|pane| snapshot(&inner, &[pane]).bridge_projection().surfaces[0].key);
    assert_eq!(
        (keys[0].context, keys[0].surface),
        (keys[1].context, keys[1].surface)
    );
    assert_ne!(keys[0].producer, keys[1].producer);
    for (relay, pane) in [(&mut blocked, 1), (&mut peer, 2)] {
        reconcile(&inner, relay, pane)?;
        frame(&inner, relay, pane)?;
        finish_delivery(&inner, relay, 0)?;
        let source = snapshot(&inner, &[pane]).sources[0].key;
        let track = relay.outer_track_id(source).unwrap();
        let activation = Instant::now() + Duration::from_secs(3);
        loop {
            relay.retry_pending_activation()?;
            if snapshot(&outer, &[pane + 8])
                .bridge_projection()
                .sources
                .iter()
                .any(|source| source.key.track == track && source.active)
            {
                break;
            }
            assert!(
                Instant::now() < activation,
                "native fixture track never activated"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        // Delivery and control activation can both finish while the proxy is
        // still forwarding the initial scene. Do not arm the media gate in the
        // middle of that initial frame: it must reach the actual outer receiver
        // before the pressure experiment starts and consumes fresh media credit.
        let ready_before_wait = snapshot(&outer, &[pane + 8])
            .sources
            .iter()
            .any(|source| source.key.track == track && source.retained.is_some());
        let (_, applied) = outer_frame(&outer, pane + 8, |source, _| source.track == track);
        assert_eq!(
            applied.canvas.commands().len(),
            if pane == 1 { 2 } else { 1 }
        );
        eprintln!(
            "Initial outer scene applied: pane={}, track={track}, ready_before_wait={ready_before_wait}",
            pane + 8
        );
    }
    let source_keys = [1, 2].map(|pane| snapshot(&inner, &[pane]).sources[0].key);
    assert_eq!(
        (
            source_keys[0].context,
            source_keys[0].surface,
            source_keys[0].track
        ),
        (
            source_keys[1].context,
            source_keys[1].surface,
            source_keys[1].track
        )
    );
    let source = snapshot(&inner, &[1]).sources.remove(0);
    let frame_bytes = source
        .retained_vector
        .iter()
        .find(|(kind, _)| *kind == vivid_protocol::messages::VECTOR_FRAME)
        .unwrap()
        .1
        .len();
    let send = carrier.writes.send_capacity.load(Ordering::Acquire);
    let receive = carrier.writes.recv_capacity.load(Ordering::Acquire);
    assert!(
        send != 0 && receive != 0,
        "native fixture buffers were not observed"
    );
    let capacity = send
        .checked_add(receive)
        .and_then(|bytes| bytes.checked_add(u64::try_from(COPY_BYTES).unwrap()))
        .expect("bounded native fixture capacity");
    assert!(
        u64::try_from(frame_bytes).unwrap() > capacity,
        "one vector frame must exceed native buffers plus proxy prefetch"
    );
    eprintln!(
        "Native TCP fixture: frame={frame_bytes} bytes, setup socket buffers plus prefetch={capacity} bytes"
    );
    let peer_surface = peer.outer_surface_id(keys[1]);
    let cancel = blocked.cancel_handle();
    std::thread::scope(|scope| -> io::Result<()> {
        let (drop_tx, drop_rx) = std::sync::mpsc::sync_channel(1);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(1);
        let _cleanup = Cleanup {
            cancel: cancel.clone(),
            drop_tx: drop_tx.clone(),
        };
        carrier.gate.arm(Stall::TrackWrite);
        let source = snapshot(&inner, &[1]).sources[0].key;
        let mut queued = 0;
        while blocked.can_accept_media(source) && queued < 32 {
            frame(&inner, &mut blocked, 1)?;
            queued += 1;
        }
        assert!(
            !blocked.can_accept_media(source),
            "native stall escaped the bounded media queue"
        );
        carrier.gate.entered();
        let (held_call, held_bytes) = carrier.blocked_write();
        let (live_send, live_receive) = carrier.live_buffers()?;
        let live_capacity = live_send
            .checked_add(live_receive)
            .and_then(|n| n.checked_add(COPY_BYTES))
            .expect("bounded live native buffers");
        assert!(
            frame_bytes > live_capacity,
            "native receiver outgrew the pressure frame: send={live_send}, receive={live_receive}, frame={frame_bytes}"
        );
        eprintln!(
            "Native TCP sustained witness: call={held_call}, completed={held_bytes}, live_send={live_send}, live_receive={live_receive}, frame={frame_bytes}"
        );
        a.set_bounds(Rect::new(8., 8., 240., 120.).unwrap())?;
        reconcile(&inner, &mut blocked, 1)?;
        assert!(
            carrier.writes.active.load(Ordering::Acquire)
                && carrier.writes.calls.load(Ordering::Acquire) == held_call
                && carrier.writes.completed_bytes.load(Ordering::Acquire) == held_bytes,
            "native write ended: {}",
            carrier.observation()
        );
        let worker = scope.spawn(move || {
            let _ = drop_rx.recv();
            drop(blocked);
            let _ = done_tx.send(());
        });
        // Prove the original control lane and a same-ID independent owner remain
        // usable while the stalled owner's actual write(2) is still outstanding.
        b.set_bounds(Rect::new(264., 4., 240., 120.).unwrap())?;
        reconcile(&inner, &mut peer, 2)?;
        assert_eq!(
            b.submit(scene(2))?.wait(Duration::from_secs(3))?,
            Some(vivid_sdk::overlay::PresentationOutcome::Presented)
        );
        frame(&inner, &mut peer, 2)?;
        finish_delivery(&inner, &mut peer, 0)?;
        // Observe the peer's receiver-applied scene, not only its local write.
        let peer_track = peer.outer_track_id(source_keys[1]).unwrap();
        let expected_peer = scene(2);
        outer_frame(&outer, 10, |source, applied| {
            source.track == peer_track && applied.canvas == expected_peer
        });
        assert_eq!(peer.outer_surface_id(keys[1]), peer_surface);
        assert!(
            carrier.writes.active.load(Ordering::Acquire)
                && carrier.writes.calls.load(Ordering::Acquire) == held_call
                && carrier.writes.completed_bytes.load(Ordering::Acquire) == held_bytes,
            "native write ended: {}",
            carrier.observation()
        );
        let start = Instant::now();
        cancel.cancel();
        let _ = drop_tx.send(());
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("native TCP cancellation did not retire the writer");
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(!carrier.writes.active.load(Ordering::Acquire));
        worker.join().unwrap();
        eprintln!(
            "Verified native TCP write blocked for at least 500 ms, {queued} bounded media handoffs, same-owner control and same-ID peer progress; cancellation retired the writer in {} ms.",
            start.elapsed().as_millis()
        );
        Ok(())
    })
}
