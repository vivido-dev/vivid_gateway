//! Real TCP delivery stalls must not defeat carrier cancellation or independent owners.

// Rust guideline compliant 2026-10-07
use super::*;
use std::{
    io::{Read, Write},
    net::{Shutdown, TcpStream},
    sync::{Arc, Condvar, Mutex},
};
use vivid_protocol::wire::Connection;
use vivid_sdk::{ConnectionFactory, ConnectionKind};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stall {
    None,
    ControlRead,
    TrackWrite,
}
struct State {
    mode: Stall,
    entered: bool,
    cancelled: bool,
}
struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}
impl Gate {
    fn wait(&self, mode: Stall) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        while state.mode == mode && !state.cancelled {
            state.entered = true;
            self.changed.notify_all();
            state = self.changed.wait(state).unwrap();
        }
        if state.cancelled {
            // Permanent I/O cancellation must not be the retryable Interrupted error.
            Err(io::ErrorKind::ConnectionAborted.into())
        } else {
            Ok(())
        }
    }
    fn arm(&self, mode: Stall) {
        let mut state = self.state.lock().unwrap();
        state.mode = mode;
        state.entered = false;
    }
    fn entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut state = self.state.lock().unwrap();
        while !state.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "TCP delivery did not enter the requested stall"
            );
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
}
struct Reader {
    stream: TcpStream,
    gate: Arc<Gate>,
    control: bool,
}
impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = self.stream.read(buf)?;
        // Gate after the real read so an already-blocked read cannot leak one reply.
        if self.control && count != 0 {
            self.gate.wait(Stall::ControlRead)?;
        }
        Ok(count)
    }
}
struct Writer {
    stream: TcpStream,
    gate: Arc<Gate>,
    track: bool,
}
impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.track {
            self.gate.wait(Stall::TrackWrite)?;
        }
        self.stream.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}
struct Carrier {
    endpoint: String,
    gate: Arc<Gate>,
    sockets: Mutex<Vec<TcpStream>>,
}
impl Carrier {
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
            sockets: Mutex::new(Vec::new()),
        }
    }
}
impl ConnectionFactory for Carrier {
    fn open(&self, kind: ConnectionKind, _: Option<LaneClass>) -> io::Result<Connection> {
        let stream = TcpStream::connect(&self.endpoint)?;
        stream.set_nodelay(true)?;
        let mut sockets = self.sockets.lock().unwrap();
        if self.gate.state.lock().unwrap().cancelled || sockets.len() >= 32 {
            return Err(io::ErrorKind::Interrupted.into());
        }
        sockets.push(stream.try_clone()?);
        drop(sockets);
        Connection::from_streams(
            Reader {
                stream: stream.try_clone()?,
                gate: Arc::clone(&self.gate),
                control: kind == ConnectionKind::Control,
            },
            Writer {
                stream,
                gate: Arc::clone(&self.gate),
                track: kind == ConnectionKind::Track,
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
fn reconcile(inner: &VirtualVivid, relay: &mut OuterBridge, pane: u64) -> io::Result<()> {
    let projection = snapshot(inner, &[pane]).bridge_projection();
    relay.rebuild(&projection.surfaces, &projection.sources, &projection.nodes)?;
    Ok(())
}
fn frame(inner: &VirtualVivid, relay: &mut OuterBridge, pane: u64) -> io::Result<()> {
    let source = snapshot(inner, &[pane]).sources.into_iter().next().unwrap();
    let (kind, body) = source.retained_vector.last().unwrap();
    relay.media_chunk(MediaChunk {
        delivery_id: 0,
        source: source.key,
        record_type: *kind,
        offset: 0,
        total: u32::try_from(body.len()).unwrap(),
        last: true,
        bytes: body.to_vec(),
    })?;
    Ok(())
}
struct Cleanup {
    cancel: vivid_gateway::BridgeCancel,
    drop_tx: std::sync::mpsc::SyncSender<()>,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.cancel.cancel();
        let _ = self.drop_tx.try_send(());
    }
}
fn stalled_transport(mode: Stall) -> io::Result<()> {
    let (inner, endpoint) = overlay_presenter()?;
    let first = overlay_producer(&endpoint, &pane(&inner, 1)?, "stalled")?;
    let second = overlay_producer(&endpoint, &pane(&inner, 2)?, "peer")?;
    let a = window_of(&first, 0., 0.)?;
    let b = window_of(&second, 260., 0.)?;
    for window in [&a, &b] {
        assert_eq!(
            window.submit(scene(1))?.wait(Duration::from_secs(3))?,
            Some(vivid_sdk::overlay::PresentationOutcome::Presented)
        );
    }
    let (outer, endpoint) = overlay_presenter()?;
    let carrier = Arc::new(Carrier::new(&endpoint));
    let mut blocked = OuterBridge::builder(
        Secret32::from_hex(&pane(&outer, 9)?).map_err(io::Error::other)?,
        DisplayMetrics::default(),
    )
    .connection_factory(Arc::<Carrier>::clone(&carrier))
    .build()?;
    let mut peer = bridge(&endpoint, &pane(&outer, 10)?)?;
    let keys = [1, 2].map(|pane| snapshot(&inner, &[pane]).bridge_projection().surfaces[0].key);
    assert_eq!(
        (keys[0].context, keys[0].surface),
        (keys[1].context, keys[1].surface)
    );
    assert_ne!(keys[0].producer, keys[1].producer);
    reconcile(&inner, &mut blocked, 1)?;
    reconcile(&inner, &mut peer, 2)?;
    for (relay, pane) in [(&mut blocked, 1), (&mut peer, 2)] {
        frame(&inner, relay, pane)?;
        finish_delivery(&inner, relay, 0)?;
        relay.retry_pending_activation()?;
    }
    let peer_surface = peer.outer_surface_id(keys[1]);
    let cancel = blocked.cancel_handle();
    carrier.gate.arm(mode);
    std::thread::scope(|scope| -> io::Result<()> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let (drop_tx, drop_rx) = std::sync::mpsc::sync_channel(1);
        let _cleanup = Cleanup {
            cancel: cancel.clone(),
            drop_tx: drop_tx.clone(),
        };
        let inner_ref = &inner;
        let worker = if mode == Stall::ControlRead {
            a.set_bounds(Rect::new(8., 8., 240., 120.).unwrap())?;
            Some(scope.spawn(move || {
                assert!(reconcile(inner_ref, &mut blocked, 1).is_err());
                assert_eq!(
                    blocked.replace_session(&[], &[], &[]).unwrap_err().kind(),
                    io::ErrorKind::Interrupted
                );
                drop(blocked);
                tx.send(()).unwrap();
            }))
        } else {
            frame(&inner, &mut blocked, 1)?;
            carrier.gate.entered();
            // Control still crosses the socket while this track writer is held.
            a.set_bounds(Rect::new(8., 8., 240., 120.).unwrap())?;
            reconcile(&inner, &mut blocked, 1)?;
            let source = snapshot(&inner, &[1]).sources[0].key;
            let mut queued = 0;
            while blocked.can_accept_media(source) && queued < 64 {
                frame(&inner, &mut blocked, 1)?;
                queued += 1;
            }
            assert!(
                !blocked.can_accept_media(source),
                "stalled track escaped its queue bound"
            );
            // Move Drop onto the worker too: cancellation must unblock writer retirement.
            Some(scope.spawn(move || {
                drop_rx.recv().unwrap();
                drop(blocked);
                tx.send(()).unwrap();
            }))
        };
        carrier.gate.entered();
        b.set_bounds(Rect::new(264., 4., 240., 120.).unwrap())?;
        reconcile(&inner, &mut peer, 2)?;
        assert_eq!(
            b.submit(scene(2))?.wait(Duration::from_secs(3))?,
            Some(vivid_sdk::overlay::PresentationOutcome::Presented)
        );
        frame(&inner, &mut peer, 2)?;
        finish_delivery(&inner, &mut peer, 0)?;
        assert_eq!(peer.outer_surface_id(keys[1]), peer_surface);
        let start = Instant::now();
        cancel.cancel();
        let _ = drop_tx.send(());
        // Both the foreground read and the background write must stop without
        // waiting for the ordinary host RPC deadline or another peer response.
        rx.recv_timeout(Duration::from_secs(2))
            .expect("cancelled bridge did not retire");
        assert!(start.elapsed() < Duration::from_secs(2));
        worker.unwrap().join().unwrap();
        Ok(())
    })
}
#[test]
fn a_blackholed_control_read_cancels_without_stopping_a_same_id_peer() -> io::Result<()> {
    stalled_transport(Stall::ControlRead)
}
#[test]
fn a_blackholed_track_write_leaves_control_live_and_cancels_within_its_owner() -> io::Result<()> {
    stalled_transport(Stall::TrackWrite)
}

#[path = "transport_kernel.rs"]
mod kernel;
