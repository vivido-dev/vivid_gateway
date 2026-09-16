//! Real-socket scaffolding shared by the gateway's integration tests.
//!
//! A gateway test is only worth running over a real transport: the ordering, credit and handshake
//! behaviour these tests exist to pin down lives in the socket path, not in the state snapshots.

use std::io;
use std::net::{Shutdown, TcpListener};
use std::sync::Arc;

use vivid_gateway::{ConnectionCancel, PresenterListener, Transport};
use vivid_sdk::ProducerConfig;

/// A loopback TCP listener a presenter can accept on, chosen over a Unix socket so the suite runs
/// unchanged where socket files are not permitted.
pub struct TcpPresenterListener {
    listener: TcpListener,
    endpoint: String,
}

impl TcpPresenterListener {
    pub fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let endpoint = format!("tcp:{}", listener.local_addr()?);
        Ok(Self { listener, endpoint })
    }
}

impl PresenterListener for TcpPresenterListener {
    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn accept(&self) -> io::Result<Transport> {
        let (stream, _) = self.listener.accept()?;
        stream.set_nodelay(true)?;
        let reader = stream.try_clone()?;
        let timeout_stream = stream.try_clone()?;
        let cancel_stream = stream.try_clone()?;
        Ok(Transport::new(
            Box::new(reader),
            Box::new(stream),
            ConnectionCancel::new(move || {
                let _ = cancel_stream.shutdown(Shutdown::Both);
            }),
            Arc::new(move |timeout| timeout_stream.set_read_timeout(timeout)),
        ))
    }
}

/// Send every lane of one producer to the presenter under test.
///
/// Without this an ambient `VIVID_ENDPOINT*` in the environment can redirect a session's lanes
/// somewhere else, and the test passes or fails for a reason that has nothing to do with it.
pub fn pin_endpoints(producer: &mut ProducerConfig, endpoint: String) {
    producer.endpoint_control = Some(endpoint.clone());
    producer.endpoint_interactive = Some(endpoint.clone());
    producer.endpoint_realtime = Some(endpoint.clone());
    producer.endpoint_bulk = Some(endpoint);
}
