use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct Transport {
    pub reader: Box<dyn Read + Send>,
    pub writer: Box<dyn Write + Send>,
    cancel: ConnectionCancel,
    pub(crate) timeout: Arc<dyn Fn(Option<Duration>) -> io::Result<()> + Send + Sync>,
    pub(crate) deadline: Arc<Mutex<Option<Instant>>>,
}

/// Product-provided accepted-connection listener used by the inner presenter.
pub trait PresenterListener: Send + 'static {
    fn endpoint(&self) -> String;
    fn accept(&self) -> io::Result<Transport>;
}

impl Transport {
    pub fn new(
        reader: Box<dyn Read + Send>,
        writer: Box<dyn Write + Send>,
        cancel: ConnectionCancel,
        timeout: Arc<dyn Fn(Option<Duration>) -> io::Result<()> + Send + Sync>,
    ) -> Self {
        let deadline = Arc::new(Mutex::new(None));
        let reader = Box::new(DeadlineReader {
            inner: reader,
            timeout: timeout.clone(),
            deadline: deadline.clone(),
        });
        Self {
            reader,
            writer,
            cancel,
            timeout,
            deadline,
        }
    }

    pub fn cancel(&self) -> ConnectionCancel {
        self.cancel.clone()
    }

    pub fn set_read_deadline(&self, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "deadline is too large"))?;
        *self
            .deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(deadline);
        (self.timeout)(Some(timeout))
    }
}

struct DeadlineReader {
    inner: Box<dyn Read + Send>,
    timeout: Arc<dyn Fn(Option<Duration>) -> io::Result<()> + Send + Sync>,
    deadline: Arc<Mutex<Option<Instant>>>,
}

impl Read for DeadlineReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let deadline = *self
            .deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Vivid handshake deadline expired",
                ));
            }
            (self.timeout)(Some(remaining))?;
        }
        self.inner.read(buffer)
    }
}

#[derive(Clone)]
pub struct ConnectionCancel {
    inner: Arc<CancelInner>,
}

struct CancelInner {
    cancelled: AtomicBool,
    callback: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl ConnectionCancel {
    pub fn new(callback: impl FnOnce() + Send + 'static) -> Self {
        Self {
            inner: Arc::new(CancelInner {
                cancelled: AtomicBool::new(false),
                callback: Mutex::new(Some(Box::new(callback))),
            }),
        }
    }

    pub fn inert() -> Self {
        Self::new(|| {})
    }

    pub fn cancel(&self) {
        if self.inner.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(callback) = self
            .inner
            .callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            callback();
        }
    }
}

impl Drop for CancelInner {
    fn drop(&mut self) {
        if !self.cancelled.swap(true, Ordering::AcqRel)
            && let Some(callback) = self
                .callback
                .get_mut()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
        {
            callback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SlowBytes(&'static [u8]);

    impl Read for SlowBytes {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.0.is_empty() || buffer.is_empty() {
                return Ok(0);
            }
            std::thread::sleep(Duration::from_millis(20));
            buffer[0] = self.0[0];
            self.0 = &self.0[1..];
            Ok(1)
        }
    }

    #[test]
    fn read_deadline_is_absolute_across_partial_reads() {
        let transport = Transport::new(
            Box::new(SlowBytes(b"abc")),
            Box::new(io::sink()),
            ConnectionCancel::inert(),
            Arc::new(|_| Ok(())),
        );
        transport
            .set_read_deadline(Duration::from_millis(30))
            .unwrap();
        let mut reader = transport.reader;
        let mut bytes = [0; 3];
        assert_eq!(
            reader.read_exact(&mut bytes).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
    }
}
