//! Accepted-side Vivid 1.5 framing for the per-session private presenter endpoint.

use std::io::{self, IoSlice, Read, Write};
use std::sync::{Arc, Mutex};

use vivid_protocol::wire::{
    ConnectionKind, HEADER_SIZE, PREFACE_SIZE, Preface, PrefaceClassification, RECORD_KNOWN_FLAGS,
    Record, RecordHeader,
};
use vivid_protocol::{CONTROL_MAX_RECORD_BODY, HARD_MAX_RECORD_BODY};

use crate::io::{ConnectionCancel, Transport};

pub struct Reader {
    reader: Box<dyn Read + Send>,
    writer: Arc<Writer>,
    timeout: Arc<dyn Fn(Option<std::time::Duration>) -> io::Result<()> + Send + Sync>,
    deadline: Arc<Mutex<Option<std::time::Instant>>>,
    _cancel: ConnectionCancel,
    negotiated_maximum: u32,
    maximum: u32,
    sequence: u64,
    first_record: bool,
}

impl Reader {
    pub fn new(mut stream: Transport) -> io::Result<(Self, Preface, [u8; PREFACE_SIZE])> {
        let cancel = stream.cancel();
        let mut bytes = [0_u8; PREFACE_SIZE];
        stream.reader.read_exact(&mut bytes)?;
        let preface = match Preface::classify(bytes)? {
            PrefaceClassification::Accepted(preface) => preface,
            PrefaceClassification::UnsupportedVersion(_) => {
                stream
                    .writer
                    .write_all(&vivid_protocol::wire::unsupported_version_record())?;
                stream.writer.flush()?;
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "unsupported Vivid protocol version",
                ));
            }
        };
        let maximum = preface.initiator_tx_body_limit.min(HARD_MAX_RECORD_BODY);
        let writer = Arc::new(Writer {
            inner: Mutex::new(WriterInner {
                stream: stream.writer,
                maximum: if preface.kind == ConnectionKind::Control {
                    CONTROL_MAX_RECORD_BODY
                } else {
                    HARD_MAX_RECORD_BODY
                },
                sequence: 0,
            }),
        });
        Ok((
            Self {
                reader: stream.reader,
                writer,
                timeout: stream.timeout,
                deadline: stream.deadline,
                _cancel: cancel,
                negotiated_maximum: maximum,
                maximum: if preface.kind == ConnectionKind::Control {
                    maximum.min(CONTROL_MAX_RECORD_BODY)
                } else {
                    maximum
                },
                sequence: 0,
                first_record: true,
            },
            preface,
            bytes,
        ))
    }

    pub fn read_record(&mut self, kind: ConnectionKind) -> io::Result<Record> {
        let mut body = Vec::new();
        let header = self.read_record_body_into(kind, &mut body)?;
        Ok(Record {
            record_type: header.record_type,
            flags: header.flags,
            object_id: header.object_id,
            sequence: header.sequence,
            body,
        })
    }

    fn read_record_body_into(
        &mut self,
        kind: ConnectionKind,
        body: &mut Vec<u8>,
    ) -> io::Result<RecordHeader> {
        let mut bytes = [0_u8; HEADER_SIZE];
        self.reader
            .read_exact(&mut bytes)
            .map_err(|error| contextual(error, "record header"))?;
        let header = RecordHeader::decode(bytes);
        if header.flags & !RECORD_KNOWN_FLAGS != 0 {
            return Err(invalid("Vivid record has nonzero reserved flags"));
        }
        if header.body_length > self.maximum || header.body_length > HARD_MAX_RECORD_BODY {
            return Err(invalid("Vivid record exceeds the accepted body limit"));
        }
        let expected = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("record sequence exhausted"))?;
        if header.sequence != expected {
            return Err(invalid("Vivid record sequence is not contiguous"));
        }
        if self.first_record {
            kind.validate_first_record(&header)?;
            self.first_record = false;
        }
        self.sequence = header.sequence;
        body.resize(header.body_length as usize, 0);
        self.reader
            .read_exact(body)
            .map_err(|error| contextual(error, "record body"))?;
        Ok(header)
    }

    pub fn writer(&self) -> Arc<Writer> {
        self.writer.clone()
    }

    pub fn set_maximum(&mut self, maximum: u32) -> io::Result<()> {
        if maximum == 0 || maximum > HARD_MAX_RECORD_BODY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid incoming record limit",
            ));
        }
        self.maximum = self.negotiated_maximum.min(maximum);
        Ok(())
    }

    pub fn clear_read_deadline(&self) -> io::Result<()> {
        *self
            .deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        (self.timeout)(None)
    }
}

pub struct Writer {
    inner: Mutex<WriterInner>,
}

struct WriterInner {
    stream: Box<dyn Write + Send>,
    maximum: u32,
    sequence: u64,
}

impl Writer {
    pub fn set_maximum(&self, maximum: u32) -> io::Result<()> {
        if maximum == 0 || maximum > HARD_MAX_RECORD_BODY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid outgoing record limit",
            ));
        }
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .maximum = maximum;
        Ok(())
    }

    pub fn write_record(&self, record_type: u16, object_id: u64, body: &[u8]) -> io::Result<u64> {
        self.write_record_parts(record_type, object_id, &[body])
    }

    pub fn write_record_parts(
        &self,
        record_type: u16,
        object_id: u64,
        parts: &[&[u8]],
    ) -> io::Result<u64> {
        let body_length = parts.iter().try_fold(0_usize, |total, part| {
            total
                .checked_add(part.len())
                .ok_or_else(|| invalid_input("record body length overflows"))
        })?;
        let body_length =
            u32::try_from(body_length).map_err(|_| invalid_input("record body exceeds u32"))?;
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if body_length > inner.maximum || body_length > HARD_MAX_RECORD_BODY {
            return Err(invalid_input(
                "outgoing Vivid record exceeds the accepted body limit",
            ));
        }
        inner.sequence = inner
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("outgoing sequence exhausted"))?;
        let sequence = inner.sequence;
        let header = RecordHeader {
            body_length,
            record_type,
            flags: 0,
            object_id,
            sequence,
        };
        write_parts(inner.stream.as_mut(), &header.encode(), parts)?;
        inner.stream.flush()?;
        Ok(sequence)
    }
}

fn write_parts(stream: &mut dyn Write, header: &[u8], parts: &[&[u8]]) -> io::Result<()> {
    let mut buffers = Vec::with_capacity(parts.len() + 1);
    buffers.push(IoSlice::new(header));
    buffers.extend(parts.iter().map(|part| IoSlice::new(part)));
    let mut index = 0;
    let mut offset = 0;
    while index < buffers.len() {
        let current = &buffers[index..];
        let mut adjusted = Vec::with_capacity(current.len());
        adjusted.push(IoSlice::new(&current[0][offset..]));
        adjusted.extend(current[1..].iter().map(|slice| IoSlice::new(slice)));
        let written = stream.write_vectored(&adjusted)?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write Vivid record",
            ));
        }
        let mut remaining = written;
        while index < buffers.len() {
            let available = buffers[index].len() - offset;
            if remaining < available {
                offset += remaining;
                break;
            }
            remaining -= available;
            index += 1;
            offset = 0;
            if remaining == 0 {
                break;
            }
        }
    }
    Ok(())
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn contextual(error: io::Error, part: &'static str) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("failed to read Vivid {part}: {error}"),
    )
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;
    use std::thread;

    fn transport(stream: UnixStream) -> Transport {
        let reader = stream.try_clone().unwrap();
        Transport::new(
            Box::new(reader),
            Box::new(stream),
            ConnectionCancel::inert(),
            Arc::new(|_| Ok(())),
        )
    }

    #[test]
    fn exact_1_5_preface_is_accepted() {
        let (mut client, server) = UnixStream::pair().unwrap();
        client
            .write_all(&vivid_protocol::wire::encode_preface(
                ConnectionKind::Control,
                CONTROL_MAX_RECORD_BODY,
            ))
            .unwrap();
        let (_, preface, _) = Reader::new(transport(server)).unwrap();
        assert_eq!((preface.major, preface.minor), (1, 5));
        assert_eq!(preface.kind, ConnectionKind::Control);
    }

    #[test]
    fn valid_1_1_preface_gets_one_typed_version_error_then_close() {
        let (mut client, server) = UnixStream::pair().unwrap();
        let service = thread::spawn(move || {
            let error = match Reader::new(transport(server)) {
                Ok(_) => panic!("Vivid 1.1 preface was accepted"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        });
        client
            .write_all(&vivid_protocol::wire::encode_preface_version(
                ConnectionKind::Control,
                CONTROL_MAX_RECORD_BODY,
                1,
                1,
            ))
            .unwrap();
        client.shutdown(Shutdown::Write).unwrap();
        let mut reply = Vec::new();
        client.read_to_end(&mut reply).unwrap();
        service.join().unwrap();
        assert_eq!(reply, vivid_protocol::wire::unsupported_version_record());
    }
}
