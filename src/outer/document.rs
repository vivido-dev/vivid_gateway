//! Opt-in document re-origination with bounded, independently allocated hop identities.

// Rust guideline compliant 2026-10-07
use super::{OuterBridge, invalid_data, overlay::OverlayJob};
use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use vivid_protocol::{
    cbor::Value,
    messages,
    overlay::wire::{
        WindowAddress,
        text::editor::{Context, Edit, SetContext},
    },
};
use vivid_sdk::{
    overlay::OverlayHostHandle,
    presenter::{BridgeSurfaceKey, OverlayHostRequest, VirtualVivid},
};

// Match the SDK's document slots and input bound. Old revisions and pending edits
// expire/drop rather than growing with a long-running editor or blocking raw input.
const CAPACITY: usize = 64;
const INPUT_LIFETIME: Duration = Duration::from_secs(1);

#[derive(Default)]
pub(super) struct Documents {
    next: u64,
    windows: HashMap<BridgeSurfaceKey, Arc<Mutex<Window>>>,
}
struct Window {
    inner_address: WindowAddress,
    outer_address: WindowAddress,
    inner: Option<Context>,
    outer: Option<Context>,
    revisions: VecDeque<Revision>,
    busy: bool,
}
struct Revision {
    outer: u64,
    inner: u64,
    target: vivid_sdk::presenter::OverlayDocumentTarget,
}
struct Grant {
    documents: Arc<Mutex<Documents>>,
    presenter: std::sync::Weak<VirtualVivid>,
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    key: BridgeSurfaceKey,
    window: Arc<Mutex<Window>>,
}
struct Reservation(Grant);
impl Drop for Reservation {
    fn drop(&mut self) {
        self.0.window.lock().expect("document window mutex").busy = false;
    }
}
impl Grant {
    fn live(&self, documents: &Documents) -> bool {
        !self.cancelled.load(std::sync::atomic::Ordering::Acquire)
            && documents
                .windows
                .get(&self.key)
                .is_some_and(|window| Arc::ptr_eq(window, &self.window))
    }
}

/// An immutable document input retaining its original bridge/editor grant and deadline.
///
/// This type deliberately has no serialization or retargeting API. Integrations
/// must forward it to the original inner presenter. Legacy out-of-process mux
/// transports must continue declining document editing until they preserve these scopes.
pub struct DocumentInput {
    destination: Grant,
    edit: Edit,
    target: vivid_sdk::presenter::OverlayDocumentTarget,
    deadline: Instant,
}
impl std::fmt::Debug for DocumentInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DocumentInput").finish_non_exhaustive()
    }
}
impl DocumentInput {
    /// Forwards only while the original bridge, editor, excerpt and input grant remain live.
    ///
    /// Returns false after expiry, focus loss, hide, removal, session replacement,
    /// or an incompatible document update. Ordinary relative typing may follow its
    /// own earlier commits. There is no retry after an uncertain forwarding outcome.
    ///
    /// # Errors
    /// Returns strict edit or ownership validation errors from the inner presenter.
    pub fn forward(self) -> io::Result<bool> {
        let Some(inner) = self.destination.presenter.upgrade() else {
            return Ok(false);
        };
        let documents = self
            .destination
            .documents
            .lock()
            .map_err(super::poisoned("document relay"))?;
        if Instant::now() >= self.deadline || !self.destination.live(&documents) {
            return Ok(false);
        }
        let window = self
            .destination
            .window
            .lock()
            .map_err(super::poisoned("document window"))?;
        let Some(context) = &window.inner else {
            return Ok(false);
        };
        if self.edit.validate_context(context).is_err() {
            return Ok(false);
        }
        inner.relay_overlay_document_edit(&self.target, &self.edit)
    }
}
impl OuterBridge {
    /// Takes bounded typed document input after draining ordinary overlay input.
    ///
    /// Opt in at construction with `overlay_document_editing(&inner)`. Service host
    /// requests and call `take_overlay_input` first; forward each returned item
    /// immediately to its original inner presenter. At most 64 edits await this
    /// drain. Saturation discards document edits while ordinary input stays live.
    pub fn take_overlay_document_input(&mut self) -> Vec<DocumentInput> {
        self.document_input
            .drain(..)
            .filter(|input| Instant::now() < input.deadline)
            .collect()
    }

    pub(super) fn retire_documents(&mut self, key: Option<BridgeSurfaceKey>) {
        let mut documents = self.documents.lock().expect("document relay mutex");
        if let Some(key) = key {
            documents.windows.remove(&key);
            self.document_input
                .retain(|input| input.destination.key != key);
        } else {
            documents.windows.clear();
            self.document_input.clear();
        }
    }

    pub(super) fn document_request_available(&self, request: &OverlayHostRequest) -> bool {
        if request.record_type != messages::SET_OVERLAY_TEXT_CONTEXT {
            return true;
        }
        let documents = self.documents.lock().expect("document relay mutex");
        self.document_relay
            && documents
                .windows
                .get(&request.surface)
                .map_or(documents.windows.len() < CAPACITY, |window| {
                    !window.lock().expect("document window mutex").busy
                })
    }

    pub(super) fn prepare_document_request(
        &self,
        request: &OverlayHostRequest,
        address: WindowAddress,
        host: OverlayHostHandle,
    ) -> io::Result<OverlayJob> {
        let key = request.surface;
        if !self.document_relay {
            return Err(invalid_data("document relay is not enabled"));
        }
        let envelope = messages::decode_control(&request.body)?;
        let query = SetContext::decode(key.surface, &Value::Map(envelope.payload))?;
        let expected = self
            .overlay_windows
            .get(&key)
            .ok_or_else(|| invalid_data("overlay window is absent"))?;
        if query.address.context_id != key.context
            || query.address.generation != expected.inner_generation
        {
            return Err(invalid_data("document surface is stale"));
        }
        let target = self
            .document_target
            .upgrade()
            .and_then(|inner| inner.overlay_document_target(key))
            .ok_or_else(|| invalid_data("original inner document grant is unavailable"))?;
        let mut documents = self.documents.lock().expect("document relay mutex");
        if !documents.windows.contains_key(&key) && documents.windows.len() >= CAPACITY {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let window = Arc::clone(documents.windows.entry(key).or_insert_with(|| {
            Arc::new(Mutex::new(Window {
                inner_address: query.address,
                outer_address: address,
                inner: None,
                outer: None,
                revisions: VecDeque::new(),
                busy: false,
            }))
        }));
        let mut state = window.lock().expect("document window mutex");
        if state.busy {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        if state.inner_address != query.address || state.outer_address != address {
            return Err(invalid_data("document address changed"));
        }
        state.busy = true;
        drop(state);
        drop(documents);
        let reservation = Reservation(Grant {
            documents: Arc::clone(&self.documents),
            presenter: std::sync::Weak::clone(&self.document_target),
            cancelled: Arc::clone(&self.cancellation.cancelled),
            key,
            window,
        });
        let revisions = Arc::clone(&self.overlay_revisions);
        Ok(Box::new(move || {
            let scene =
                revisions.wait_presented(key, query.scene_revision, Duration::from_secs(1))?;
            let mut documents = reservation
                .0
                .documents
                .lock()
                .expect("document relay mutex");
            if !reservation.0.live(&documents) {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let mut state = reservation.0.window.lock().expect("document window mutex");
            let allocate = |documents: &mut Documents| -> io::Result<u64> {
                documents.next = documents
                    .next
                    .checked_add(1)
                    .ok_or_else(|| invalid_data("document identity exhausted"))?;
                Ok(documents.next)
            };
            let context = query
                .context
                .as_ref()
                .map(|inner| -> io::Result<Context> {
                    let same = state
                        .inner
                        .as_ref()
                        .is_some_and(|old| old.editor == inner.editor);
                    let mut outer = inner.clone();
                    outer.editor = if same {
                        state
                            .outer
                            .as_ref()
                            .ok_or_else(|| invalid_data("missing outer editor"))?
                            .editor
                    } else {
                        allocate(&mut documents)?
                    };
                    // Each publication has its own captured grant, even when the
                    // excerpt is unchanged. Old callbacks retain their own mapping.
                    outer.revision = allocate(&mut documents)?;
                    Ok(outer)
                })
                .transpose()?;
            if state.inner.as_ref().map(|c| c.editor) != query.context.as_ref().map(|c| c.editor) {
                state.revisions.clear();
            }
            if let (Some(inner), Some(outer)) = (&query.context, &context) {
                if state.revisions.len() >= CAPACITY {
                    state.revisions.pop_front();
                }
                state.revisions.push_back(Revision {
                    outer: outer.revision,
                    inner: inner.revision,
                    target,
                });
            }
            // The physical control reader exposes this context before its host
            // callback acknowledges. Publish the immutable mapping first so input
            // admitted in that interval cannot use the previous scene's grant.
            // A failed/uncertain RPC never rolls back input or retargets old mappings.
            state.inner = query.context;
            state.outer.clone_from(&context);
            drop(state);
            drop(documents);
            let fields = host(
                messages::SET_OVERLAY_TEXT_CONTEXT,
                address.surface_id,
                SetContext {
                    address,
                    scene_revision: scene,
                    context,
                }
                .payload()?,
            )?;
            let documents = reservation
                .0
                .documents
                .lock()
                .expect("document relay mutex");
            if !reservation.0.live(&documents) {
                return Err(io::ErrorKind::Interrupted.into());
            }
            Ok(messages::Envelope::new(envelope.request_id, fields).encode()?)
        }))
    }

    pub(super) fn queue_document_input(&mut self, mut edit: Edit) -> io::Result<()> {
        if !self.document_relay {
            return Err(invalid_data("document relay is not enabled"));
        }
        let Some(key) = self.outer_surface_key(&edit.address) else {
            return Ok(());
        };
        let documents = self.documents.lock().expect("document relay mutex");
        let Some(window) = documents.windows.get(&key).cloned() else {
            return Ok(());
        };
        let state = window.lock().expect("document window mutex");
        let Some(context) = &state.outer else {
            return Ok(());
        };
        if edit.validate_context(context).is_err() {
            return Ok(());
        }
        let Some(revision) = state
            .revisions
            .iter()
            .find(|revision| revision.outer == edit.revision)
        else {
            return Ok(());
        };
        let Some(scene) = self
            .overlay_revisions
            .lock()?
            .get(&key)
            .and_then(|w| {
                w.revisions
                    .iter()
                    .find(|(outer, _)| *outer == edit.scene_revision)
            })
            .map(|(_, inner)| *inner)
        else {
            return Ok(());
        };
        edit.address = state.inner_address;
        edit.scene_revision = scene;
        edit.editor = state
            .inner
            .as_ref()
            .ok_or_else(|| invalid_data("missing source context"))?
            .editor;
        edit.revision = revision.inner;
        let target = revision.target.clone();
        drop(state);
        drop(documents);
        self.document_input
            .retain(|input| Instant::now() < input.deadline);
        if self.document_input.len() < CAPACITY {
            self.document_input.push_back(DocumentInput {
                destination: Grant {
                    documents: Arc::clone(&self.documents),
                    presenter: std::sync::Weak::clone(&self.document_target),
                    cancelled: Arc::clone(&self.cancellation.cancelled),
                    key,
                    window,
                },
                edit,
                target,
                deadline: Instant::now() + INPUT_LIFETIME,
            });
        }
        Ok(())
    }
}
