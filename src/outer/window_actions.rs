//! One-shot original action admission across an integration's process boundary.

// Rust guideline compliant 2026-10-07
use super::overlay::OverlayJob;
use super::{OuterBridge, invalid_data};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use vivid_protocol::cbor::Value;
use vivid_protocol::messages;
use vivid_protocol::overlay::wire::{
    Action, Query, SetWindow, Status, WindowAction, WindowAddress,
};
use vivid_protocol::vector::Rect;
use vivid_sdk::presenter::{
    BridgeSurfaceKey, OverlayActionRequest, OverlayActionWindow, OverlayHostRequest,
};

// Match the original SDK window-action deadline without renewing it remotely.
const CLAIM_LIFETIME: Duration = Duration::from_secs(10);
// A cancelled bridge must not wait for a slow/unresponsive integration to answer.
const CANCEL_POLL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy)]
enum Phase {
    Waiting,
    Ready,
    Issued,
    Decision(bool, Instant),
    Retired,
}
pub(super) struct Admission {
    surface: BridgeSurfaceKey,
    created: Instant,
    cancelled: Arc<AtomicBool>,
    phase: Mutex<Phase>,
    changed: Condvar,
}
impl Admission {
    fn live(&self) -> bool {
        !self.cancelled.load(Ordering::Acquire) && self.created.elapsed() < CLAIM_LIFETIME
    }
    fn pending(&self) -> bool {
        self.live()
            && self
                .phase
                .lock()
                .is_ok_and(|phase| matches!(*phase, Phase::Waiting | Phase::Ready | Phase::Issued))
    }
    fn retire(&self) {
        if let Ok(mut phase) = self.phase.lock() {
            *phase = Phase::Retired;
            self.changed.notify_all();
        }
    }
    fn decide(&self, allowed: bool, deadline: Instant) -> bool {
        let Ok(mut phase) = self.phase.lock() else {
            return false;
        };
        if !self.live() || !matches!(*phase, Phase::Issued) || Instant::now() >= deadline {
            return false;
        }
        *phase = Phase::Decision(allowed, deadline.min(self.created + CLAIM_LIFETIME));
        self.changed.notify_all();
        true
    }
    fn wait(&self) -> bool {
        let Ok(mut phase) = self.phase.lock() else {
            return false;
        };
        if !self.live() || !matches!(*phase, Phase::Waiting) {
            return false;
        }
        *phase = Phase::Ready;
        loop {
            if !self.live() {
                *phase = Phase::Retired;
                return false;
            }
            match *phase {
                Phase::Decision(allowed, deadline) => {
                    *phase = Phase::Retired;
                    return allowed && Instant::now() < deadline;
                }
                Phase::Retired => return false,
                _ => (),
            }
            let Ok((next, _)) = self.changed.wait_timeout(phase, CANCEL_POLL) else {
                return false;
            };
            phase = next;
        }
    }
}

/// A one-shot reply channel for an original presenter's remote action claim.
///
/// Neither serializable nor retargetable. After validating the reporting client,
/// bridge and request correlation, ask the original SDK request to `claim`, then
/// complete this exact receipt with that decision. Numeric metadata alone grants
/// no authority. Dropping the receipt refuses dispatch. Cancellation, retirement
/// and the bounded deadline make late decisions ineffective.
pub struct ExternalActionClaim {
    id: u64,
    admission: Arc<Admission>,
}
impl std::fmt::Debug for ExternalActionClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalActionClaim")
            .field("request_id", &self.id)
            .finish_non_exhaustive()
    }
}
impl ExternalActionClaim {
    /// Returns the integration's original request correlation, without granting authority.
    #[must_use]
    pub fn request_id(&self) -> u64 {
        self.id
    }

    /// Completes this exact claim once; returns false after retirement or expiry.
    #[must_use]
    pub fn complete(self, allowed: bool) -> bool {
        self.admission
            .decide(allowed, self.admission.created + CLAIM_LIFETIME)
    }

    /// Completes a remote decision before its conservatively retained original deadline.
    ///
    /// `deadline` is a local monotonic instant, computed by subtracting the full
    /// claim round trip from the original server's remaining lifetime. The worker
    /// rechecks it before consuming admission. This cannot extend its own bound.
    #[must_use]
    pub fn complete_before(self, allowed: bool, deadline: Instant) -> bool {
        self.admission.decide(allowed, deadline)
    }
}
impl Drop for ExternalActionClaim {
    fn drop(&mut self) {
        self.admission
            .decide(false, self.admission.created + CLAIM_LIFETIME);
    }
}

enum ActionAdmission {
    Local(OverlayActionRequest),
    External(Arc<Admission>),
}
impl ActionAdmission {
    fn pending(&self) -> bool {
        match self {
            Self::Local(request) => request.is_pending(),
            Self::External(value) => value.pending(),
        }
    }
    fn claim(self) -> bool {
        match self {
            Self::Local(request) => request.claim(),
            Self::External(value) => value.wait(),
        }
    }
}
struct Retire(Option<Arc<Admission>>);
impl Drop for Retire {
    fn drop(&mut self) {
        if let Some(value) = &self.0 {
            value.retire();
        }
    }
}

impl OuterBridge {
    /// Starts one bounded window action whose original admission lives in another process.
    ///
    /// Configure external input epochs first. `preceding` and `centered` must be
    /// copied from the original SDK request's non-authoritative metadata. They
    /// must never be reconstructed from current owner-local IDs. No physical
    /// mutation happens until the worker queries its exact physical window and
    /// yields an [`ExternalActionClaim`]. Drain those claims while servicing
    /// input/media, validate client/bridge/correlation at the original server and
    /// complete each with its original SDK request's one-shot `claim` result.
    /// Return the eventual host reply to that original request separately.
    ///
    /// Shares the existing 16-global/four-per-owner worker bounds. Admission waits
    /// at most ten seconds from this call, without extending the original SDK
    /// deadline; cancellation wakes within 25 ms. Dropped decisions refuse the
    /// RPC. Neither requests nor decisions are retried against replacement roles.
    ///
    /// # Errors
    /// Returns `Unsupported` without external input mode or with a local action
    /// route; `Interrupted` after cancellation; `InvalidData` for malformed,
    /// duplicate or stale requests/barriers; `WouldBlock` for worker pressure or
    /// preceding windows not yet projected. Thread creation errors propagate.
    pub fn start_external_overlay_action(
        &mut self,
        request: &OverlayHostRequest,
        preceding: &[OverlayActionWindow],
        centered: Option<Rect>,
    ) -> io::Result<()> {
        if self.cancellation.cancelled.load(Ordering::Acquire) {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.action_relay.is_some() || self.external_input_epochs.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "external action mode is unavailable",
            ));
        }
        checked_request(request, preceding, centered)?;
        if preceding.iter().any(|before| {
            self.overlay_windows
                .get(&before.surface)
                .is_some_and(|after| {
                    after.inner_generation > before.generation
                        || (after.inner_generation == before.generation
                            && after.projected.revision > before.revision)
                })
        }) {
            return Err(invalid_data(
                "physical projection has overtaken the original action barrier",
            ));
        }
        self.external_action_claims
            .retain(|(_, admission)| admission.pending());
        if self.overlay_jobs.iter().any(|job| job.id == request.id)
            || self
                .external_action_claims
                .iter()
                .any(|(id, _)| *id == request.id)
        {
            return Err(invalid_data(
                "external action correlation is already pending",
            ));
        }
        if !self.overlay_worker_available(request.surface.producer)
            || !self.window_barrier_ready(preceding)
        {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let admission = Arc::new(Admission {
            surface: request.surface,
            created: Instant::now(),
            cancelled: Arc::clone(&self.cancellation.cancelled),
            phase: Mutex::new(Phase::Waiting),
            changed: Condvar::new(),
        });
        let job = self.prepare_window_action(
            request,
            centered,
            ActionAdmission::External(Arc::clone(&admission)),
        )?;
        self.spawn_overlay_job(request.id, request.surface.producer, job)?;
        self.external_action_claims.push((request.id, admission));
        Ok(())
    }

    /// Drains one-shot original-server claims without blocking input or media service.
    pub fn take_external_overlay_action_claims(&mut self) -> Vec<ExternalActionClaim> {
        let mut claims = Vec::new();
        self.external_action_claims.retain(|(id, admission)| {
            if !admission.live() {
                admission.retire();
                return false;
            }
            let Ok(mut phase) = admission.phase.lock() else {
                return false;
            };
            match *phase {
                Phase::Ready => {
                    *phase = Phase::Issued;
                    claims.push(ExternalActionClaim {
                        id: *id,
                        admission: Arc::clone(admission),
                    });
                    true
                }
                Phase::Waiting | Phase::Issued => true,
                _ => false,
            }
        });
        claims
    }

    /// Retires one original request's pending physical admission without affecting peers.
    ///
    /// Validate the reporting server/bridge and request correlation before calling
    /// this on a remote retirement. Already-dispatched RPCs may finish, but can
    /// never target a replacement physical identity.
    pub fn retire_external_overlay_action(&mut self, id: u64) {
        self.external_action_claims.retain(|(current, admission)| {
            if *current == id {
                admission.retire();
                false
            } else {
                true
            }
        });
    }

    pub(super) fn retire_external_actions(&mut self, surface: Option<BridgeSurfaceKey>) {
        self.external_action_claims.retain(|(_, admission)| {
            if surface.is_none_or(|key| key == admission.surface) {
                admission.retire();
                false
            } else {
                true
            }
        });
    }

    pub(super) fn window_barrier_ready(&self, windows: &[OverlayActionWindow]) -> bool {
        windows.iter().all(|before| {
            self.overlay_windows
                .get(&before.surface)
                .is_some_and(|after| {
                    after.inner_generation == before.generation
                        && after.projected.revision >= before.revision
                })
        })
    }

    pub(super) fn prepare_local_window_action(
        &self,
        request: &OverlayHostRequest,
    ) -> io::Result<OverlayJob> {
        let admission = self
            .action_relay
            .as_ref()
            .ok_or_else(|| invalid_data("window-action forwarding is not enabled"))?
            .request(request);
        let windows = admission
            .preceding_windows()
            .ok_or_else(|| invalid_data("original window action is retired or expired"))?;
        if !self.window_barrier_ready(&windows) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let centered = admission.centered_bounds();
        self.prepare_window_action(request, centered, ActionAdmission::Local(admission))
    }

    fn prepare_window_action(
        &self,
        request: &OverlayHostRequest,
        centered: Option<Rect>,
        admission: ActionAdmission,
    ) -> io::Result<OverlayJob> {
        let envelope = messages::decode_control(&request.body)?;
        let mut query = Action::decode(request.surface.surface, &Value::Map(envelope.payload))?;
        let window = self
            .overlay_windows
            .get(&request.surface)
            .ok_or_else(|| invalid_data("action window is absent"))?;
        if query.address.context_id != request.surface.context
            || query.address.generation != window.inner_generation
            || query.expected_revision != window.projected.revision
        {
            return Err(invalid_data("action window revision is stale"));
        }
        if !admission.pending() {
            return Err(invalid_data("original window action is retired or expired"));
        }
        let centered = if query.action == WindowAction::Center {
            let bounds =
                centered.ok_or_else(|| invalid_data("original center bounds are unavailable"))?;
            Some(
                Rect::new(
                    bounds.origin.x.get() + super::overlay::exact_f64(window.offset_x)?,
                    bounds.origin.y.get() + super::overlay::exact_f64(window.offset_y)?,
                    bounds.width.get(),
                    bounds.height.get(),
                )
                .map_err(io::Error::other)?,
            )
        } else {
            None
        };
        let outer = self
            .surfaces
            .get(&request.surface)
            .ok_or_else(|| invalid_data("outer overlay surface is absent"))?;
        let address = WindowAddress {
            context_id: outer.context_id(),
            surface_id: outer.id(),
            generation: outer.generation().get(),
        };
        query.address = address;
        let host = self
            .session
            .overlay_host_handle()
            .ok_or_else(|| invalid_data("outer overlay host is offline"))?;
        let owner = vivid_protocol::identity::SessionIdentity::new(
            vivid_protocol::identity::PresenterInstanceId([0; 16]),
            self.session.info().session_id,
        )
        .map_err(io::Error::other)?;
        let cancelled = Arc::clone(&self.cancellation.cancelled);
        Ok(Box::new(move || {
            let _retire = Retire(match &admission {
                ActionAdmission::External(value) => Some(Arc::clone(value)),
                ActionAdmission::Local(_) => None,
            });
            if cancelled.load(Ordering::Acquire) || !admission.pending() {
                return Err(invalid_data("original window action is retired or expired"));
            }
            let fields = host(
                messages::QUERY_OVERLAY,
                address.surface_id,
                Query {
                    context_id: address.context_id,
                    surface_id: address.surface_id,
                }
                .payload()?,
            )?;
            let status = Status::decode(owner, address.surface_id, &Value::Map(fields))?;
            if status.window.address != address {
                return Err(invalid_data("outer action window generation changed"));
            }
            query.expected_revision = status.window.expected_revision;
            if let Some(bounds) = centered
                && (bounds.width != status.window.options.bounds.width
                    || bounds.height != status.window.options.bounds.height)
            {
                return Err(invalid_data("physical window size changed before center"));
            }
            if cancelled.load(Ordering::Acquire)
                || !admission.claim()
                || cancelled.load(Ordering::Acquire)
            {
                return Err(invalid_data("original window action is retired"));
            }
            if let Some(bounds) = centered {
                let mut request = status.window;
                request.options.bounds = bounds;
                let fields = host(
                    messages::SET_OVERLAY_WINDOW,
                    address.surface_id,
                    request.payload(owner)?,
                )?;
                let accepted = SetWindow::decode(owner, address.surface_id, &Value::Map(fields))?;
                if accepted.address != address || accepted.options.bounds != bounds {
                    return Err(invalid_data(
                        "physical center acknowledgement changed bounds",
                    ));
                }
                return messages::Envelope::new(0, Vec::new())
                    .encode()
                    .map_err(io::Error::other);
            }
            let fields = host(
                messages::OVERLAY_ACTION,
                address.surface_id,
                query.payload()?,
            )?;
            messages::Envelope::new(0, fields)
                .encode()
                .map_err(io::Error::other)
        }))
    }
}

fn checked_request(
    request: &OverlayHostRequest,
    preceding: &[OverlayActionWindow],
    centered: Option<Rect>,
) -> io::Result<()> {
    if request.id == 0
        || request.record_type != messages::OVERLAY_ACTION
        || !request.layout_ids.is_empty()
        || request.body.len() > vivid_protocol::LANE_MAX_RECORD_BODY as usize
        || preceding.is_empty()
        || preceding.len() > vivid_protocol::overlay::MAX_WINDOWS_PER_OWNER
    {
        return Err(invalid_data("invalid external window action metadata"));
    }
    let envelope = messages::decode_control(&request.body)?;
    let action = Action::decode(request.surface.surface, &Value::Map(envelope.payload))?;
    if request.surface.producer == 0
        || action.address.context_id != request.surface.context
        || (action.action == WindowAction::Center) != centered.is_some()
    {
        return Err(invalid_data(
            "external action owner or center metadata mismatch",
        ));
    }
    let mut keys = std::collections::HashSet::new();
    for window in preceding {
        if window.surface.producer != request.surface.producer
            || window.surface.context == 0
            || window.surface.surface == 0
            || window.generation == 0
            || window.revision == 0
            || !keys.insert(window.surface)
        {
            return Err(invalid_data("invalid external owner window barrier"));
        }
    }
    if !preceding.iter().any(|window| {
        window.surface == request.surface
            && window.generation == action.address.generation
            && window.revision == action.expected_revision
    }) {
        return Err(invalid_data(
            "external action target is absent from its original barrier",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_and_cancelled_decisions_cannot_claim_or_retarget() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let make = |created| {
            Arc::new(Admission {
                surface: BridgeSurfaceKey {
                    producer: 1,
                    context: 1,
                    surface: 1,
                },
                created,
                cancelled: Arc::clone(&cancelled),
                phase: Mutex::new(Phase::Issued),
                changed: Condvar::new(),
            })
        };
        let retired = make(Instant::now());
        retired.retire();
        let replacement = make(Instant::now());
        assert!(
            !ExternalActionClaim {
                id: 1,
                admission: retired
            }
            .complete(true)
        );
        assert!(
            ExternalActionClaim {
                id: 1,
                admission: replacement
            }
            .complete(true)
        );
        let expired = make(Instant::now().checked_sub(CLAIM_LIFETIME).unwrap());
        assert!(
            !ExternalActionClaim {
                id: 1,
                admission: expired
            }
            .complete(true)
        );
        let current = make(Instant::now());
        cancelled.store(true, Ordering::Release);
        assert!(
            !ExternalActionClaim {
                id: 1,
                admission: Arc::clone(&current)
            }
            .complete(true)
        );
        assert!(!current.wait());
        let replacement = make(Instant::now());
        assert!(
            !ExternalActionClaim {
                id: 1,
                admission: replacement
            }
            .complete(true)
        );
    }
}
