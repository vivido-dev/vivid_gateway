//! Bounded physical outcome observations for integrations retaining original grants across IPC.

// Rust guideline compliant 2026-10-07
use super::{OuterBridge, invalid_data};
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
};
use vivid_protocol::overlay::wire::PresentationOutcome;
use vivid_sdk::presenter::BridgeSourceKey;

// Match the SDK's original presentation-copy admission, including undrained reports.
const CAPACITY: usize = 64;
const PER_OWNER: usize = 4;

/// A downstream display outcome, independent of record write completion.
///
/// This is correlation metadata, not an original grant. An IPC integration must
/// retain its original SDK receipt on the owning server, allocate a nonzero
/// correlation unique for that attachment, and validate the reporting client and
/// bridge incarnation before completing that exact receipt. Never retarget an
/// observation using only a source's current numeric revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PresentationReport {
    /// Integration-owned correlation supplied with the original record.
    pub correlation: u64,
    /// Source lookup key; the retained original receipt owns channel/incarnation authority.
    pub source: BridgeSourceKey,
    /// Physical outcome, or `None` when the copy was abandoned without an outcome.
    pub outcome: Option<PresentationOutcome>,
}

#[derive(Debug)]
struct Slot {
    report: PresentationReport,
    ready: bool,
}
#[derive(Debug, Default)]
struct State {
    next: u64,
    slots: HashMap<u64, Slot>,
    cancelled: bool,
}
#[derive(Debug, Default)]
pub(super) struct Reports(Mutex<State>);

#[derive(Debug)]
pub(super) struct Receipt {
    reports: Arc<Reports>,
    id: u64,
    pub(super) correlation: u64,
}
impl Reports {
    fn available(&self, source: BridgeSourceKey) -> bool {
        self.0.lock().is_ok_and(|state| {
            !state.cancelled
                && state.slots.len() < CAPACITY
                && state
                    .slots
                    .values()
                    .filter(|slot| slot.report.source.producer == source.producer)
                    .count()
                    < PER_OWNER
        })
    }
    pub(super) fn reserve(
        self: &Arc<Self>,
        source: BridgeSourceKey,
        correlation: u64,
    ) -> io::Result<Receipt> {
        let mut state = self
            .0
            .lock()
            .map_err(|_poison| io::Error::other("presentation reports poisoned"))?;
        if state.cancelled {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "presentation reports retired",
            ));
        }
        if state
            .slots
            .values()
            .any(|slot| slot.report.correlation == correlation)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "presentation correlation is already live",
            ));
        }
        if state.slots.len() >= CAPACITY
            || state
                .slots
                .values()
                .filter(|slot| slot.report.source.producer == source.producer)
                .count()
                >= PER_OWNER
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "presentation report capacity exhausted",
            ));
        }
        let id = state
            .next
            .checked_add(1)
            .ok_or_else(|| invalid_data("presentation report identity exhausted"))?;
        state.next = id;
        state.slots.insert(
            id,
            Slot {
                report: PresentationReport {
                    correlation,
                    source,
                    outcome: None,
                },
                ready: false,
            },
        );
        Ok(Receipt {
            reports: Arc::clone(self),
            id,
            correlation,
        })
    }
    fn take(&self) -> Vec<PresentationReport> {
        let mut state = self.0.lock().expect("presentation reports mutex");
        let ready = state
            .slots
            .iter()
            .filter_map(|(id, slot)| slot.ready.then_some(*id))
            .collect::<Vec<_>>();
        ready
            .into_iter()
            .filter_map(|id| state.slots.remove(&id).map(|slot| slot.report))
            .collect()
    }
    pub(super) fn cancel(&self) {
        let mut state = self.0.lock().expect("presentation reports mutex");
        state.cancelled = true;
        for slot in state.slots.values_mut().filter(|slot| !slot.ready) {
            slot.ready = true;
            slot.report.outcome = None;
        }
    }
}
impl Receipt {
    fn finish(&self, outcome: Option<PresentationOutcome>) {
        let mut state = self.reports.0.lock().expect("presentation reports mutex");
        if let Some(slot) = state.slots.get_mut(&self.id)
            && !slot.ready
        {
            slot.ready = true;
            slot.report.outcome = outcome;
        }
    }
    pub(super) fn complete(self, outcome: PresentationOutcome) {
        self.finish(Some(outcome));
    }
}
impl Drop for Receipt {
    fn drop(&mut self) {
        self.finish(None);
    }
}

pub(super) enum OriginalReceipt {
    Local(vivid_sdk::presenter::OverlayPresentationReceipt),
    Report(Receipt),
}
impl OriginalReceipt {
    pub(super) fn complete(self, outcome: PresentationOutcome) {
        match self {
            Self::Local(receipt) => {
                receipt.complete(outcome);
            }
            Self::Report(receipt) => receipt.complete(outcome),
        }
    }
    pub(super) fn abandon(self) {
        match self {
            Self::Local(receipt) => {
                receipt.abandon();
            }
            Self::Report(receipt) => drop(receipt),
        }
    }
}
impl OuterBridge {
    /// Checks whether a correlated source may hand off its next chunk now.
    ///
    /// A scheduling hint only; media admission still validates the record and
    /// reserves its bounded report. Existing partial records keep their report
    /// reservation and may finish while other sources are at their owner limit.
    pub fn can_accept_media_with_presentation(&self, source: BridgeSourceKey) -> bool {
        !self
            .cancellation
            .cancelled
            .load(std::sync::atomic::Ordering::Acquire)
            && self.can_accept_media(source)
            && (self.pending.contains_key(&source)
                || self.overlay_revisions.reports.available(source))
    }
    /// Drains bounded display reports for records submitted with external correlations.
    ///
    /// Continue servicing `take_overlay_input` or `take_overlay_routed_input` to
    /// receive physical outcomes. Writer retirement produces abandonment reports
    /// without manufacturing `Presented`. Draining releases report capacity;
    /// undrained completed reports continue to count against admission.
    ///
    /// # Panics
    /// Panics if an earlier panic poisoned the internal outcome queue.
    pub fn take_presentation_reports(&mut self) -> Vec<PresentationReport> {
        self.overlay_revisions.reports.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(producer: u64) -> BridgeSourceKey {
        BridgeSourceKey {
            producer,
            context: 1,
            surface: 1,
            track: 1,
        }
    }
    #[test]
    fn undrained_outcomes_keep_admission_charged_and_owner_pressure_is_isolated() {
        let reports = Arc::new(Reports::default());
        for id in 1..=4 {
            reports
                .reserve(source(1), id)
                .unwrap()
                .complete(PresentationOutcome::Presented);
        }
        assert!(!reports.available(source(1)));
        assert!(reports.available(source(2)));
        assert_eq!(
            reports.reserve(source(1), 5).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(
            reports.reserve(source(2), 1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let peer = reports.reserve(source(2), 6).unwrap();
        let ready = reports.take();
        assert_eq!(ready.len(), 4);
        assert!(ready.iter().all(|r|r.source==source(1) && r.outcome==Some(PresentationOutcome::Presented)));
        assert!(reports.available(source(1)));
        let resumed = reports.reserve(source(1), 7).unwrap();
        drop(resumed);
        let ready = reports.take();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].outcome, None);
        assert_eq!(ready[0].source, source(1));
        peer.complete(PresentationOutcome::Presented);
        let ready = reports.take();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].source, source(2));
    }
    #[test]
    fn global_capacity_cancellation_and_late_callbacks_remain_bounded() {
        let reports = Arc::new(Reports::default());
        let mut live = Vec::new();
        for owner in 1..=16 {
            for id in 1..=4 {
                live.push(reports.reserve(source(owner), owner * 4 + id).unwrap());
            }
        }
        assert_eq!(
            reports.reserve(source(17), 100).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(!reports.available(source(17)));
        reports.cancel();
        let ready = reports.take();
        assert!(!reports.available(source(17)));
        assert_eq!(ready.len(), 64);
        assert!(ready.iter().all(|r| r.outcome.is_none()));
        for receipt in live {
            receipt.complete(PresentationOutcome::Presented);
        }
        assert!(reports.take().is_empty());
        assert_eq!(
            reports.reserve(source(17), 101).unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
    }
}
