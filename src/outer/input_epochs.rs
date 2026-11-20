//! Opaque integration correlations for original input incarnations retained across IPC.

// Rust guideline compliant 2026-10-07
use super::{OuterBridge, invalid_data};
use std::{collections::HashMap, io};
use vivid_sdk::presenter::BridgeSurfaceKey;

// Match the SDK's pane/source bound; this table carries no grant or input bytes.
const CAPACITY: usize = 64;

/// Authenticated physical input tagged with the integration's original-incarnation correlation.
///
/// Metadata only. The integration must validate the reporting client and bridge,
/// then forward using its server-retained original SDK input stamp.
pub struct ExternalOverlayInput {
    /// Integration-owned, nonzero original input-incarnation correlation.
    pub correlation: u64,
    /// Complete original source window key.
    pub surface: BridgeSurfaceKey,
    /// Encoded original-address input, bounded by the authenticated outer lane.
    pub body: Vec<u8>,
}
impl std::fmt::Debug for ExternalOverlayInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalOverlayInput")
            .field("correlation", &self.correlation)
            .field("surface", &self.surface)
            .finish_non_exhaustive()
    }
}
impl OuterBridge {
    /// Sets bounded input-incarnation metadata before reconciling the corresponding projection.
    ///
    /// Each nonzero correlation names a server-retained original SDK input stamp.
    /// Replacement correlations retire only the affected physical windows during
    /// the next `rebuild`/`rebuild_resetting`; existing independent peers survive.
    /// Missing entries disable forwarding for that source. Until reconciliation,
    /// old physical input is refused. Use `take_overlay_input_with_epochs` once
    /// this mode is enabled; untagged draining is refused even for an empty table.
    ///
    /// # Errors
    /// Returns `Unsupported` with a locally bound action relay, or `InvalidData`
    /// for invalid keys, duplicate keys/correlations, zero correlations or more
    /// than 64 entries. Returns `Interrupted` after bridge retirement.
    pub fn set_external_overlay_input_epochs(
        &mut self,
        epochs: &[(BridgeSurfaceKey, u64)],
    ) -> io::Result<bool> {
        if self
            .cancellation
            .cancelled
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.action_relay.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "external input cannot bind a local action route",
            ));
        }
        let desired = checked_epochs(epochs)?;
        let changed = self.external_input_epochs.as_ref() != Some(&desired);
        self.external_input_epochs = Some(desired);
        Ok(changed)
    }

    /// Drains physical input with its exact bound integration correlation.
    ///
    /// Retired physical addresses and input whose observed binding no longer
    /// matches the desired incarnation are discarded. Queued old input can never
    /// inherit the current source's correlation after a replacement.
    ///
    /// # Errors
    /// Returns `Unsupported` before external input mode is configured, or outer
    /// lane decoding, lease renewal and connection errors. Retired bridges return
    /// `Interrupted`.
    pub fn take_overlay_input_with_epochs(&mut self) -> io::Result<Vec<ExternalOverlayInput>> {
        if self
            .cancellation
            .cancelled
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.external_input_epochs.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "external input epochs are not configured",
            ));
        }
        let input = self.drain_overlay_input()?;
        Ok(input
            .into_iter()
            .filter_map(|(surface, body)| {
                self.external_input_bound
                    .get(&surface)
                    .copied()
                    .flatten()
                    .map(|correlation| ExternalOverlayInput {
                        correlation,
                        surface,
                        body,
                    })
            })
            .collect())
    }
}

fn checked_epochs(
    epochs: &[(BridgeSurfaceKey, u64)],
) -> io::Result<HashMap<BridgeSurfaceKey, u64>> {
    if epochs.len() > CAPACITY {
        return Err(invalid_data("external input epoch table exceeds capacity"));
    }
    let mut desired = HashMap::new();
    let mut values = std::collections::HashSet::new();
    for (key, correlation) in epochs {
        if key.producer == 0
            || key.context == 0
            || key.surface == 0
            || *correlation == 0
            || desired.insert(*key, *correlation).is_some()
            || !values.insert(*correlation)
        {
            return Err(invalid_data("invalid external input epoch correlation"));
        }
    }
    Ok(desired)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_epoch_metadata_is_bounded_unique_and_debug_redacted() {
        let key = BridgeSurfaceKey {
            producer: 1,
            context: 1,
            surface: 1,
        };
        let peer = BridgeSurfaceKey { producer: 2, ..key };
        assert_eq!(checked_epochs(&[(key, 1), (peer, 2)]).unwrap().len(), 2);
        for invalid in [
            vec![(key, 0)],
            vec![(key, 1), (key, 2)],
            vec![(key, 1), (peer, 1)],
            vec![(BridgeSurfaceKey { producer: 0, ..key }, 1)],
        ] {
            checked_epochs(&invalid).unwrap_err();
        }
        let mut full = (1..=64)
            .map(|producer| (BridgeSurfaceKey { producer, ..key }, producer))
            .collect::<Vec<_>>();
        assert_eq!(checked_epochs(&full).unwrap().len(), 64);
        full.push((
            BridgeSurfaceKey {
                producer: 65,
                ..key
            },
            65,
        ));
        checked_epochs(&full).unwrap_err();
        let input = ExternalOverlayInput {
            correlation: 1,
            surface: key,
            body: b"private input body".to_vec(),
        };
        assert!(!format!("{input:?}").contains("private input body"));
    }
}
