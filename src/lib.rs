//! Vivid 1.5 re-origination gateway.
//!
//! A gateway terminates one authenticated session and originates another. The terminating half is
//! [`vivid_sdk::presenter`]; this crate is the originating half. An [`OuterBridge`] takes the inner
//! presenter's projection of surfaces, tracks, scene nodes and media, and re-creates it as an
//! independent producer session on an outer presenter.
//!
//! The two hops never share secrets, protocol identities, revisions, generations, epochs or media
//! IDs: every outer object is allocated by the outer session, and inner IDs are only lookup keys.
//! The crate owns protocol state only. Products accept inner connections themselves and either
//! name the outer presenter's native endpoints or supply a [`vivid_sdk::ConnectionFactory`].
//!
//! # Examples
//!
//! ```no_run
//! use vivid_gateway::OuterBridge;
//! use vivid_protocol::auth::Secret32;
//! use vivid_sdk::presenter::DisplayMetrics;
//!
//! # fn main() -> std::io::Result<()> {
//! let secret = Secret32::from_hex(&"00".repeat(32)).map_err(std::io::Error::other)?;
//! let mut bridge = OuterBridge::builder(secret, DisplayMetrics::default())
//!     .control_endpoint("unix:/run/vivid/presenter.sock")
//!     .build()?;
//! // Relay each inner projection snapshot as it changes.
//! bridge.rebuild(&[], &[], &[])?;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

// Rust guideline compliant 2026-10-07

mod microphone;
mod outer;

#[doc(inline)]
pub use microphone::MicrophonePacket;
#[doc(inline)]
pub use outer::{
    BridgeCancel, ClockState, DeliveryOutcome, EosState, MediaChunk, OuterBridge,
    OuterBridgeBuilder, PlaybackSnapshot,
};
