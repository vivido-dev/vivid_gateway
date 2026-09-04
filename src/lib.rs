//! Vivid 1.5 re-origination gateway.
//!
//! A gateway terminates one authenticated session and originates another. The terminating half is
//! [`vivid_sdk::presenter`], which this crate re-exports so the two halves keep one import path;
//! the originating half is [`outer`], which drives a `vivid_sdk` producer session.
//!
//! The inner presenter and outer producer have independent authority and identity domains. The
//! crate owns only protocol state; products provide the accepted-connection listener.

#![forbid(unsafe_code)]

pub mod outer;

/// The terminating presenter, which now lives in `vivid_sdk`.
///
/// Re-exported rather than moved-and-renamed so `vivid_gateway::VirtualVivid` and its neighbours
/// keep resolving for the products that already use them.
pub mod presenter {
    pub use vivid_sdk::presenter::*;
}

pub mod transport {
    pub use vivid_sdk::presenter::{Reader, Writer};
}

pub use outer::{
    CapabilityChange, ConnectionFactory, OuterBridge, PlaybackSnapshot, intersect_surface_policy,
};
pub use vivid_sdk::presenter::*;
