//! Reusable Vivid 1.5 terminating-gateway core.
//!
//! The inner presenter and outer producer have independent authority and identity domains. The
//! crate owns only protocol state; products provide the accepted-connection listener.

#![forbid(unsafe_code)]

mod io;
pub mod outer;
pub mod presenter;
pub mod transport;
mod types;

pub use io::{ConnectionCancel, PresenterListener, Transport};
pub use outer::{
    CapabilityChange, ConnectionFactory, OuterBridge, PlaybackSnapshot, intersect_surface_policy,
};
pub use presenter::{
    AudioSourceConfig, BridgeProjection, ClipRect, GatewayLeaseReady, KeyframeRequestOutcome,
    MediaEvent, NodeConfig, OuterMediaProjection, PlayRequest, ProducerId, ProjectionSnapshot,
    SceneNode, SceneNodeConfig, SemanticDescriptor, SnapshotSource, SnapshotSurface,
    SourceDescriptor, SourceKey, VirtualVivid,
};
pub use types::*;
