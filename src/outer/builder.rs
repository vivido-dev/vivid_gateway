//! Configuration and connection of an [`OuterBridge`].

use std::io;
use std::sync::Arc;

use vivid_protocol::auth::Secret32;
use vivid_protocol::registry;
use vivid_sdk::ConnectionFactory;
use vivid_sdk::presenter::DisplayMetrics;

use super::{OuterBridge, Route};

/// Configures and connects an [`OuterBridge`].
///
/// Create one with [`OuterBridge::builder`]. A bridge reaches its outer presenter either through
/// native endpoints, starting with [`control_endpoint`](Self::control_endpoint), or through a
/// [`connection_factory`](Self::connection_factory), never both. The outer target profile defaults
/// to `terminal-surface-v1`.
pub struct OuterBridgeBuilder {
    authentication: Secret32,
    display: DisplayMetrics,
    control: Option<String>,
    realtime: Option<String>,
    bulk: Option<String>,
    factory: Option<Arc<dyn ConnectionFactory>>,
    target_profile: String,
}

impl OuterBridge {
    /// Starts configuring a bridge that authenticates to its outer presenter with `authentication`.
    ///
    /// `display` is the terminal geometry used until, and whenever, the outer presenter's target
    /// descriptor does not carry a complete one; a desktop target always uses it.
    #[must_use]
    pub fn builder(authentication: Secret32, display: DisplayMetrics) -> OuterBridgeBuilder {
        OuterBridgeBuilder {
            authentication,
            display,
            control: None,
            realtime: None,
            bulk: None,
            factory: None,
            target_profile: registry::TERMINAL_SURFACE.into(),
        }
    }
}

impl OuterBridgeBuilder {
    /// Connects the control lane to this native endpoint.
    ///
    /// Without [`bulk_endpoint`](Self::bulk_endpoint) the bulk lane uses this endpoint as well.
    #[must_use]
    pub fn control_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.control = Some(endpoint.into());
        self
    }

    /// Connects the realtime lane to this native endpoint instead of the bulk one.
    #[must_use]
    pub fn realtime_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.realtime = Some(endpoint.into());
        self
    }

    /// Connects the bulk lane to this native endpoint instead of the control one.
    #[must_use]
    pub fn bulk_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.bulk = Some(endpoint.into());
        self
    }

    /// Opens every outer connection through `factory` instead of native endpoints.
    ///
    /// Cancelling the bridge also cancels the factory.
    #[must_use]
    pub fn connection_factory(mut self, factory: Arc<dyn ConnectionFactory>) -> Self {
        self.factory = Some(factory);
        self
    }

    /// Selects the outer target profile: `terminal-surface-v1` or `desktop-surface-v1`.
    #[must_use]
    /// The outer target profile this bridge's session negotiated.
    pub fn target_profile(mut self, profile: impl Into<String>) -> Self {
        self.target_profile = profile.into();
        self
    }

    /// Connects the outer producer session and returns the bridge driving it.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] when neither or both of a control endpoint and a
    /// connection factory were given, when a realtime or bulk endpoint was given without a control
    /// endpoint, or when the target profile is not a supported outer target. Connection and
    /// authentication failures, and an outer target descriptor that carries no usable terminal
    /// geometry when `display` has none either, are returned as the outer session reports them.
    pub fn build(self) -> io::Result<OuterBridge> {
        let route = match (self.control, self.factory) {
            (Some(control), None) => Route::Native {
                control,
                realtime: self.realtime,
                bulk: self.bulk,
            },
            (None, Some(factory)) if self.realtime.is_none() && self.bulk.is_none() => {
                Route::Factory(factory)
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "an outer bridge needs exactly one of a control endpoint and a connection \
                     factory, and realtime or bulk endpoints only with a control endpoint",
                ));
            }
        };
        let session = route.connect(&self.authentication, &self.target_profile)?;
        OuterBridge::from_session(session, self.authentication, route, self.display)
    }
}

impl std::fmt::Debug for OuterBridgeBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OuterBridgeBuilder")
            .field("authentication", &self.authentication)
            .field("display", &self.display)
            .field("control", &self.control)
            .field("realtime", &self.realtime)
            .field("bulk", &self.bulk)
            .field("factory", &self.factory.is_some())
            .field("target_profile", &self.target_profile)
            .finish()
    }
}
