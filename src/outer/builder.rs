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
    documents: Option<std::sync::Weak<vivid_sdk::presenter::VirtualVivid>>,
    focus: Option<std::sync::Weak<vivid_sdk::presenter::VirtualVivid>>,
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
            documents: None,
            focus: None,
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

    /// Opts into typed, owner-scoped document re-origination on terminal targets.
    ///
    /// Off by default. The caller must service host requests, drain ordinary input,
    /// then drain `take_overlay_document_input` and forward each opaque input to
    /// its original inner presenter. Install negotiated services with the SDK
    /// document-aware host-profile setter. Legacy mux clients cannot route this type and
    /// must leave this disabled. The outer native host must also negotiate the profile.
    #[must_use]
    pub fn overlay_document_editing(
        mut self,
        inner: &Arc<vivid_sdk::presenter::VirtualVivid>,
    ) -> Self {
        self.documents = Some(Arc::downgrade(inner));
        self
    }

    /// Forwards producer-requested focus through its original terminating presenter.
    ///
    /// Off by default. Enable host relaying on `inner` before building; this
    /// attaches an exclusive SDK focus route. Continue servicing bounded host
    /// requests and physical input events. Legacy snapshot-only mux routes are unchanged.
    #[must_use]
    pub fn overlay_focus_forwarding(
        mut self,
        inner: &Arc<vivid_sdk::presenter::VirtualVivid>,
    ) -> Self {
        self.focus = Some(Arc::downgrade(inner));
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
        if (self.documents.is_some() || self.focus.is_some())
            && self.target_profile != registry::TERMINAL_SURFACE
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "focus and document relays require a terminal overlay target",
            ));
        }
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
        let session = route.connect(
            &self.authentication,
            &self.target_profile,
            self.documents.is_some(),
        )?;
        let mut bridge =
            OuterBridge::from_session(session, self.authentication, route, self.display)?;
        bridge.document_relay = self.documents.is_some();
        bridge.document_target = self.documents.unwrap_or_default();
        if let Some(inner) = self.focus {
            if !bridge
                .overlay_host_profiles()
                .iter()
                .any(|profile| profile == registry::OVERLAY_INPUT)
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "focus relay requires outer overlay input",
                ));
            }
            let focus = inner
                .upgrade()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "original focus presenter is gone")
                })?
                .attach_overlay_focus_relay()?;
            bridge.cancellation.install_focus(&focus);
            bridge.focus_relay = Some(focus);
        }
        Ok(bridge)
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
            .field("documents", &self.documents.is_some())
            .field("focus", &self.focus.is_some())
            .finish()
    }
}
