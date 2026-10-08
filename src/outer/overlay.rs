//! Relays inner overlay windows, text layouts and host requests into the outer session.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use vivid_protocol::cbor::Value;
use vivid_protocol::messages;
use vivid_protocol::overlay::wire::{SetWindow, WindowAddress};
use vivid_protocol::overlay::{WindowMode, WindowOptions};
use vivid_protocol::vector::{Rect, Scalar};
use vivid_sdk::presenter::BridgeOverlayWindow;
use vivid_sdk::presenter::{BridgeSurface, BridgeSurfaceKey};

use super::{OuterBridge, invalid_data, poisoned};

impl OuterBridge {
    /// Drain native overlay input, preserving physical keys, text/IME and pointer precision.
    ///
    /// # Errors
    ///
    /// Returns an error when the input watchdog cannot be renewed or an event cannot be encoded, and
    /// [`io::ErrorKind::BrokenPipe`] when the outer overlay input lane closes.
    pub fn take_overlay_input(&mut self) -> io::Result<Vec<(BridgeSurfaceKey, Vec<u8>)>> {
        let Some(input) = &self.overlay_input else {
            return Ok(Vec::new());
        };
        if self.overlay_renewed.elapsed() >= INPUT_RENEW_INTERVAL {
            input.renew(INPUT_WATCHDOG_US, INPUT_REQUEST_TIMEOUT)?;
            self.overlay_renewed = Instant::now();
        }
        let mut events = Vec::new();
        for _ in 0..MAX_INPUT_EVENTS_PER_CALL {
            let Some(event) = input.wait_event(Duration::ZERO)? else {
                break;
            };
            let mut event = match event {
                vivid_sdk::OverlayLaneEvent::Input(event) => event,
                vivid_sdk::OverlayLaneEvent::Environment(environment) => {
                    self.overlay_environment =
                        Some(messages::Envelope::new(0, environment.payload()?).encode()?);
                    continue;
                }
                vivid_sdk::OverlayLaneEvent::Outcome(outcome) => {
                    if outcome.outcome
                        == vivid_protocol::overlay::wire::PresentationOutcome::Presented
                        && let Some(key) = self.outer_surface_key(&outcome.submission.address)
                    {
                        self.overlay_revisions
                            .record_presented(key, outcome.submission.revision)?;
                    }
                    continue;
                }
                vivid_sdk::OverlayLaneEvent::Accessibility {
                    address,
                    scene_revision,
                    node,
                    action,
                } => vivid_protocol::overlay::wire::InputEvent {
                    address,
                    scene_revision,
                    event: vivid_protocol::overlay::Event::Accessibility { node, action },
                },
                vivid_sdk::OverlayLaneEvent::ConnectionLost { .. } => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "outer overlay input lane closed",
                    ));
                }
                vivid_sdk::OverlayLaneEvent::Viewport(_) => continue,
            };
            let Some(key) = self.outer_surface_key(&event.address) else {
                continue;
            };
            let Some(window) = self.overlay_windows.get(&key) else {
                continue;
            };
            let revision = if event.scene_revision == 0 {
                0
            } else {
                let Some(revision) = self
                    .overlay_revisions
                    .lock()?
                    .get(&key)
                    .and_then(|window| {
                        window
                            .revisions
                            .iter()
                            .find(|(outer, _)| *outer == event.scene_revision)
                    })
                    .map(|(_, inner)| *inner)
                else {
                    continue;
                };
                revision
            };
            event.address = WindowAddress {
                context_id: key.context,
                surface_id: key.surface,
                generation: window.inner_generation,
            };
            event.scene_revision = revision;
            if let vivid_protocol::overlay::Event::Geometry { bounds, .. } = &mut event.event {
                *bounds = Rect::new(
                    bounds.origin.x.get() - exact_f64(window.offset_x)?,
                    bounds.origin.y.get() - exact_f64(window.offset_y)?,
                    bounds.width.get(),
                    bounds.height.get(),
                )
                .map_err(io::Error::other)?;
            }
            events.push((key, messages::Envelope::new(0, event.payload()?).encode()?));
        }
        Ok(events)
    }

    /// The inner surface an outer window address belongs to.
    fn outer_surface_key(&self, address: &WindowAddress) -> Option<BridgeSurfaceKey> {
        self.surfaces
            .iter()
            .find(|(_, surface)| {
                surface.id() == address.surface_id
                    && surface.context_id() == address.context_id
                    && surface.generation().get() == address.generation
            })
            .map(|(key, _)| *key)
    }

    /// Takes the latest overlay environment the outer presenter announced.
    ///
    /// It is encoded as a control envelope ready to forward to the inner overlay producer.
    pub fn take_overlay_environment(&mut self) -> Option<Vec<u8>> {
        self.overlay_environment.take()
    }

    /// The overlay profiles the outer presenter accepted for this session.
    pub fn overlay_host_profiles(&self) -> Vec<String> {
        self.session
            .info()
            .accepted_profiles
            .iter()
            .filter(|profile| profile.starts_with("overlay-"))
            .cloned()
            .collect()
    }

    /// Service one bounded text request in the outer window's namespace.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] when the request's surface has no outer surface, the outer
    /// host is offline, the request type is not an overlay host request, or the scene it names is no
    /// longer relayed, and [`io::ErrorKind::TimedOut`] when the outer presenter does not show that scene
    /// in time. The outer host's own refusal is returned as it reports it.
    pub fn overlay_host_request(
        &self,
        request: &vivid_sdk::presenter::OverlayHostRequest,
    ) -> io::Result<Vec<u8>> {
        self.prepare_overlay_host_request(request.clone())?()
    }

    /// Whether [`start_overlay_host_request`](Self::start_overlay_host_request) would accept `request` now.
    ///
    /// At most 16 requests run at once. Text measurement may always start; any other request waits
    /// until its surface has no relayed media in flight, so it cannot overtake the scene it names.
    pub fn can_service_overlay_host_request(
        &self,
        request: &vivid_sdk::presenter::OverlayHostRequest,
    ) -> bool {
        self.overlay_jobs.len() < MAX_OVERLAY_JOBS
            && (request.record_type == messages::MEASURE_OVERLAY_TEXT_BATCH
                || self
                    .tracks
                    .iter()
                    .filter(|(key, _)| {
                        key.producer == request.surface.producer
                            && key.context == request.surface.context
                            && key.surface == request.surface.surface
                    })
                    .all(|(_, track)| track.media_inflight == 0))
    }

    /// Starts `request` on a worker thread.
    ///
    /// Its reply arrives through [`take_overlay_host_replies`](Self::take_overlay_host_replies).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] when
    /// [`can_service_overlay_host_request`](Self::can_service_overlay_host_request) is false, and
    /// [`io::ErrorKind::InvalidData`] when the request's surface has no outer surface or the outer
    /// overlay host is offline. A worker that cannot be spawned is reported as the system reports it.
    pub fn start_overlay_host_request(
        &mut self,
        request: vivid_sdk::presenter::OverlayHostRequest,
    ) -> io::Result<()> {
        if !self.can_service_overlay_host_request(&request) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "overlay host worker is busy",
            ));
        }
        let id = request.id;
        let job = self.prepare_overlay_host_request(request)?;
        let worker = thread::Builder::new()
            .name("vivid-overlay-host".into())
            .spawn(job)?;
        self.overlay_jobs.push((id, worker));
        Ok(())
    }

    /// Takes the replies of finished overlay host requests, keyed by request ID.
    ///
    /// A request abandoned by a session replacement replies with [`io::ErrorKind::Interrupted`].
    pub fn take_overlay_host_replies(&mut self) -> Vec<(u64, io::Result<Vec<u8>>)> {
        let mut index = 0;
        while index < self.overlay_jobs.len() {
            if self.overlay_jobs[index].1.is_finished() {
                let (id, worker) = self.overlay_jobs.swap_remove(index);
                self.overlay_results.push((
                    id,
                    worker
                        .join()
                        .unwrap_or_else(|_| Err(io::Error::other("overlay host worker panicked"))),
                ));
            } else {
                index += 1;
            }
        }
        std::mem::take(&mut self.overlay_results)
    }

    pub(super) fn retire_overlay_jobs(&mut self) {
        for (id, worker) in self.overlay_jobs.drain(..) {
            let _ = worker.join();
            self.overlay_results.push((
                id,
                Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "outer overlay session was replaced",
                )),
            ));
        }
    }

    fn prepare_overlay_host_request(
        &self,
        request: vivid_sdk::presenter::OverlayHostRequest,
    ) -> io::Result<OverlayJob> {
        use vivid_protocol::overlay::wire::text::styled::{
            BatchMeasured, MeasureBatch, ReleaseLayouts,
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
        let host = self
            .session
            .overlay_host_handle()
            .ok_or_else(|| invalid_data("outer overlay host is offline"))?;
        let overlay_layouts = Arc::clone(&self.overlay_layouts);
        let overlay_revisions = Arc::clone(&self.overlay_revisions);
        let input = self.overlay_input.clone();
        Ok(Box::new(move || {
            let envelope = messages::decode_control(&request.body)?;
            let value = Value::Map(envelope.payload);
            let payload = match request.record_type {
                messages::OVERLAY_INPUT_CAPTURE => {
                    let mut query = vivid_protocol::overlay::wire::Capture::decode(
                        request.surface.surface,
                        &value,
                    )?;
                    query.address = address;
                    query.scene_revision = overlay_revisions
                        .lock()?
                        .get(&request.surface)
                        .and_then(|window| {
                            window
                                .revisions
                                .iter()
                                .rev()
                                .find(|(_, inner)| *inner == query.scene_revision)
                        })
                        .map(|(outer, _)| *outer)
                        .ok_or_else(|| invalid_data("outer capture scene is unavailable"))?;
                    input
                        .as_ref()
                        .ok_or_else(|| invalid_data("outer overlay input lane is absent"))?
                        .capture(query, INPUT_REQUEST_TIMEOUT)?;
                    Vec::new()
                }
                messages::MEASURE_OVERLAY_TEXT_BATCH => {
                    let mut query = MeasureBatch::decode(request.surface.surface, &value)
                        .map_err(io::Error::other)?;
                    let inner = query.address;
                    query.address = address;
                    let fields = host(
                        messages::MEASURE_OVERLAY_TEXT_BATCH,
                        address.surface_id,
                        query.payload()?,
                    )?;
                    let mut answer = BatchMeasured::decode(address, &Value::Map(fields))?;
                    if query.retain {
                        if request.layout_ids.len() != answer.layouts.len() {
                            return Err(invalid_data("overlay layout identity count mismatch"));
                        }
                        let mut layouts = overlay_layouts
                            .lock()
                            .map_err(poisoned("overlay layouts"))?;
                        if !layouts.live.contains(&request.surface) {
                            return Err(invalid_data(
                                "overlay surface disappeared during measurement",
                            ));
                        }
                        for ((outer_id, _), inner_id) in
                            answer.layouts.iter_mut().zip(&request.layout_ids)
                        {
                            layouts.ids.insert((request.surface, *inner_id), *outer_id);
                            *outer_id = *inner_id;
                        }
                    }
                    answer.address = inner;
                    answer.payload().map_err(io::Error::other)?
                }
                messages::RELEASE_OVERLAY_TEXT_LAYOUTS => {
                    let mut query = ReleaseLayouts::decode(request.surface.surface, &value)
                        .map_err(io::Error::other)?;
                    query.address = address;
                    {
                        let layouts = overlay_layouts
                            .lock()
                            .map_err(poisoned("overlay layouts"))?;
                        for id in &mut query.ids {
                            *id = *layouts
                                .ids
                                .get(&(request.surface, *id))
                                .ok_or_else(|| invalid_data("missing outer layout"))?;
                        }
                    }
                    // The inner snapshot retains released layouts while its displayed frame uses
                    // them. Retire the physical layout when it leaves that authoritative snapshot.
                    Vec::new()
                }
                messages::SET_OVERLAY_CLIPBOARD => {
                    let mut query = vivid_protocol::overlay::wire::Clipboard::decode(
                        request.surface.surface,
                        &value,
                    )?;
                    query.address = address;
                    host(request.record_type, address.surface_id, query.payload()?)?
                }
                messages::SET_OVERLAY_EDITOR | messages::SET_OVERLAY_SEMANTICS => {
                    let map_revision = |inner| -> io::Result<u64> {
                        // Presentation outcomes arrive on the outer input lane. Without one the
                        // relayed scene can only be named, not waited for.
                        if input.is_some() {
                            return overlay_revisions.wait_presented(
                                request.surface,
                                inner,
                                OUTER_PRESENTATION_WAIT,
                            );
                        }
                        overlay_revisions
                            .lock()?
                            .get(&request.surface)
                            .and_then(|window| {
                                window
                                    .revisions
                                    .iter()
                                    .rev()
                                    .find(|(_, revision)| *revision == inner)
                            })
                            .map(|(outer, _)| *outer)
                            .ok_or_else(|| invalid_data("outer scene revision is unavailable"))
                    };
                    let payload = if request.record_type == messages::SET_OVERLAY_EDITOR {
                        let mut query =
                            vivid_protocol::overlay::wire::text::EditorGeometry::decode(
                                request.surface.surface,
                                &value,
                            )?;
                        query.address = address;
                        query.scene_revision = map_revision(query.scene_revision)?;
                        query.payload()?
                    } else {
                        let mut query = vivid_protocol::overlay::wire::SetSemantics::decode(
                            request.surface.surface,
                            &value,
                        )?;
                        query.address = address;
                        query.semantics.scene_revision =
                            map_revision(query.semantics.scene_revision)?;
                        query.payload()?
                    };
                    host(request.record_type, address.surface_id, payload)?
                }
                _ => return Err(invalid_data("unsupported overlay host request")),
            };
            Ok(messages::Envelope::new(envelope.request_id, payload).encode()?)
        }))
    }

    /// Relay one surface's overlay window to the outer session, if it has one and its geometry
    /// changed since the last relay. The outer presenter's own window revision is tracked so the
    /// next update names the correct `expected_revision`; an outer presenter that never negotiated
    /// the overlay bundle simply returns an error here, which the caller surfaces like any other
    /// unsupported relayed configuration.
    pub(super) fn sync_overlay_layouts(&mut self, surface: &BridgeSurface) -> io::Result<()> {
        if surface.overlay_window.is_none() {
            return Ok(());
        }
        self.overlay_layouts
            .lock()
            .map_err(poisoned("overlay layouts"))?
            .live
            .insert(surface.key);
        for layout in &surface.overlay_layouts {
            if self
                .overlay_layouts
                .lock()
                .map_err(poisoned("overlay layouts"))?
                .ids
                .contains_key(&(surface.key, layout.id))
            {
                continue;
            }
            self.overlay_host_request(&vivid_sdk::presenter::OverlayHostRequest {
                id: 0,
                surface: surface.key,
                record_type: messages::MEASURE_OVERLAY_TEXT_BATCH,
                body: layout.body.clone(),
                layout_ids: vec![layout.id],
            })?;
        }
        let desired: HashSet<_> = surface
            .overlay_layouts
            .iter()
            .map(|layout| (surface.key, layout.id))
            .collect();
        let layouts = self
            .overlay_layouts
            .lock()
            .map_err(poisoned("overlay layouts"))?;
        let removed: Vec<_> = layouts
            .published
            .iter()
            .filter(|key| key.0 == surface.key && !desired.contains(key))
            .copied()
            .collect();
        let ids = removed
            .iter()
            .filter_map(|key| layouts.ids.get(key).copied())
            .collect::<Vec<_>>();
        drop(layouts);
        if !ids.is_empty() {
            let outer = &self.surfaces[&surface.key];
            self.session.release_overlay_layouts(
                &vivid_protocol::overlay::wire::text::styled::ReleaseLayouts {
                    address: WindowAddress {
                        context_id: outer.context_id(),
                        surface_id: outer.id(),
                        generation: outer.generation().get(),
                    },
                    ids,
                },
            )?;
        }
        let mut layouts = self
            .overlay_layouts
            .lock()
            .map_err(poisoned("overlay layouts"))?;
        for key in removed {
            layouts.ids.remove(&key);
            layouts.published.remove(&key);
        }
        layouts.published.extend(desired);
        Ok(())
    }

    pub(super) fn sync_overlay_window(&mut self, surface: &BridgeSurface) -> io::Result<()> {
        let Some(window) = surface.overlay_window else {
            self.overlay_windows.remove(&surface.key);
            return Ok(());
        };
        if self.overlay_input.is_none() {
            let input = self
                .session
                .open_overlay_input_lane(INPUT_LANE_GENERATION)?;
            input.renew(INPUT_WATCHDOG_US, INPUT_REQUEST_TIMEOUT)?;
            self.overlay_input = Some(Arc::new(input));
            self.overlay_renewed = Instant::now();
        }
        if self
            .overlay_windows
            .get(&surface.key)
            .is_some_and(|state| state.projected == window)
        {
            return Ok(());
        }
        let outer = self
            .surfaces
            .get(&surface.key)
            .ok_or_else(|| invalid_data("outer surface was not created"))?;
        let mut expected_revision = self
            .overlay_windows
            .get(&surface.key)
            .map_or(0, |state| state.outer_revision);
        if expected_revision != 0 {
            expected_revision = self
                .session
                .query_overlay_window(vivid_protocol::overlay::wire::Query {
                    context_id: outer.context_id(),
                    surface_id: outer.id(),
                })?
                .window
                .expected_revision;
        }
        let request = SetWindow {
            address: WindowAddress {
                context_id: outer.context_id(),
                surface_id: outer.id(),
                generation: outer.generation().get(),
            },
            expected_revision,
            options: WindowOptions {
                bounds: Rect::new(
                    exact_f64(window.x)?,
                    exact_f64(window.y)?,
                    exact_f64(window.width)?,
                    exact_f64(window.height)?,
                )
                .map_err(io::Error::other)?,
                mode: window_mode_from_wire(window.mode)?,
                visible: window.visible,
                parent: window
                    .parent
                    .map(|parent| {
                        if parent.producer != surface.key.producer
                            || parent.context != surface.key.context
                        {
                            return Err(invalid_data(
                                "overlay parent belongs to another owner or context",
                            ));
                        }
                        let parent = self
                            .surfaces
                            .get(&parent)
                            .ok_or_else(|| invalid_data("overlay parent is absent"))?;
                        vivid_protocol::identity::SessionIdentity::new(
                            vivid_protocol::identity::PresenterInstanceId([0; 16]),
                            self.session.info().session_id,
                        )
                        .and_then(|session| session.context(parent.context_id()))
                        .and_then(|context| context.surface(parent.id()))
                        .map_err(io::Error::other)
                    })
                    .transpose()?,
                min_width: Scalar::new(exact_f64(window.min_width.max(1))?)
                    .map_err(io::Error::other)?,
                min_height: Scalar::new(exact_f64(window.min_height.max(1))?)
                    .map_err(io::Error::other)?,
            },
        };
        let accepted = self.session.set_overlay_window(&request)?;
        self.overlay_windows.insert(
            surface.key,
            OverlayWindowState {
                offset_x: window.offset_x,
                offset_y: window.offset_y,
                inner_generation: window.generation,
                projected: window,
                outer_revision: accepted.expected_revision,
            },
        );
        Ok(())
    }
}

type OverlayJob = Box<dyn FnOnce() -> io::Result<Vec<u8>> + Send>;

pub(super) type OverlayRevisions = Arc<RelayedScenes>;

/// How long a scene-bound request waits for the outer presenter to put its scene on screen.
///
/// The inner presenter reports a scene presented once it holds it, which is before the outer one
/// has composed the relayed copy; one outer frame is the usual wait.
const OUTER_PRESENTATION_WAIT: Duration = Duration::from_secs(1);

/// Watchdog the outer presenter arms on the overlay input lane.
///
/// The outer presenter revokes overlay input when no renewal arrives within it, so the bridge must
/// renew well inside this window even when its caller polls slowly.
const INPUT_WATCHDOG_US: u64 = 2_000_000;
/// How often [`OuterBridge::take_overlay_input`] renews the watchdog: a quarter of it.
const INPUT_RENEW_INTERVAL: Duration = Duration::from_millis(500);
/// Longest wait for the outer presenter to answer a renewal or an input capture.
const INPUT_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
/// Generation of the overlay input lane; one lane is opened per outer session.
const INPUT_LANE_GENERATION: u64 = 1;
/// Overlay input events drained per call, so one busy window cannot starve the caller's loop.
const MAX_INPUT_EVENTS_PER_CALL: usize = 64;
/// Overlay host requests that may run at once, each on its own worker thread.
const MAX_OVERLAY_JOBS: usize = 16;
/// Relayed scenes remembered per window for translating between inner and outer revisions.
///
/// An input event, capture, caret or semantic tree naming an older scene can no longer be
/// translated and is dropped or refused.
pub(super) const RELAYED_REVISIONS: usize = 64;

/// Which inner scene each relayed outer scene carries, and which one the outer presenter shows.
#[derive(Default)]
pub(super) struct RelayedScenes {
    windows: Mutex<HashMap<BridgeSurfaceKey, WindowScenes>>,
    presented: Condvar,
}

#[derive(Default)]
pub(super) struct WindowScenes {
    /// `(outer, inner)` revisions of the last [`RELAYED_REVISIONS`] scenes relayed, oldest first.
    pub(super) revisions: VecDeque<(u64, u64)>,
    /// The outer revision the outer presenter last reported presented.
    pub(super) presented: Option<u64>,
}

impl RelayedScenes {
    pub(super) fn lock(
        &self,
    ) -> io::Result<std::sync::MutexGuard<'_, HashMap<BridgeSurfaceKey, WindowScenes>>> {
        self.windows.lock().map_err(poisoned("overlay revisions"))
    }

    pub(super) fn record_presented(&self, key: BridgeSurfaceKey, outer: u64) -> io::Result<()> {
        if let Some(window) = self.lock()?.get_mut(&key) {
            window.presented = Some(outer);
            self.presented.notify_all();
        }
        Ok(())
    }

    /// Forget one window's scenes, or every window's, releasing anyone waiting on them.
    pub(super) fn retire(&self, key: Option<BridgeSurfaceKey>) {
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match key {
            Some(key) => {
                windows.remove(&key);
            }
            None => windows.clear(),
        }
        self.presented.notify_all();
    }

    /// The outer revision relaying `inner`, once the outer presenter has it on screen.
    ///
    /// A semantic tree or editor caret names the scene it describes, and the outer host refuses
    /// one whose scene it is not showing. Forwarding as soon as the inner presenter holds the
    /// scene would race the outer composition, so this waits for the outer presentation outcome
    /// and fails at once when a later relayed scene replaced it.
    pub(super) fn wait_presented(
        &self,
        key: BridgeSurfaceKey,
        inner: u64,
        timeout: Duration,
    ) -> io::Result<u64> {
        let deadline = Instant::now() + timeout;
        let mut windows = self.lock()?;
        loop {
            // A window this bridge relays nothing for, or stopped relaying, has no scene to wait on.
            let window = windows
                .get(&key)
                .ok_or_else(|| invalid_data("outer scene revision is unavailable"))?;
            match window
                .revisions
                .iter()
                .rposition(|(_, relayed)| *relayed == inner)
            {
                Some(index) => {
                    let outer = window.revisions[index].0;
                    if window.presented == Some(outer) {
                        return Ok(outer);
                    }
                    if window
                        .revisions
                        .range(index + 1..)
                        .any(|(later, _)| window.presented == Some(*later))
                    {
                        return Err(invalid_data("outer scene was replaced before presentation"));
                    }
                }
                // Inner revisions only advance, so a later one relayed means this one never will be.
                None if window.revisions.iter().any(|(_, relayed)| *relayed > inner) => {
                    return Err(invalid_data("outer scene revision is unavailable"));
                }
                None => {}
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "outer scene was not presented in time",
                ));
            }
            windows = self
                .presented
                .wait_timeout(windows, remaining)
                .map_err(poisoned("overlay revisions"))?
                .0;
        }
    }
}

#[derive(Default)]
pub(super) struct OverlayLayouts {
    pub(super) live: HashSet<BridgeSurfaceKey>,
    pub(super) ids: HashMap<(BridgeSurfaceKey, u64), u64>,
    pub(super) published: HashSet<(BridgeSurfaceKey, u64)>,
}

/// Revision bookkeeping for one surface's relayed overlay window.
pub(super) struct OverlayWindowState {
    offset_x: i64,
    offset_y: i64,
    inner_generation: u64,
    /// The inner window's own revision last successfully relayed, so an unchanged snapshot does
    /// not re-send the same geometry every reconciliation pass.
    pub(super) projected: BridgeOverlayWindow,
    /// The outer window's own revision, returned by the last accepted `SET_OVERLAY_WINDOW`, and
    /// required as the next update's `expected_revision`.
    pub(super) outer_revision: u64,
}

fn window_mode_from_wire(mode: u64) -> io::Result<WindowMode> {
    match mode {
        0 => Ok(WindowMode::Floating),
        1 => Ok(WindowMode::Popup),
        2 => Ok(WindowMode::Modal),
        _ => Err(invalid_data("unknown overlay window mode")),
    }
}

/// Converts an overlay coordinate to `f64`, refusing one `f64` cannot represent exactly.
fn exact_f64(value: i64) -> io::Result<f64> {
    /// Largest magnitude every integer up to which `f64` represents exactly.
    const EXACT: i64 = 1 << f64::MANTISSA_DIGITS;
    if !(-EXACT..=EXACT).contains(&value) {
        return Err(invalid_data("overlay coordinate exceeds f64 precision"));
    }
    #[expect(
        clippy::cast_precision_loss,
        reason = "the range check above keeps the value exactly representable"
    )]
    Ok(value as f64)
}
