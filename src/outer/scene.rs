//! Re-originates inner surfaces and scene nodes as independent outer objects.

use std::collections::{HashMap, HashSet};
use std::io;
use std::thread;
use std::time::{Duration, Instant};
use vivid_protocol::registry;
use vivid_sdk::presenter::{BridgeNode, BridgeSourceKind, BridgeSurface};
use vivid_sdk::{RequestMetadata, SceneNode};

use super::config::{scene_node, surface_definition};
use super::{OuterBridge, START_POLICY_SYNCHRONIZED, invalid_data, presenter_code, surface_key};

impl OuterBridge {
    /// Reconciles only the outer scene nodes with `nodes`.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] when a node names a surface with no outer surface, and
    /// [`io::ErrorKind::WouldBlock`] when a commit outlives a moving outer target. Other outer
    /// session failures are returned as the session reports them.
    pub fn update_nodes(&mut self, nodes: &[BridgeNode]) -> io::Result<()> {
        self.reconcile_nodes(nodes)
    }

    pub(super) fn reconcile_surfaces(&mut self, desired: &[BridgeSurface]) -> io::Result<()> {
        for surface in desired {
            if let Some(existing) = self.surfaces.get(&surface.key).cloned() {
                let definition = surface_definition(
                    &self.session,
                    surface,
                    Some(&existing),
                    self.enforced_surface_policy,
                )?;
                if existing.definition()? != definition {
                    self.session.update_surface(
                        &existing,
                        definition,
                        &RequestMetadata::default(),
                    )?;
                }
            } else {
                let definition =
                    surface_definition(&self.session, surface, None, self.enforced_surface_policy)?;
                let outer = self
                    .session
                    .create_surface(definition, &RequestMetadata::default())?;
                self.surfaces.insert(surface.key, outer);
            }
        }
        let mut remaining = desired.iter().collect::<Vec<_>>();
        let mut established = HashSet::new();
        while !remaining.is_empty() {
            let Some(index) = remaining.iter().position(|surface| {
                surface
                    .overlay_window
                    .and_then(|window| window.parent)
                    .is_none_or(|parent| established.contains(&parent))
            }) else {
                return Err(invalid_data("overlay parent is missing or cyclic"));
            };
            let surface = remaining.remove(index);
            self.sync_overlay_window(surface)?;
            self.sync_overlay_layouts(surface)?;
            established.insert(surface.key);
        }
        Ok(())
    }

    pub(super) fn remove_absent_surfaces(&mut self, desired: &[BridgeSurface]) -> io::Result<()> {
        let desired_keys = desired
            .iter()
            .map(|surface| surface.key)
            .collect::<HashSet<_>>();
        let removed = self
            .surfaces
            .keys()
            .copied()
            .filter(|key| !desired_keys.contains(key) && !self.retiring_surfaces.contains_key(key))
            .collect::<Vec<_>>();
        for key in removed {
            self.surface_clock.remove(&key);
            self.hold_serials.remove(&key);
            self.hold_updates.retain(|source, _| {
                source.producer != key.producer
                    || source.context != key.context
                    || source.surface != key.surface
            });
            self.overlay_windows.remove(&key);
            self.overlay_revisions.retire(Some(key));
            {
                let mut layouts = self
                    .overlay_layouts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                layouts.live.remove(&key);
                layouts.ids.retain(|(surface, _), _| *surface != key);
                layouts.published.retain(|(surface, _)| *surface != key);
            };
            if let Some(surface) = self.surfaces.get(&key).cloned() {
                self.session
                    .destroy_surface(&surface, &RequestMetadata::default())?;
                self.surfaces.remove(&key);
            }
        }
        Ok(())
    }

    /// Read the outer control connection and apply what the bridge owns on it.
    ///
    /// Nothing else drains this connection, so the target generation the SDK names on every scene
    /// commit only follows the outer terminal from here. Returns whether the target moved.
    pub fn poll_outer_session(&mut self) -> bool {
        let generation = self.session.info().target_generation;
        if let Err(error) = self.drain_session_events() {
            self.terminal_error = Some(error.to_string());
        }
        self.session.info().target_generation != generation
    }

    /// Run one scene commit, following the outer presentation target while it moves.
    ///
    /// A commit names the target generation it was planned against, so an outer resize that lands
    /// between planning and committing is answered with `STALE_TARGET_GENERATION`. The presenter
    /// announces every change that causes one, so apply what has arrived and commit again against
    /// the target the outer terminal has now. A target that is still moving when the window closes
    /// is reported as `WouldBlock`, which asks for a fresh projection in this same outer session
    /// rather than tearing down healthy sources.
    fn commit_node_following_target(&mut self, commit: NodeCommit<'_>) -> io::Result<()> {
        let deadline = Instant::now() + TARGET_FOLLOW_TIMEOUT;
        loop {
            let metadata = RequestMetadata::default();
            let attempt = match commit {
                NodeCommit::Create(node) => self.session.create_node(node, &metadata).map(|_| ()),
                NodeCommit::Update(node) => self.session.update_node(node, &metadata).map(|_| ()),
                NodeCommit::Delete {
                    context_id,
                    node_id,
                } => self
                    .session
                    .delete_node(context_id, node_id, &metadata)
                    .map(|_| ()),
            };
            match attempt {
                Ok(()) => return Ok(()),
                Err(error)
                    if presenter_code(&error) == Some(registry::error::STALE_TARGET_GENERATION) =>
                {
                    let moved = self.poll_outer_session();
                    self.drain_session_events()?;
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "outer target is still moving",
                        ));
                    }
                    if !moved {
                        thread::sleep(TARGET_FOLLOW_POLL);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub(super) fn reconcile_nodes(&mut self, nodes: &[BridgeNode]) -> io::Result<()> {
        self.desired_nodes = nodes.to_vec();
        let desired = nodes
            .iter()
            .map(|node| ((node.producer, node.node, node.fragment), node))
            .collect::<HashMap<_, _>>();
        let removed = self
            .nodes
            .keys()
            .copied()
            .filter(|key| !desired.contains_key(key))
            .collect::<Vec<_>>();
        for key in removed {
            let Some((node_id, old)) = self.nodes.get(&key).cloned() else {
                continue;
            };
            self.commit_node_following_target(NodeCommit::Delete {
                context_id: old.owning_context_id,
                node_id,
            })?;
            self.nodes.remove(&key);
        }
        for (stable, node) in desired {
            let surface = self
                .surfaces
                .get(&node.surface)
                .ok_or_else(|| invalid_data("scene node references a missing outer surface"))?;
            let node_id = self
                .nodes
                .get(&stable)
                .map(|(id, _)| *id)
                .map_or_else(|| self.session.allocate_id(), Ok)?;
            let mut replacement = scene_node(
                self.session.info().root_context_id,
                node_id,
                surface,
                node,
                &self.target_profile,
            );
            if self.active_sources.values().any(|source| {
                surface_key(source) == node.surface
                    && source.play_request.start_policy == START_POLICY_SYNCHRONIZED
                    && matches!(source.kind, BridgeSourceKind::Video { .. })
                    && self
                        .tracks
                        .get(&source.key)
                        .is_none_or(|track| !track.target_picture_ready)
            }) {
                replacement.visible = false;
            }
            match self.nodes.get(&stable) {
                Some((_, old)) if old == &replacement => {}
                Some(_) => {
                    self.commit_node_following_target(NodeCommit::Update(&replacement))?;
                    self.nodes.insert(stable, (node_id, replacement));
                }
                None => {
                    self.commit_node_following_target(NodeCommit::Create(&replacement))?;
                    self.nodes.insert(stable, (node_id, replacement));
                }
            }
        }
        Ok(())
    }
}

/// How long a scene commit keeps following a moving outer target before it asks for a fresh
/// projection instead.
const TARGET_FOLLOW_TIMEOUT: Duration = Duration::from_millis(500);

/// Pause between attempts while the announcement that explains a stale reply is still in flight.
const TARGET_FOLLOW_POLL: Duration = Duration::from_millis(5);

/// One scene mutation, named so a stale reply can be retried against the target that caused it.
#[derive(Debug, Clone, Copy)]
enum NodeCommit<'a> {
    Create(&'a SceneNode),
    Update(&'a SceneNode),
    Delete { context_id: u64, node_id: u64 },
}
