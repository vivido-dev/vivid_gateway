//! Microphone requests re-originated in the foreground attachment's Vivid session.
use crate::{BridgeSourceKey, MicrophoneRequest};
use std::{collections::HashMap, io};
use vivid_protocol::{audio_input, registry};
use vivid_sdk::{
    CoordinateModel, RequestMetadata, Session, Surface, SurfaceDefinition, SurfaceDescriptor,
    SurfaceRole, Track, TrackChannel,
};

struct Route {
    request: MicrophoneRequest,
    surface: Surface,
    track: Track,
    channel: TrackChannel,
    ended: bool,
}
#[derive(Default)]
pub(crate) struct Microphones {
    routes: HashMap<BridgeSourceKey, Route>,
    unfinished: HashMap<BridgeSourceKey, Surface>,
}

impl Microphones {
    pub fn requests(&self) -> Vec<MicrophoneRequest> {
        self.routes.values().map(|r| r.request.clone()).collect()
    }
    pub fn sync(
        &mut self,
        session: &mut Session,
        requests: &[MicrophoneRequest],
    ) -> io::Result<()> {
        if !session
            .info()
            .accepted_profiles
            .iter()
            .any(|p| p == registry::AUDIO_INPUT)
        {
            return if requests.is_empty() {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "outer presenter does not support microphones",
                ))
            };
        }
        if requests.len() > 64 {
            return Err(io::Error::other("too many microphone routes"));
        }
        for key in self.unfinished.keys().copied().collect::<Vec<_>>() {
            session.destroy_surface(&self.unfinished[&key], &RequestMetadata::default())?;
            self.unfinished.remove(&key);
        }
        let retired: Vec<_> = self
            .routes
            .iter()
            .filter(|(key, _)| !requests.iter().any(|request| request.source == **key))
            .map(|(key, _)| *key)
            .collect();
        for key in retired {
            if let Some(route) = self.routes.get(&key) {
                let _ = route.channel.close();
                // Surface destruction also destroys its microphone track atomically.
                session.destroy_surface(&route.surface, &RequestMetadata::default())?;
                self.routes.remove(&key);
            }
        }
        for request in requests {
            if let Some(route) = self.routes.get_mut(&request.source) {
                if route.request.generation != request.generation {
                    // Preserve the outer track identity so local selection remains pinned, but
                    // advance its channel: replacement generations never inherit capture consent.
                    let _ = route.channel.close();
                    session.advance_channel(&route.track, 1, &RequestMetadata::default())?;
                    let channel = session.open_track_channel(&route.track)?;
                    if let Err(error) = channel.grant_audio_input() {
                        let _ = channel.close();
                        return Err(error);
                    }
                    route.channel = channel;
                    route.ended = false;
                }
                route.request = request.clone();
                continue;
            }
            let context = session.info().root_context_id;
            let id = session.allocate_id()?;
            let track_id = session.allocate_id()?;
            let title = format!("pane {}: {}", request.pane, request.title);
            let surface = session.create_surface(
                SurfaceDefinition {
                    context_id: context,
                    surface_id: id,
                    semantic_profile: registry::GENERIC_CONTENT.into(),
                    coordinate_model: CoordinateModel::CanvasLogicalUnits,
                    logical_width: 1,
                    logical_height: 1,
                    scale_numerator: 1,
                    scale_denominator: 1,
                    rotation: 0,
                    descriptor: SurfaceDescriptor {
                        role: SurfaceRole::ApplicationCanvas,
                        title: title.chars().filter(|c| !c.is_control()).take(60).collect(),
                        semantic_content_revision: 0,
                        semantic_availability: 0,
                        locator_hint: String::new(),
                    },
                    policy: 0,
                    profile_parameters: Vec::new(),
                },
                &RequestMetadata::default(),
            )?;
            self.unfinished.insert(request.source, surface.clone());
            let track = match session.create_track(
                audio_input::configuration(context, surface.id(), track_id),
                &RequestMetadata::default(),
            ) {
                Ok(track) => track,
                Err(error) => {
                    if session
                        .destroy_surface(&surface, &RequestMetadata::default())
                        .is_ok()
                    {
                        self.unfinished.remove(&request.source);
                    }
                    return Err(error);
                }
            };
            let channel = match session.open_track_channel(&track).and_then(|channel| {
                if let Err(error) = channel.grant_audio_input() {
                    let _ = channel.close();
                    return Err(error);
                }
                Ok(channel)
            }) {
                Ok(channel) => channel,
                Err(error) => {
                    if session
                        .destroy_surface(&surface, &RequestMetadata::default())
                        .is_ok()
                    {
                        self.unfinished.remove(&request.source);
                    }
                    return Err(error);
                }
            };
            self.unfinished.remove(&request.source);
            self.routes.insert(
                request.source,
                Route {
                    request: request.clone(),
                    surface,
                    track,
                    channel,
                    ended: false,
                },
            );
        }
        Ok(())
    }

    pub fn take(&mut self) -> io::Result<Vec<(BridgeSourceKey, u64, Vec<u8>)>> {
        let mut packets = Vec::new();
        for (key, route) in &mut self.routes {
            if route.ended {
                continue;
            }
            match route.channel.take_audio_input() {
                Ok(Some(packet)) => {
                    packets.push((*key, route.request.generation, packet.encode()?));
                    route.channel.grant_audio_input()?;
                }
                Ok(None) => {}
                Err(_) => {
                    route.ended = true;
                    packets.push((*key, route.request.generation, Vec::new()));
                }
            }
        }
        Ok(packets)
    }
}

impl Drop for Microphones {
    fn drop(&mut self) {
        for route in self.routes.values() {
            let _ = route.channel.close();
        }
    }
}
