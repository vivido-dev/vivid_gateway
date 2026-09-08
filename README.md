# vivid_gateway

`vivid_gateway` is the Vivid 1.5 re-origination core used by `vvmux`.
It terminates an authenticated inner producer session as a presenter and
re-originates validated surfaces, tracks, scene nodes, and media into an independent outer producer
session.

The terminating half is `vivid_sdk::presenter`, which this crate re-exports so
`vivid_gateway::VirtualVivid` and its neighbours keep resolving; the re-origination half is
`vivid_gateway::outer`. Both halves are still reached through this crate.

The two hops never share secrets, protocol identities, revisions, generations, epochs, or media
IDs. Products provide the inner accepted-connection listener and the outer `ConnectionFactory`.
The inner target is configurable through `PresentationTarget`; terminal and desktop targets are
included. Resource contracts, supported profiles, and additional capture restrictions are route
policy.

Media assembly validates source, record kind, declared size, offset, completion flag and delivery
identity before reserving storage. Incomplete bodies share a 64 MiB budget, with at most 256
assemblies; entries older than 30 seconds are discarded on the next chunk admission. A source
replacement or removal also discards its incomplete body. Invalid chunks fail without consuming
another source's assembly. Raster deltas must reference the last successfully forwarded inner
frame; recovery remains pending until a new full frame is forwarded successfully.

Both event-polling APIs preserve terminal errors and pending display changes. `poll_outer_session`
reports whether that call advanced the target; `service_session_events` consumes the pending
display change and continues reporting a terminal error until session replacement. Replacement
cancels the abandoned root session without waiting for GOODBYE and retires its queued playback,
loss and recovery notifications. Delivery completions retain their existing writer-identity checks.

Failed remote object setup retains cleanup ownership until destruction succeeds. Failed scene,
track and surface deletion likewise retains the identity needed to retry; replacing the entire
session discards those retired handles. Microphone cleanup destroys its owning surface and track
together, with failed setup retained for cleanup on the next synchronization.

Run the standalone gates from this directory:

```text
cargo fmt --all --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

## Playback position feedback

The outer bridge has one bounded background TRACK_STATUS observer with one outstanding query,
round-robin across active timed tracks at up to 20 queries per second. Control reconciliation and
media forwarding do not wait on that observer. Results are checked against the source owner,
writer identity, channel generation, decoder-reset serial and current playback request before
being forwarded as private `BridgePositionSnapshot` metadata. Session replacement and shutdown
cancel outstanding observations and join the observer. Playback-ended observations share this
worker. No protocol assignments or media wire formats change.

EOS completion is reported by the background physical-status observer. The foreground bridge
never synchronously drains a track after EOS; paused audio must leave seek/resume control live.

Playback completion snapshots include the source decoder reset serial; consumers must pass it
back to the virtual presenter so a late EOS report cannot end a replacement generation.
