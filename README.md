# vivid_gateway

`vivid_gateway` is the Vivid 1.5 re-origination core used by `vvmux`.
It terminates an authenticated inner producer session as a presenter and
re-originates validated surfaces, tracks, scene nodes, and media into an independent outer producer
session.

The terminating half is `vivid_sdk::presenter`; import its types from there. This crate is the
re-origination half: `OuterBridge`, configured with `OuterBridge::builder` and connected by
`build`, either to native outer endpoints or through a `vivid_sdk::ConnectionFactory`.

The two hops never share secrets, protocol identities, revisions, generations, epochs, or media
IDs. Products provide the inner accepted-connection listener and the outer endpoints or factory.

Plain text measurements and retained-layout batches both reach the physical host;
addresses are re-originated on each hop and returned geometry is validated against
the source text. Measurements do not wait for unrelated media delivery.
`overlay-editor-v1` is off by default. An explicitly opted-in, in-process gateway
route re-originates document context and returns opaque typed edits to its original
inner presenter. Legacy mux clients and generic/font-only outer hosts continue to
decline it. See [Document relay](#document-relay) for the integration and limits.

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
round-robin across tracks awaiting activation and active timed tracks at up to 20 queries per
second. Control reconciliation and media forwarding do not wait on that observer. In particular,
assembling a fragmented image does not issue a synchronous query per chunk. Timed decoder pre-roll
uses its existing 32-record bootstrap ceiling and authenticated per-channel credit; it does not
wait for returned credit or a readiness round trip after every access unit. One pending writer
handoff per inactive timed track and the existing bounded queues isolate backpressure. Static
retained deliveries keep their ingress-capacity barrier until activation.

Results are checked against the source owner,
writer identity, channel generation, decoder-reset serial and current playback request before
being forwarded as private `BridgePositionSnapshot` metadata. Session replacement and shutdown
cancel outstanding observations and join the observer. Playback-ended observations share this
worker. No protocol assignments or media wire formats change.

For retained images, EOS waits until the current outer writer has accepted its complete image
body. A projection snapshot can precede the requested retained replay; an empty foreground queue
alone does not prove that replay has arrived. This gate is per owner and writer generation.

EOS completion is reported by the background physical-status observer. The foreground bridge
never synchronously drains a track after EOS; paused audio must leave seek/resume control live.

Playback completion snapshots include the source decoder reset serial; consumers must pass it
back to the virtual presenter so a late EOS report cannot end a replacement generation.

## Document relay

A document-capable route must use all three parts of the contract: an opted-in
bridge bound to the original inner presenter, the SDK's document-aware profile
setter, and consumption of opaque typed document input. Configure this before
inner producers authenticate:

```rust
inner.enable_overlay_host_relay();
let mut bridge = OuterBridge::builder(authentication, display)
    .control_endpoint(outer_endpoint)
    .overlay_document_editing(&inner) // Arc<VirtualVivid>; lifetime is held weakly
    .build()?;
inner.set_overlay_document_host_profiles(&bridge.overlay_host_profiles());
```

Continue servicing projection/media changes, pending activation and bounded host
requests as usual. Keep unscheduled requests until `can_service_overlay_host_request`
returns true; publication is serialized per window while unrelated host services
remain available. Drain ordinary input before typed edits, so focus revocation is
observed first:

```rust
for (surface, body) in bridge.take_overlay_input()? {
    inner.relay_overlay_input(surface, &body)?;
}
for edit in bridge.take_overlay_document_input() {
    let accepted = edit.forward()?; // consumes the original grant; never retarget or retry
    // false means the original editor, input scope or local deadline was retired.
}
```

If action forwarding is also enabled, replace the ordinary-input loop above with
`take_overlay_routed_input` and consume each receipt's `forward()` before typed edits.

The physical presenter must actually negotiate `overlay-editor-v1`; the bridge
never requires it or manufactures support. The legacy SDK `set_overlay_host_profiles`
continues to withhold it even if a caller lists it. Generic/font-only physical hosts
and current vvmux/vlib routes still decline it. Desktop targets cannot opt in because
the terminal overlay profile is unavailable there. A new inner presenter needs a
new bound bridge; opaque inputs have no serialization or destination-replacement API.

Window addresses, scene revisions, editor incarnations and excerpt revisions are
mapped independently between authenticated sessions. Text is at most 4 KiB per
excerpt/commit/preedit, and sensitive contexts stay empty and masked. The bridge
retains at most 64 document windows and 64 revision mappings per window; one request
per window may be in flight. Counters use checked allocation and never reuse a
failed request's proposed identity. Install each immutable reverse mapping before
calling the physical host: its control reader applies the context before the host
callback acknowledges it. Each publication gets a fresh outer excerpt revision and
retains its own original input target, including when the excerpt is unchanged.
A failed or uncertain publication does not roll back admitted input or lend a new
grant to an old callback; it must not be retried as an edit. The original presenter
still rechecks the exact mapped scene and current editor authority at delivery.

At most 64 typed input records await the caller, each for at most one second locally.
An overflowing document queue drops document input while ordinary input continues.
Forwarding is one-shot and rechecks the original inner authenticated writer/resume,
source/channel/scene, input lane, focus incarnation and editor, plus the current
excerpt for absolute operations. Own ordered relative commits may follow earlier
revisions. Focus loss, hide, owner removal, cancellation and outer-session replacement
retire queued and already-drained grants. Source-local IDs cannot select a peer.
An integration must forward immediately; these receipts do not prove app mutation,
GPU submission or physical display.

The explicit native suite uses real loopback sockets on two independently authenticated
hops and the SDK `NativeVectorHost` compilation/presentation API. It checks source
scene 2 against an independently allocated outer scene, Unicode replacement,
masked fields, same-ID peers, republished contexts after focus return, slow input
expiry, outer replacement, cancellation and owner-scoped cleanup:

```sh
cargo test --all-targets --features native-vector-testing
```

Two controlled publication regressions hold a physical context acknowledgement
while delivering Unicode through the applied context, including a retained relative
composition callback from before repaint. Both reproduce commit loss against the
previous implementation. The SDK's opaque editor target now retains source/window,
track/channel/epoch, focus and lane incarnation across ordinary repaint; each edit
still names the exact current displayed scene. Old scene receipts and callbacks
from a focus round trip fail, fresh callbacks deliver once, and same-ID peers keep
their physical identity and receive no document edits. Revision history stores at
most 64 target/identity mappings per window, without retaining historical excerpts.
The [local macOS Unicode follow-up](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-unicode-macos.json)
records overlapping full typed-relay Metal checks and separate isolated fixtures.

This suite does not submit GPU work or drive a platform IME. The compositor's
developer-only `vector-relay-testing` adapter separately exercises this core with
independent producer processes, actual macOS Metal rendering, GUI display receipts,
clipboard grants and synthetic native document/composition callbacks. Static hosts
can decline timed-media and audio-gain: both profiles are optional on the outer hop;
the gateway still requires core control, live media, observability and its target.
The static-host socket regression covers that negotiation without inventing audio
or timed capabilities.

The combined check now uses explicit producer-requested focus forwarding, without
a peer pointer-focus workaround. Production GUI/mux adoption must opt into that
contract and qualify saturation, source lease/lane recovery and combined display retry. Physical input
sources/candidate menus, other platforms/devices and static Metal builds remain
unrun. The [native core evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-relay-macos.json)
and [combined GUI evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-gateway-gui-macos.json)
record the initial boundaries; the [focus follow-up](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-focus-macos.json)
records the subsequent focus coverage. Production synchronized-media negotiation is unchanged.

## Producer window-action forwarding

Producer focus, raise, lower, center and close requests travel as actions,
independently of persistent geometry snapshots.
An in-process terminating route can opt in before producers authenticate:

```rust
inner.enable_overlay_host_relay();
let mut bridge = OuterBridge::builder(authentication, display)
    .control_endpoint(outer_endpoint)
    .overlay_action_forwarding(&inner) // Arc<VirtualVivid>; exclusive original-presenter route
    .build()?;
```

Continue servicing bounded `OverlayHostRequest` jobs and returning physical input
through one-shot `take_overlay_routed_input` receipts. Forward each receipt with
`input.forward()` immediately, before draining typed document edits. Bound action
routes reject the legacy byte-returning `take_overlay_input` API. The builder attaches one opaque SDK
`OverlayActionRoute`; ordinary snapshot-only mux routes keep their existing behavior.
It is independent of document editing, which retains its separate opt-in contract.
This replaces the focus-only builder and SDK route/request API names with
`overlay_action_forwarding`, `attach_overlay_action_relay`, `OverlayActionRoute`
and `OverlayActionRequest`. No wire records or registry assignments change.

Each admission retains the exact original request, owner/context/surface, window
generation/revision and authenticated writer/resume. Focus, raise and lower also
retain the canonical input lane. Center and close require window control authority
without granting input. Altered or duplicate requests cannot claim another owner's
admission. At most 64 host RPCs await service, for at most ten seconds. A
window-action job does not wait for media delivery;
it waits for its window projection, queries the physical window's current revision,
then consumes admission immediately before dispatch to the fixed outer address.
An admitted RPC can finish after concurrent retirement; uncertain results are not
retried against replacement windows or sessions.

Window addresses and revisions are independent on each hop. The inner revision
advances only after a successful physical acknowledgement. That acknowledgement
does not synthesize local focus or input: physical focus events remain authoritative.
An inactive pane can remember the request without activating itself or another pane.
Cancellation, route drop and outer-session replacement retire pending and already
drained action admissions and clear old window focus while preserving pane activation.
After route loss, window-action requests fail closed until a fresh route attaches.

Center captures the original pane viewport, rejects a queued request if that
viewport changes, and translates the centered bounds by the relay placement offset.
The physical update uses `SET_OVERLAY_WINDOW` with its current revision and preserves
all other physical window options. Other actions use existing `OVERLAY_ACTION`.
Raise/lower preserve keyboard focus and capture. `OverlayWindow::request_close`
dismisses a parent and its descendants; `close` then releases producer-owned
surfaces and retained resources. A successful close fences later submissions from
that group. Physical dismissal before or after the RPC reply produces one
original dismissal per window.

Socket regressions cover repeated focus across different per-hop revisions,
same-ID owners, inactive panes, isolated physical peers, tampering/replay, owner
removal, route drop, cancellation and session replacement. The compositor's opt-in
macOS headless Metal check also verifies focus-driven keyboard and native editor
delivery without a pointer gesture. A separate macOS Metal action check verifies
raise/lower pixels, pane-relative center, group close and an unchanged same-ID peer.
See the [window-action evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-actions-macos.json).
Production transport adoption, load/recovery and physical platform/device
acceptance remain separate gates.

An action captures the bounded set of original owner windows already created at
admission (`OverlayActionRequest::preceding_windows`). The gateway waits for their
physical roles and revisions before dispatch, without waiting for media or unrelated
owners. Changes to that original window set or its revisions retire the admission;
`claim` rechecks it immediately before dispatch. This keeps a popup's implicit focus
on creation from overtaking an earlier producer-requested Focus.

Original and physical revisions remain independent. A revision-only acknowledgement
updates the cached original projection without issuing another physical
`SET_OVERLAY_WINDOW`. Actual option changes query the current physical revision;
options already applied by Center or native geometry feedback keep their display
receipt. Real geometry changes still invalidate stale native input targets.

With an action route attached, `can_accept_media` also waits for the original vector
window generation's physical role. `media_chunk` returns `WouldBlock` before
assembling such a chunk; retain and retry it after projection reconciliation.
This barrier applies to role creation, not every focus/geometry revision. Windowless
vector tracks and unbound legacy routes keep their existing media contract. The
bounded developer adapter retains a record even if control advances between its
readiness check and handoff, rotating blocked sources so peers remain serviceable.

The [local Metal startup evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-first-key-macos.json)
records three deterministic two-hop ordering regressions and fresh sequential/paired
GUI action launches. No physical key is retried or retargeted and the ten-second
action admission deadline is unchanged. Production integration, sustained load,
physical input/devices and other platforms remain separate gates. The full typed-relay
fixture also hit one Unicode commit timeout during overlapping acceptance workloads;
the Unicode follow-up above qualifies the corrected local publication/repaint ordering.
Sustained production workloads remain unqualified.

## Slow clients and action deadlines

Relayed actions run on the SDK's bounded host workers, leaving the authenticated
control reader available for queries and cleanup. The SDK allows at most 64 host
workers, at most eight per authenticated original producer (or a smaller negotiated
pending-request budget). The gateway runs at most 16 host jobs and four per original
producer, including text callbacks and actions. A busy origin returns `WouldBlock`
so the integration can retain its bounded queue and continue servicing peers. Reap
finished jobs with `take_overlay_host_replies` before scheduling the next batch.
The synchronous `overlay_host_request` helper runs on the caller; integrations
needing this scheduling policy must use the asynchronous API.

An action's original ten-second deadline starts at admission and includes queueing
and callback wakeup time. The SDK refuses acknowledgements after that deadline.
The gateway checks the opaque receipt before querying the physical window and
claims it immediately before mutation. Expired or retired actions are ready for
immediate rejection even when live jobs fill the pool or the original projection
is gone; `can_service_overlay_host_request` therefore can return true for a request
that `start_overlay_host_request` immediately rejects without allocating a worker.
Neither dispatch nor return of a delayed result refreshes the original deadline.

A physical mutation already admitted before expiry may have happened even when
the original call times out. Its late reply cannot advance the original window;
there is no rollback or automatic retry. A new explicit producer action queries
the independent physical revision again. The local deadline bounds original
admission and acknowledgement, rather than arbitrary external transport writes or
physical GPU presentation.

The [local macOS pressure evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-pressure-macos.json)
records real loopback checks for eight held actions and a rejected ninth, prompt
same-session queries, same-ID peer actions, queued expiry after owner cleanup,
late claimed results, four stalled font callbacks, peer retained-media receipts,
focus revocation and recovered capacity. These pressure checks use authenticated
socket/native protocol paths without GPU stress or physical input. Existing
headless Metal fixtures separately cover the final GUI integration. Sustained
multi-owner load, an unresponsive transport, combined display recovery and other
platform/device acceptance remain open gates.

## Source resume and input-lane replacement

An opted-in action route binds each physical overlay window to an opaque original
`OverlayInputEpoch`. It includes the source identity, authenticated resume generation
and canonical input-lane generation. This is comparison metadata and grants no
input. `take_overlay_routed_input` discards physical feedback while the captured
incarnation is absent or differs from the original presenter. Each returned
`OverlayRelayInput` is one-shot and non-serializable; `forward()` rechecks that
incarnation atomically at the original mutation. Source resume, lane replacement,
route retirement and a one-second local receipt deadline fence already-drained
input too. A bound action route returns `Unsupported` from the legacy byte API
before consuming events; ordinary snapshot-only routes keep their existing API.

```rust
for input in bridge.take_overlay_routed_input()? {
    let accepted = input.forward(); // consumes its original receipt; never retarget
}
```

On the next projection reconciliation, changed incarnations retire only their old
physical surfaces, tracks, mappings and document grants before creating fresh outer
identities. Child windows follow their parent through this removal pass, including
a popup first created during a lane gap. Old frame targets and queued physical
addresses cannot name the replacement. The same-ID peer keeps its physical surface.
Recreated sources request full retained-frame replay through `take_full_frame_requests`;
service that API as usual before expecting fresh display/input admission. SDK lane
open/revocation advances projection revision so the local adapter reconciles it.

Lane loss or replacement promptly cancels queued and claimed focus/raise/lower
admissions. An old physical result or acknowledgement cannot restore their original
input grant. Center and close retain independent control authority and can finish
without an input lane. Lease suspension cancels every pending original host request;
authenticated resume retains logical IDs but adopts surfaces/tracks explicitly,
advances the media channel, and opens a newer input lane. Old lane cleanup cannot
close that replacement, and old channels cannot publish into it. A fresh explicit
action queries the current physical revision rather than replaying an old request.

The [local macOS recovery evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-resume-macos.json)
records two authenticated hops with the `NativeVectorHost` API, reused owner-local
IDs, queued/claimed action retirement, stale focus feedback, already-drained input
rejection, the real one-second receipt deadline, native frame/input invalidation,
retained-media replay and isolated cleanup. These are protocol/native
API recovery checks without GPU submission. Separate direct/action/relay Metal
fixtures exercise the final GUI integration with synthetic callbacks. Automatic
source lease recovery in a production GUI/mux frontend, combined GPU/display loss,
physical input/device behavior and other operating systems remain open gates.
