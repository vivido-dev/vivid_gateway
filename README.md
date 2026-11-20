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
inner.set_overlay_window_capacity(vivid_protocol::overlay::MAX_WINDOWS_PER_OWNER)?;
inner.enable_overlay_host_relay();
let mut bridge = OuterBridge::builder(authentication, display)
    .control_endpoint(outer_endpoint)
    .overlay_document_editing(&inner) // Arc<VirtualVivid>; lifetime is held weakly
    .build()?;
inner.set_overlay_document_host_profiles(&bridge.overlay_host_profiles());
```

When several inner owners map onto one outer session, bound aggregate inner
windows to that outer owner's 32-window protocol ceiling before authentication.
The setter rejects invalid reductions without eviction; ordinary updates and
close preserve the current owner lifecycle. Media-source budgets are separate.
The local compositor route installs this guard and qualifies 32 texture windows
from eight owners through reconnect and scoped cleanup.
[Native capacity evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-window-capacity-macos.json).

The local runtime separately qualifies 64 mixed sources (32 vector windows and
32 raw raster tracks) through two outer reconnects and exact-owner cleanup.
SDK connection admission accounts for the configured source count plus bounded
control/input/replacement headroom. Its media loops wait on a full event queue
without holding the presenter lock or losing the source; clean goodbye explicitly
retires retained image/raster sockets. The route still consumes bounded events
and original-generation completions, and GPU readback grants no presentation.
[Mixed-capacity evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-mixed-capacity-macos.json).

Native geometry acceptance also covers full 1024×1024 source canvases, odd target
strides, explicit 1.25/2× host rendering and exact original logical pointer reports
through three outer replacements. An independent same-address pane keeps its
pending frame throughout. These cases qualify explicit SDK host scales. The local
compositor also forwards GUI scale atomically with logical extent; its direct/relay
GUI checks pass simulated scale events at 2, 1.25, 1 and 2, fresh admission, logical
pointer/candidate coordinates, Unicode and stale callback rejection. Physical
monitor/DPI transitions and scanout remain unrun.
[GUI scale evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-gui-scale-macos.json).
The local compositor's eight-worker saturated-offer check also passes at GUI scale
2, with exact 130×570 logical and 260×1140 physical canvases before and after
recovery/replacement. Exact input, Unicode, ninth-worker refusal and retirement
remain correct; sampled source rates stay near the declared 60 records/s per
owner with four receipts. This does not qualify aggregate 64-source saturation,
larger concurrent canvases or GPU/display peak rates.
[Eight-owner scale evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-capacity-hidpi-macos.json).
[Geometry evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-geometry-macos.json).

The extended geometry check now passes thirteen cases per direct/relay mode up to
8192×8192 at scale 8, including downscaling and padded 8191-pixel rows. Four root
replacements include the largest case while original revisions, logical pointers
and an independent same-address pending peer remain correct. Large outputs use
bounded 4096-square Vello renders, one reusable scratch texture and ordered copies
into the original full target. Oversized GPU requests change no work or failure
state. The maximum-size GPU regression preserves same-ID peer pixels and receipts
after owner close. Physical monitor/device, dense-scene and other-platform gates
remain open.
[Large-target evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-large-target-macos.json).

The retained-image follow-up also passes three outer replacements at the maximum
target bound, including displayed/unshown released assets and a live asset. Nine
RGBA readbacks crossing both tile seams match an untiled reference within one
channel value and preserve same-ID owner isolation and presentation fences. It
exposed and corrected the SDK's double unpremultiplication of Vello 0.11 output;
registered textures and readback now consistently use straight alpha. Physical
display/device checks and broader workloads remain separate gates.
[Tiled asset evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-tiled-assets-macos.json).

The full 64-source mixed fixture also passes at the 8192-square target bound:
32 vector/bitmap windows and 32 raw-raster tracks from sixteen independent owners.
Two outer replacements preserve exact source keys and released/live retained state.
Closing one owner of each kind leaves 56 sources and 28 pending vector receipts
intact; retired frames cannot regain presentation authority. Source 65 is refused
nonfatally. This qualifies two bounded update waves and four-tile composition;
sustained aggregate saturation and denser content remain separate gates.
[Maximum mixed-target evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-mixed-tiled-macos.json).

The local native relay also passes mixed straight/premultiplied raster tracks at
1× and 8×. The SDK now retains the configured alpha mode through native preparation
and image-cache identity, fixing translucent premultiplied pixels that were
previously darkened. Four total root replacements preserve 64-source live/retained
state, peer receipts and source identities. SDK ingress rejects malformed
premultiplied full/delta pixels before retained-canvas mutation; raw and compressed
cases leave an unrelated same-ID peer usable. These are bounded local/native
checks; physical devices, dense workloads and sustained saturation remain gates.
[Raster alpha evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-raster-alpha-macos.json).

The local native relay also passes overlapping retained glyph/bitmap windows over
a premultiplied raster at the 8192-square bound. Six complete RGBA readbacks match
coordinate-aligned untiled references exactly, with independent alpha and glyph
color checks. Displayed and unshown released layout/image references survive two
root replacements. Exact-owner close preserves the pending peer and raster source;
released handles do not regain producer lookup authority. The fixture polls normal
font jobs throughout setup and reconnect. Translation-sensitive glyph AA, denser
content and physical platform/device behavior remain explicit separate gates.
A repeated native run exposed reversed overlap after reconnect: projection
exported surfaces in hash-map order. The SDK now exports ordinary surfaces in
deterministic identity order, followed by the authoritative pane window stack,
including hidden entries. The additive read-only protocol `Windows::stacked`
iterator exposes that existing order. Gateway reconstruction creates windows in
the supplied order, with parents established before children. The socket
regression covers same-ID owners, raise/lower, hidden windows, five outer
replacements and an unrelated physical peer. Five fresh native processes pass
thirty complete RGBA comparisons across ten root replacements after the fix.

[Overlapping composition evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-composition-macos.json).

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
ordinary opt-in `vector-gateway` route and `vector-relay-testing` fixtures exercise this core with
independent producer processes, actual macOS Metal rendering, GUI display receipts,
clipboard grants and synthetic native document/composition callbacks. Static hosts
can decline timed-media and audio-gain: both profiles are optional on the outer hop;
the gateway still requires core control, live media, observability and its target.
The static-host socket regression covers that negotiation without inventing audio
or timed capabilities.

The combined check now uses explicit producer-requested focus forwarding, without
a peer pointer-focus workaround. Frontends must explicitly bind that contract.
The local compositor now offers it with `vector-gateway`; production mux adoption
and broader frontend lease-recovery policies remain incomplete. Physical input
sources/candidate menus, other platforms/devices and static Metal builds remain
unrun. The [native core evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-relay-macos.json)
and [combined GUI evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-gateway-gui-macos.json)
record the initial boundaries; the [focus follow-up](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-focus-macos.json)
records the subsequent focus coverage. Production synchronized-media negotiation is unchanged.

Typed document routes now automatically bind original scene presentation. Build the
bridge before any inner producer authenticates. `overlay_presentation_forwarding`
can also opt in without documents; document/action/presentation origins must be the
same presenter. The SDK's exclusive `OverlayPresentationRoute` captures one-shot
`OverlayPresentationReceipt`s for exact accepted submissions. Original scenes and
ACKs advance only after matching physical outcomes; an accepted but undisplayed
repaint cannot revoke valid early Unicode input for the current scene. Cancellation
fails closed and never restores synthetic ACKs. Only unforwarded scenes coalesce;
already-forwarded scenes retain their own outcomes across newer source acceptance.
The SDK bounds copies and receipt metadata to four per owner and 64 globally,
and charges additional forwarded canvases against its encoded retained-byte budget.
Temporary receipt or aggregate-byte pressure returns SDK `WouldBlock` without
resolving the current original scene. Only that source's bounded media writer
retries; control and other sources keep progressing, and writer retirement cancels
the wait. No layout, revision or channel lock is held while waiting. Obsolete or
permanently oversized scenes still return `Discard`; receipt limits are unchanged.
Native 64-source streaming now covers three-minute 1×/8× runs, four total outer
replacements, unchanged original source keys, concurrent control queries and exact
final original presentation state without resubmitting the tail.
[Mixed streaming evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-mixed-stream-macos.json).

An integration that retains original receipts in another process can use
`media_chunk_with_presentation(chunk, correlation)` and drain
`take_presentation_reports()` independently of `take_media_completions()`.
Correlations are nonzero, unique for the original attachment and unchanged across
all chunks. Keep the opaque SDK original receipt at the owning server; validate
the reporting client, bridge/source incarnation and correlation before resolving
that exact object. Reports are metadata and confer no authority to retarget it.

Four reports per original producer and 64 globally bound live copies plus
undrained outcomes. Duplicate live correlations are refused. Capacity pressure
returns `WouldBlock` without charging a new slot; control and other owners remain
serviceable. Invalid partial assemblies, failed writes and exact writer retirement
report abandonment without inventing `Presented`. Cancellation abandons pending
reports and makes admission sticky-failed; late callbacks cannot revive them.
Draining a completed/abandoned report returns its slot. Write completions keep
their existing independent flow-credit semantics.

Real-socket/native-host tests keep two same-ID originals pending after completed
writes, retire only the selected owner's copy, preserve the peer's window/node
update and present its successor. A changed partial correlation abandons only its
own assembly while the peer still presents. This is the gateway primitive for
mux IPC and has now been adopted by the actual mux. Its server retains original
SDK grants, validates exact reporting client/bridge/source/copy metadata, and keeps
write ACKs independent. `can_accept_media_with_presentation` is a scheduling hint
that preserves owner isolation while allowing a reserved partial record to finish.
The actual mux's unchanged headless Metal receipt test now passes; the gateway
socket tests themselves make no GPU, AppKit or physical-device claim. Typed
native action/document mux routing and broader device/recovery gates remain open.
[Actual mux outcome evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-mux-original-outcomes-macos.json).
[External outcome API evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-external-outcomes-macos.json).

Actual mux qualification now also replaces its foreground client after graceful
detach and after abrupt fixture-client loss. Original receipts stay pending
through explicit detached holds and native replay, while original source/channel
identities, the hidden server and producer process remain stable. Stale image
callbacks cannot acknowledge the fresh image, and exact peer pixels follow window
moves before and after the other owner closes. This is two finite headless Metal
cases using the external report API, with no new gateway runtime change. Ordinary
At that checkpoint, AppKit mux windows, lease/input-lane recovery, document adoption,
sustained or arbitrary recovery/load and physical/platform/device gates remain open.
[Actual mux replacement evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-mux-replacement-macos.json).

For split-process input, configure `set_external_overlay_input_epochs` before
reconciling each corresponding projection and drain
`take_overlay_input_with_epochs`. The integration retains original SDK input
stamps on its server; the gateway receives only nonzero unique correlations,
bounded to 64 entries. Desired and physically bound incarnations remain distinct.
A changed correlation retires only its window role and queued old input before
rebinding; independent peers survive. Returned input is tagged with that bound
correlation, and untagged draining is refused. Invalid tables fail before mutation
and retired bridges refuse both configuration and draining. Input bytes are
omitted from Debug output. The server must validate reporting client/bridge and
correlation, then atomically forward through the retained original SDK epoch.

The actual macOS mux now passes direct-versus-mux canonical lane replacement:
old target pointer/key/text are rejected, peer old-target input continues, and
fresh input succeeds after explicit focus repaint admission. Same-ID generic
nodes and sources survive owner loss, with a real peer `UPDATE_NODE` and exact
native window pixels. The frozen pre-fix mux still fails the identical assertion.
This is finite synthetic input/Metal coverage; real AppKit mux windows, physical
input/devices, document and broader recovery/load remain open. Mux
pane lease creation returns `UNSUPPORTED_PROFILE` (3), so actual mux leased source
resume remains an adoption gap.
[Actual mux input-lane evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-mux-input-lane-macos.json).
Retained-scene qualification now also covers three focus changes and canonical
lane replacement directly and through the actual mux without producer repaint.
Original revision-one scenes/outcomes and channels remain unchanged while old
input is refused, peer old-target input progresses and fresh key/text arrives
once. Test-only physical source identities and ordered read-only focus readiness
observations distinguish current native targets from identical old pixels. A peer
commits a real generic `UPDATE_NODE` and native window move after owner loss.
No production gateway behavior changes in this follow-up. Ordinary AppKit mux,
documents, leased resume, broader load/recovery and physical/platform
device acceptance remain open.
[Retained mux focus evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-mux-static-focus-macos.json).

Split-process integrations can now call `start_external_overlay_action` after
configuring external input epochs. Copy the original SDK request's
`preceding_windows` and `centered_bounds` metadata from the original server;
retain its opaque `OverlayActionRequest` there. The gateway validates the bounded
same-owner barrier and exact target generation/revision, queries the independent
physical window, and yields `ExternalActionClaim` through
`take_external_overlay_action_claims`. Validate client, bridge incarnation and
request correlation at the original server, call that exact SDK receipt's
one-shot `claim`, and complete the exact gateway receipt with its decision.
Return the eventual host reply separately to the original SDK request. No opaque
SDK receipt or authority is serialized, and numeric metadata alone authorizes no
original action.

Claims share the existing 16-global/four-per-owner worker limits. The integration
must bound queued requests and preserve the original deadline; the worker's own
ten-second admission wait does not renew it. Dropping/denying a claim refuses the
RPC, expired/stale decisions cannot retarget another request, source-role removal
retires only that complete source key, and session replacement retires all old
claims before joining workers. Sticky cancellation interrupts admission waits
within 25 ms. Center keeps the original pane rectangle and checked projection
offset; its physical size is checked before claiming. The in-process typed path
uses the same physical dispatch implementation.

Six real-socket tests exercise all five actions, refusal without mutation,
owner saturation with peer action/control progress, physical replacement and
source loss with a subsequent same-ID peer `UPDATE_NODE`, plus malformed and
overtaken barriers. An unanswered ten-second claim expires without dispatch
while both input lanes renew and repeated peer node updates continue. Decision
handoff is in-process in these tests; actual mux claim/reply integration and
native external-action acceptance were still open at that API checkpoint.
No new native Metal, AppKit, physical/platform/device claim is made by this API
checkpoint. Existing local/native and actual-mux regressions pass independently.
[External action API evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-external-actions-macos.json).

The actual macOS mux now forwards producer-requested Focus, Raise, Lower,
Center and Close through VVMX 26. The hidden server installs the SDK action route
before producer authentication and retains every original opaque request. Only
bounded request/barrier metadata and Q32.32 Center bounds cross IPC. Claims and
outcomes must match the current presenter client, bridge incarnation and request
ID. Attachment replacement retires the original scope; generic host replies cannot
complete a typed action. The aggregate inner window limit is 32, fitting one outer
owner. Admission shares the gateway's 16-global/four-per-owner workers and keeps
control/input/media polling live.

`OverlayActionRequest::remaining_lifetime` observes the original deadline even
after its one-shot claim. The foreground subtracts the entire claim round trip
from the server's remaining lifetime before `ExternalActionClaim::complete_before`;
the worker rechecks that deadline before dispatch. A delayed positive decision
cannot start a fresh lifetime. The real SDK socket regression rejects wrong
client/bridge/request, duplicate claims/replies and retired attachments, and proves
a same-ID peer can still perform a real `UPDATE_NODE` after owner loss.

The direct-versus-actual-mux Metal probe verifies all five actions with exact
native pixels. Focus raises the requested window and receives once-only key/text;
Raise and Lower change overlap while preserving focus. Center uses the original
pane bounds: mux-local `(55,23,16,16)` becomes physical `(56,24,16,16)`, while the
direct host centers at `(504,504,16,16)`. Close removes only the requested role
before its producer transport closes. Both scenes remain revision one with single
original outcomes through these actions; the surviving same-ID owner's subsequent
node update and yellow repaint succeed on the same client/bridge. The final probe
fails at physical Focus on the frozen pre-adoption mux, with its runtime live and
no keys sent, and passes on the new mux.

A copied-executable control exposed a fixture error: its generic node commit
missed a target-generation announcement and was rejected with code 15. The fixture
now consumes authenticated target announcements and replans only an aborted
generic commit once, following SDK `PaneSession`; keys, typed actions, vector media
and successful commits are never replayed. The copied executable passes after
this correction. Redacted diagnostics retain only error kind and presenter code.

This qualifies finite headless Metal checks with synthetic display/key/text events.
Already-claimed typed actions under foreground replacement, mixed saturation and broader
recovery still need native acceptance. Document forwarding, generic/font-only
routes, mux lease issuance/resume (`UNSUPPORTED_PROFILE`, code 3), ordinary AppKit
mux windows, physical focus/IME/secure input/accessibility/scanout, display-scale
and device transitions, other platforms/GPUs/devices and broader sustained/fault
matrices remain open. Restart both mux server and client for private VVMX 26;
Vivid 1.5 wire/profile/registry and dependencies are unchanged.
[Mux window-action evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-mux-window-actions-macos.json).

A queued window action now also passes actual-mux foreground replacement.
Only the fixture-owned client is stopped with SIGSTOP. Public server diagnostics
prove one original Center is pending and unclaimed while its same-ID peer commits
a real node update. SIGKILL and explicit reattachment retire that request; it is
refused, both revision-one scenes replay without repaint, and the old Center does
not move the replacement window. A fresh Center then succeeds with the original
pane geometry. Source/channel identities and the hidden server/producer survive;
after owner loss the peer commits another real node update and native repaint.
The six mux probes and three-route Metal winding regression pass. This qualifies
that queued, unclaimed abrupt-loss case only. Already-claimed RPCs during
replacement, mixed saturation and ordinary AppKit/physical/platform/device gates
remain explicit. No additional feature work is included in this qualification.
[Queued action replacement evidence](../vivid_compositor/docs/performance/2026-11-15-sdk-vector-mux-pending-action-macos.json).



A native receipt-pressure fixture holds four original owner scenes unshown,
observes an actual `WouldBlock`, and displays an updated same-ID peer without
releasing the blocked owner's quota. It separately resumes the authenticated
source lease, replaces a track, and closes/recreates a window. Fresh exact-color
Metal readbacks settle only the corresponding original producer submissions;
retired images retain their pixels but cannot acknowledge the replacement, and
the outer root stays unchanged. The gateway pins capture attempts before its
bounded writer queue and layout work. SDK regressions additionally model channel
advance before cleanup and role recreation with an identical numeric submission;
those two models do not claim duplicate wire acceptance. Display admission is
synthetic. The post-change AppKit check still needs an unlocked session, and
physical input/device and other-platform gates remain unrun.
[Blocked capture replacement evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-pressure-resume-macos.json).

A dedicated authenticated channel-only follow-up now fills the owner's four
receipt slots with distinct window scenes, witnesses a fifth capture waiting,
and displays its same-ID peer while those four originals remain pending. It sends
`ADVANCE_CHANNEL` for just the target track, opens its replacement bulk channel
and rejects the old channel. Source keys, sessions, resume/input-lane generations,
surface identities, peer physical window and outer root stay unchanged. The old
original submissions resolve `Superseded`; only the fresh channel revision and
three unchanged auxiliary scenes reach `Presented`. Native Metal checks five
320×64 stages with exact color/gap probes and full byte equality of the retained
image before and after recovery. Both presenter worker counts reach zero on
teardown. This is native socket/GPU qualification with synthetic display admission;
AppKit, physical device and other-platform gates remain separate.
[Channel-only pressure evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-channel-pressure-macos.json).

The first overlay lane can now open after an already-authenticated gateway's target
has changed. Previously, pre-lane viewport updates remained queued and lane opening
appended the current revision again, causing the SDK's strict revision check to
close the lane. Unsent environment/viewport snapshots now coalesce per owner; lane
opening sends the current environment and viewport once, before the remaining
input and outcomes in their original order. Two-owner queue regressions preserve
same-ID input/outcome bytes through 1,000 changes and lane reopen. Real socket and
Metal checks service three target changes before opening the first lane, then
resize a live composition. Old input is rejected at dispatch; matching fresh
image admission delivers exact original pointer press/release reports without
reaching the peer. Original source/window/root identities stay unchanged, and
retired pixels remain immutable. Display admission is synthetic; physical window
and device gates remain separate.
[Late first-lane evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-late-lane-macos.json).

The pressure fixture now services every current composition while replacement
media primes. Its former all-five-drawings gate could withhold ready windows'
receipts; it also rejected a permitted retained track image before checking display
admission. The corrected loop accepts exact known priming states, requires clear
pixels wherever no drawing exists, and rejects regression after fresh admission.
Original outcomes are checked against the scenes actually admitted. A new visible
role deliberately delays its first media until the other three windows display;
the target stays unacknowledged until its own green frame arrives. Reinstating the
old gate reproduces a five-second, four-drawing stall with capture pressure.
One hundred corrected runs pass all four scenarios, including runs overlapping
the project CPU gates. No deadline or resource limit increased, and production
runtime code is unchanged. The original timeout and isolated retained-image
assertion failures remain in the evidence; the exact original timing was not
reproduced by the diagnostic series. Physical/platform gates remain separate.
[Pressure progress evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-pressure-progress-macos.json).

Accepted unresolved scenes and unsent outcomes now share a 256-receipt budget per
owner. Credits remain charged after records enter the socket writer and retire
only after write completion or cancellation. Deferred non-outcome transitions
retain a separate 256-record bound, with current environment/viewport snapshots
outside those counters. Overflow retires only the affected overlay lane and input;
key/button order and same-ID peer authority stay independent. Authenticated socket
and native Metal regressions cover fail-closed overflow, reserved receipts and
peer pixels. The native path retires the affected window while its same-ID peer
keeps its placement and continues displaying. Physical display and post-change ordinary/AppKit acceptance remain
separate gates. See the [bounded backlog evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-backlog-macos.json).

The developer-only `--vector-relay-curves-check` qualifies a finite curved-path
workload at 8192-square output: 315 commands per owner, 32 nested clips and 256
concentrated translucent fills. Five runs pass 30 full tiled/untiled comparisons
with zero observed difference through relay replacement and owner close. Separate
pixel and compiled-hit probes cover winding holes, reflected/sheared clips and
empty intersections; winding and shear-sign negative controls fail as intended.
The references share the compiled scene and independently exercise untiled
composition. Physical input/display and dynamic GPU arena exhaustion remain open.
See the [curved-path evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-curves-macos.json).

Before checked offscreen rendering, dynamic GPU arena qualification failed. A valid
accepted 512-fill scene (33,795 encoded bytes) returned a stale 1024-square image
as a successful readback while the device reported healthy. Three native
reproductions preserved the previous image byte for byte, including stale peer
pixels. Concurrent control queries completed, and closing the heavy owner restored
peer rendering on the same device.
A direct-Vello diagnostic confirms tile-arena exhaustion; the normal API does not
surface that allocation failure. Those failures are preserved as pre-fix evidence.
Source-isolated work admission remains required.
A separate immediate lease-resume test also failed before a later full-suite pass;
that failure is retained as pre-fix evidence for the retirement repair below. See the [failed arena qualification](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-arena-failure-macos.json).

The offscreen path now reads Vello's actual GPU allocation status before copying a
tile or publishing its cache key. Area-sensitive exhaustion subdivides aligned
tiles, with at most 64 dispatch attempts per output; irreducible allocation or work
refusal retires its partial target and returns an error without an image. An empty
composition after refusal produces fresh transparent output. The 128/512/1024/2048-fill native
regression now checks exact pixels, overlapping peer control, unchanged-image
reuse, the 64-attempt refusal, recovery on the same device, odd dimensions and
pending-validation cancellation/device loss. Readback still grants no display
authority. `vector-offscreen` enables Vello's diagnostic API; construct and poll
`Offscreen` on the render worker because its validation future is not `Send`.
An unlocked-Mac AppKit check also passes real hide/minimize/cover/restore and
minimized exact-owner close with live relayed sources, fresh key/Unicode input
and matching display admission. Native screenshots now require current image
admission after Unicode commit, including the resized surviving peer.

This repairs checked offscreen output, including the compositor worker. The
public synchronous raw-texture `Compositor::render`/`render_mixed` path still
checks submission only. Source-isolated handling of irreducible/work-limit
refusal and hostile overdraw through the two-hop relay remain open. The previous
immediate lease-resume rejection is repaired by retirement coordination below.
The earlier gateway suite
also observed `NotConnected` during outer TCP-loss replacement. A later deterministic
regression reproduces macOS repeated shutdown in the test carrier: it aborted before
closing later live sockets. The fixture now accepts only that already-closed result,
preserves other errors, closes remaining sockets and leaves an independent carrier
and replacement connections usable. Fifty final-source runs pass 150 outer replacements,
and four explicit native Metal relay recovery cases pass. This is a test-carrier
repair that did not alter production transport or resume behavior. Physical GPU
faults, scanout, monitor transitions, hardware input/IME and other platforms/devices are
unrun. See the [checked offscreen evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-checked-macos.json).
See the [outer-shutdown fixture evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-outer-shutdown-macos.json).

Immediate resume previously raced the presenter's old control retirement: a
controlled test holds witnessed EOF before suspension and reproduces the same
fatal lease-resume rejection. Before establishment, a valid proof for the exact
active lease/session/generation may now wait for retirement within the original
absolute three-second handshake deadline. The condition wait releases the global
state lock and does not cancel or replace a live owner. Invalid proofs do not park;
competing requests cannot both advance the generation. Normal establishment still
requires suspended state, unexpired grace, matching profiles and valid proof.
Four regressions cover retirement, same-ID peer control/track preservation, invalid
proof, healthy-control timeout and competing attempts. Thirty original high-level
window-adoption runs and thirty witnessed-EOF runs pass; native GPU, direct/relay
lifecycle and real AppKit checks also pass. No wire, lease state machine, key derivation,
producer retry budget or connection limit change is made. Physical network faults
and other native platforms/devices remain separate gates.
See the [resume-retirement evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-resume-retirement-macos.json).

Checked offscreen output now uses a private pinned Vello 0.11.0 backend with
cancellation-safe map callbacks and typed coarse mapped-range errors. It shares
upstream scene identity, so applications keep their original `vello::Scene`.
The explicit native test drops pending coarse validation or debug downloads while
an upstream Vello peer on the same device preserves exact pixels and produces a
fresh image. Health-guard retirement after logical device loss also passes.
Offscreen destruction no longer drains GPU work. `compositor::Error::Backend` is
an added opaque error variant; downstream exhaustive matches must account for it.

The original stock failure and isolated patch results remain preserved. Vivido's
raw consumer still needs immutable completed texture exports, bounded work and
receipts tied to the image actually rendered, without blocking terminal/peer
progress or destroying their shared device. The adopted checked backend does not
complete that migration, qualify physical GPU faults, or claim package publication.
See the [original cancellation evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-shared-cancellation-macos.json)
and [adopted backend evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-backend-macos.json).


`DeliveryOutcome::superseded` identifies an intentionally coalesced delivery: release
its ingress with `VirtualVivid::release_bridge_delivery`, without manufacturing
presentation or source loss. Outer-attachment replacement explicitly abandons its
copies after releasing the mapping lock. Retained replay does not manufacture
another original ACK. Outer vector revisions are allocated per
window, including across track replacement. Old channel-bound document receipts
reject immediately even before downstream reconciliation.

Additional native socket regressions cover deferred-presentation lease resume,
track replacement and two owners reusing local IDs. Controlled carrier delivery
stalls over real TCP isolate control reads and track writes, fill the bounded media
queue, preserve control and peer progress, and verify cancellation. These are not
native kernel-buffer exhaustion or real network-blackhole qualification. The local
macOS frontend passes logical GPU retry and a five-minute paced four-owner workload;
broader frontend recovery, production mux adoption, peak
load, physical input and other platforms/devices remain explicit gates. The ordinary GUI sample builds,
and the unlocked macOS sample passes ordinary startup, exact-owner focus and
close-policy cleanup through local control. Native screenshot inspection passes
in the [window-capture evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-window-macos.json). See the
[local qualification evidence](../vivid_compositor/docs/performance/2026-11-12-sdk-vector-local-macos.json).

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
source lease recovery beyond the default compositor static producer, physical GPU/display loss,
physical input/device behavior and other operating systems remain open gates.

The compositor's display-loss banner now overlays its retained app, preserving
the viewport through retry. Eight direct/relay Metal runs admit the first fresh
key without waiting for editor republication and continue rejecting stale input.
The [retry and native TCP follow-up](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-retry-tcp-macos.json)
also covers native kernel write backpressure: a loopback peer stops reading, real
socket buffers fill, and a native write remains blocked for at least 100 ms.
Same-owner control and a same-ID peer keep progressing, the existing 32-record
media handoff stays bounded, and cancellation retires the writer within two seconds.
The later fixture below validates live socket capacity and a sustained native
write independently of its initial buffer request and protocol credit. No writer
condition variable or fabricated I/O error creates this native stall. Packet-loss
blackholes and physical/correlated device faults remain unrun; controlled
connection cancellation with logical GPU loss is covered below.

Multiple in-flight source revisions now have native socket/host API regressions:
a forwarded first scene remains displayable and editable after a second is
accepted, repeated attachment replacement preserves replay, and an obsolete
unforwarded delivery reaches neither the physical host nor source-loss recovery.
These fixtures do not submit GPU work. A follow-up now fills all four source
forwarding slots before channel replacement, refills them before lease resume,
and displays exact source revision 12 afterward. The SDK retires old original
scene charges; the gateway retires copies by physical writer without resetting
an existing window's revision namespace. A writer-specific fence under the mapping
lock rejects late receipt insertion, and receipt abandonment runs outside that
lock. A same-ID peer retains its pending outcome, and old prepared frames remain
invalid. This socket/native-host API regression does not submit GPU work or
qualify automatic frontend recovery. Bounded Metal streaming is covered below;
higher-rate/peak workloads remain an explicit gate.
See the [source retirement follow-up](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-generation-macos.json).

An explicit GPU variant now renders the source scenes before and after channel
advance and authenticated lease resume. Red, green and yellow source readbacks
retain the same-ID peer's blue pixels. Old immutable frames remain renderable,
but cannot acknowledge retired source authority. Readback alone leaves both
source presentation revisions at zero; admitting the exact fresh frame advances
the resumed source to revision 12 and the peer to its original revision 1.
This uses native offscreen rendering and simulated display admission, without
adding automatic frontend recovery. See the
[GPU recovery evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-recovery-gpu-macos.json).
Run this adapter-dependent check explicitly:

```sh
cargo test --test overlay_smoke --features native-gpu-testing native_gpu_fences_retired_source_frames_and_preserves_same_id_peer -- --ignored --nocapture
```

A further native readback check closes the current outer TCP connections and
explicitly replaces that root session three times. Host-owned retained replay
restores both inner owners without another source submission. Fresh outer owners
reuse the same numeric window IDs; old immutable frames still render but cannot
acknowledge display, and old native/drained input is rejected. Inner session,
resume and channel identities stay unchanged. An independent physical same-ID
peer keeps its original unshown blue frame and admits it after all replacements.
Readback alone never advances source presentation. This qualifies explicit
`replace_session` integration. The subsequent compositor integration below adds
one bounded automatic policy; packet-loss recovery remains unqualified. See the
[outer replacement evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-outer-replacement-macos.json).

```sh
cargo test --test overlay_smoke --features native-gpu-testing native_gpu_replays_retained_scenes_after_outer_tcp_loss -- --ignored --nocapture
```

Native physical hosts now reserve exact source scenes during preparation, so a
successor accepted during compilation, GPU work or GUI delivery does not retire
the earlier image's physical outcome. Newer actual display, source/placement or
host lifecycle changes still reject obsolete receipts. Dropped unshown native
reservations settle during normal host polling. These reservations reuse encoded
scene charges and preserve cached peers under resource pressure. This extends the
SDK's terminating-source receipt accounting through the physical native host.

The current TCP saturation fixture also bounds MSS on both connected macOS
sockets. Reapplying a 16-KiB receive-buffer request alone did not prevent later
storage growth, and a 100-ms write pause could finish before peer checks. The
fixture now waits for the initial outer scene, checks the live sender/receiver
capacity and pins the same write call and completed-byte count for at least
500 ms and through owner control plus a receiver-applied same-ID peer update. The original scene, queue/credit
limits and two-second cancellation bound remain unchanged. Fifty final runs pass
under overlapping project/native GPU work; a control removing only MSS fails
twice. This changes only the fixture and test-only `socket2` feature selection,
not production transport or system-wide network settings. IPv4 loopback on this
Mac is qualified; other network/platform/device gates remain open.
[Sustained native TCP evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-tcp-backpressure-macos.json).

Two bounded multi-submission Metal workloads passed the full key/Unicode/recovery
and owner-replacement script; another repeat timed out on focus/editor readiness.
The compositor subsequently fixed native candidate queries that read an older
GUI caret copy instead of the current worker snapshot. A 43.2-minute follow-up
passes 18,433 exact keys and 144 Unicode compositions, GPU retry, source replacement
and stale callbacks. This qualifies the declared four-receipt, 100-ms offered
batch policy, not peak or higher-rate throughput. The historical timeout remains
recorded without retrospective sole-cause attribution. See the
[historical streaming evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-stream-macos.json)
and the [long soak](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-soak-macos.json).

A further 18.1-minute four-owner run offers batches every 10 ms while preserving
the SDK helper's 60-record/s declaration and pacing. Rendered checkpoint deltas
show approximately 60 source scenes/s per owner; 7,681 exact keys and 60 Unicode
compositions pass through GPU retry and source replacement. This qualifies that
declared ceiling, not GPU/display FPS or other track rates, canvas sizes and owner
counts. See the [higher-offer evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-fast-macos.json).

Four further native Metal runs exercise actual producer termination and relay
connection cancellation after verified logical GPU failure, using bounded
streaming producers. The peer retains its process and state; explicit Restart
gets a fresh owner and producer, rejects retired callbacks and admits fresh
Unicode input. This controlled connection-loss case does not qualify packet-loss
blackholes, physical GPU resets, production mux adoption or automatic frontend
lease recovery by that test. The later compositor static producer checks exercise
automatic inner lease resume through direct and relayed Metal; broader recovery
policies remain unqualified. See the [combined-failure evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-combined-macos.json).
The [automatic source recovery evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-auto-resume-macos.json)
records six local resumes, peer isolation and the ordinary leased-launch sample.

Further direct/relayed lifecycle checks hold a recovered source host-hidden for
eleven seconds, repeat recovery with logical GPU loss and explicit hidden renderer
retry, then close a third resumed owner before display admission. The peer stays
live and old callbacks remain rejected. This qualifies controlled source control
loss and host visibility callbacks. Actual AppKit visibility is qualified by the
native-window follow-up; physical GPU faults and packet loss remain unrun.
See the [hidden recovery evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-hidden-resume-macos.json).

The native-window follow-up passes two real AppKit runs with streaming relay
sources. Hide, minimize and full opaque-window occlusion each revoke image/text
admission for six seconds while displayed streams freeze. Restore admits fresh
images and synthetic key/text reports on unchanged processes and relay roots.
An exact-owner close while minimized retires only that source before peer display;
the peer retains its process/root through resize and fresh Unicode. Linked state
and worker retirement pass. Physical input/IME menus, secure input, accessibility,
monitor/device faults and scanout remain unrun.
[Native AppKit evidence](../vivid_compositor/docs/performance/2026-11-14-sdk-vector-native-window-macos.json).

The local compositor adapter now automatically replaces failed outer transports
for retained static sources with fresh root authentication. Unretained audio/video
keep normal live forwarding and terminal transport loss; static cuts leave their
bootstrap packets intact. Its atomic SDK retained cut releases selected
superseded deliveries without display/source-loss claims and fences replay to
the original source incarnation. `DeliveryOutcome::source` and `writer_id`,
matched with `media_writer_id`, let callers retire bounded handoffs even when
new physical owners reuse outer numeric IDs. These are gateway-local identities,
never protocol generations. `outer_session_id` identifies the physical owner
whose fresh display admission may reset the adapter's retry budget.
Six local Metal replacements cover vector/PNG/JPEG/raster, unchanged original
processes/inner identities, stale callbacks and fresh input. A hidden eleven-second
hold plus second loss verifies that authentication alone cannot renew retry
admission. Sticky cancellation remains terminal; this policy does not retry
uncertain actions. See the
[automatic outer recovery evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-outer-auto-macos.json).

The subsequent compositor asset fixture runs the same automatic runtime with two
independent authenticated TCP producer sessions in-process. Reused window/track/
asset IDs stay owner-scoped across three outer root replacements, including
asset/frame/release replay for displayed and still-unshown scenes. Exact native
readbacks preserve the source colors and blue peer; only fresh host admission
settles pending outcomes. The ordinary leased sample independently passes on-screen
capture/focus/close without testing features. See the
[asset and visible-window evidence](../vivid_compositor/docs/performance/2026-11-13-sdk-vector-assets-macos.json).
