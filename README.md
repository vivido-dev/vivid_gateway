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

Run the standalone gates from this directory:

```text
cargo fmt --all --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```
