# vivid_gateway

`vivid_gateway` is the Vivid 1.5 terminating-gateway core used by `vvmux`.
It terminates an authenticated inner producer session as a presenter and
re-originates validated surfaces, tracks, scene nodes, and media into an independent outer producer
session.

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
