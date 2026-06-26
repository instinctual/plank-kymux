# KyCom

KyCom is an IPC component to forward packets between a local IPC (TCP)
connection and a [kyproto] endpoint.

Contrary to [kynet] and [kyproto], this project does not support WASM. In the
browser, packets may not be sent to a separate process, so they are handled
directly via the kyproto API.

[kynet]: ../kynet/README.md
[kyproto]: ../kyproto/README.md

Cargo features forward the kynet features, depending on the underlying network
transport protocol:
 - `kynet-quinn`
 - `kynet-wtransport`
 - `kynet-webtransport-js` (only supports WASM)


## Build

```bash
# quinn
cargo build --features=kynet-quinn

# wtransport
cargo build --features=kynet-wtransport
```

To use _kycom_ as a dependency (adapt the features), add to your `Cargo.toml`:

```toml
kycom = { version = "0.1", path = "../kyproto/kycom", features = ["kynet-wtransport"] }
kynet = { version = "0.1", path = "../kyproto/kynet", features = ["kynet-wtransport"] }
kyproto = { version = "0.1", path = "../kyproto/kyproto", features = ["kynet-wtransport"] }
```

## Use

Firstly, a `KyCom` instance must be started listening on a port (one instance
for the whole process is sufficient):

```rust
let kycom = KyCom::start_on_port(9090).await?;
```

Then, for each kyproto endpoint to forward:

```rust
let endpoint = …; // get an endpoint instance from kyproto

// Register the kyproto endpoint
let forwarder = kycom.register(endpoint)?;

// Retrieve the URL if necessary
let url = forwarder.addr().url();

// Start forwarding
forwarder.forward().await?;
```

Alternatively, the client might decide to forward from a separate task instead:

```rust
let task = tokio::spawn(async move {
    forwarder.forward().await.unwrap();
});
```

## History

Initially, _kymux_ was doing what _kyproto_ does now, but with packets always
passed over local IPC/TCP.

In order to add support for WASM in a browser, _kymux_ has been split into two
parts:
 - `kyproto` handles endpoints and transmit packets using custom protocols
 - `kycom` exposes packets locally to a separate process (or not) over local
   IPC/TCP

All platforms use _kyproto_. In addition, non-wasm platforms use _kycom_ to
forward packets over a local IPC/TCP connection.
