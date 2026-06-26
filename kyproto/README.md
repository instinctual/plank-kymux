# KyProto

KyProto is a library to transport video, audio and input packets over QUIC and
WebTransport (via [kynet]), with different protocol implementations.

Currently, 3 protocols are implemented for video streams:
 - `reliable`: a single QUIC/WebTransport stream is used to transmit all packets
   in sequence
 - `gopstream`: one stream is used per [GoP]; this allows to discard a previous
   GoP if a new one is available
 - `unreliable_feq`: packets are sent over datagram, with some redundancy added
   by Forward Error Correction ([FEC]); this allows to recover from some packet
   loss without adding latency

Audio and input packets are only transmitted over a reliable stream for now.

[kynet]: ../kynet/README.md
[GoP]: https://en.wikipedia.org/wiki/Group_of_pictures
[FEC]: https://en.wikipedia.org/wiki/Error_correction_code

Cargo features forward the 3 kynet features, depending on the underlying network
transport protocol:
 - `kynet-quinn`
 - `kynet-webtransport-js`
 - `kynet-wtransport`


## Build

To build the project locally:

```bash
# quinn (non-wasm-only)
cargo build --features=kynet-quinn

# webtransport-js (wasm-only)
export RUSTFLAGS=--cfg=web_sys_unstable_apis
cargo build --features=kynet-webtransport-js --target=wasm32-unknown-unknown

# wtransport (non-wasm-only)
cargo build --features=kynet-wtransport
```

To use _kyproto_ as a dependency (adapt the features), add to your `Cargo.toml`:

```toml
[dependencies]
kynet = { version = "0.1", path = "../kynet", features = ["kynet-webtransport-js"] }
kyproto = { version = "0.1", path = "../kyproto" }
```

## Use

Here is a sample to send a video stream (using _wtransport_):

```rust
    // wtransport is an established connection with the wtransport library
    let wtransport = …;

    // Wrap it into a kynet connection
    let conn = kynet::Connection::from(wtransport);

    // Create a kyproto instance with that connection "as a server" (the
    // direction is used for opening the "control" stream)
    let kyproto = KyProto::accept(conn).await?;

    // Register as a video producer with endpoint 0, using the reliable protocol
    let endpoint = kyproto
        .register_video_endpoint(0, VideoProtocol::Reliable)
        .await?;

    // Await until the client is ready. This returns a ProtocolSend instance
    // (with a "reliable" driver internally because that's what we asked)
    let mut protocol = endpoint.ready().await?;

    loop {
        // Send packets to the client
        let packet = …;
        protocol.send(packet).await?;
    }
```

And to receive it from a webclient:

```rust
    // web_transport is an established connection with the native JavaScript
    // WebTransport API
    let web_transport = …;

    // Wrap it into a kynet connection
    let conn = kynet::Connection::from(web_transport);

    // Create a kyproto instance with that connection "as a client"
    let kyproto = KyProto::connect(conn).await?;

    // Register as a video consumer for endpoint 0, using the reliable protocol
    let endpoint = kyproto.connect_video_endpoint(0, VideoProtocol::Reliable)?;

    // We're ready, request the server to start
    let mut protocol = endpoint.ready().await?;

    // Recv the packets from the server
    while let Some(packet) = protocol.recv().await? {
        // decode and display packet
    }
```


## History

The features exposed by _kyproto_ were initially implemented in _kymux_ (hence
the name, packets were "muxed" into QUIC streams to be transmitted to the other
side). But in _kymux_, packets were necessarily provided over a local IPC (TCP)
connection.

In order to support WebTransport in the browser, we wanted to be able to send
and receive packet structures programmatically, to feed the decoder and consume
the packets directly.

Therefore, the API and implementation have been adapted from _kymux_ to expose
"packets", and the IPC/TCP part has been removed. This project is intended to be
the evolution of _kymux_, but for practical reasons, it has been created as a
separate project (the code from _kymux_ has been copied and adapted instead).
That's why it is named _kyproto_ rather than _kymux_.

The IPC/TCP feature has been implemented in a separate component named [kycom].

[kycom]: ../kycom/README.md
