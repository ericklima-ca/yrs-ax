# Yrs-Ax: WebSocket Connections for Yrs

This library is an extension over [Yjs](https://yjs.dev)/[Yrs](https://github.com/y-crdt/y-crdt) Conflict-Free Replicated Data Types (CRDT) message exchange protocol. It provides utilities to connect with Yjs web socket providers using Rust's [axum](https://github.com/tokio-rs/axum) web framework.

> **Note:** This is an [axum](https://github.com/tokio-rs/axum) port of [yrs-warp](https://github.com/y-crdt/yrs-warp). If you're using warp, please use the original yrs-warp library instead.

## Features

- **Y-WebSocket Protocol**: Full support for y-websocket provider protocol
- **Y-WebRTC Signaling**: Built-in signaling server for y-webrtc connections
- **Broadcast Groups**: Efficient document synchronization across multiple clients
- **Custom Protocol Extensions**: Extend the protocol with your own message handlers
- **Axum Integration**: Native axum websocket support with modern async/await patterns

## Demo

A working demo can be seen under [examples](./examples) subfolder. It integrates this library with CodeMirror 6, providing collaborative rich text document editing capabilities.

To run the demo:

```bash
# Build the frontend
cd examples/frontend
bun install
bun run build

# Run the server
cd ../..
cargo run --example main
```

Then open `http://localhost:8000` in multiple browser windows to see real-time collaboration.

## Example: Broadcast Group

To gossip updates between different WebSocket connections from clients collaborating over the same logical document, use a broadcast group:

```rust
use std::sync::Arc;
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use tokio::sync::Mutex;
use yrs::Doc;
use yrs_ax::broadcast::BroadcastGroup;
use yrs_ax::ws::{AxumSink, AxumStream};

#[tokio::main]
async fn main() {
    // We're using a single static document shared among all the peers.
    let awareness = Arc::new(yrs::sync::Awareness::new(Doc::new()));

    // Open a broadcast group that listens to awareness and document updates
    // with a pending message buffer of up to 32 updates
    let bcast = Arc::new(BroadcastGroup::new(awareness, 32).await);

    let app = Router::new()
        .route(
            "/my-room",
            get({
                let bcast = bcast.clone();
                move |ws: WebSocketUpgrade| ws_handler(ws, bcast.clone())
            }),
        );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, bcast: Arc<BroadcastGroup>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| peer(socket, bcast))
}

async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>) {
    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);
    let sub = bcast.subscribe(sink, stream);
    
    match sub.completed().await {
        Ok(_) => println!("broadcasting for channel finished successfully"),
        Err(e) => eprintln!("broadcasting for channel finished abruptly: {}", e),
    }
}
```

## Custom Protocol Extensions

[y-sync](https://crates.io/crates/y-sync) protocol enables extensions to its own protocol, and yrs-ax supports this as well. You can implement your own protocol by implementing the `Protocol` trait:

```rust
use y_sync::sync::{Protocol, Message};
use yrs::sync::{Awareness, Error};

struct EchoProtocol;

impl Protocol for EchoProtocol {
    fn missing_handle(
        &self,
        awareness: &mut Awareness,
        tag: u8,
        data: Vec<u8>,
    ) -> Result<Option<Message>, Error> {
        // All messages prefixed with tags unknown to y-sync protocol
        // will be echoed back to the sender
        Ok(Some(Message::Custom(tag, data)))
    }
}

async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>) {
    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);
    
    // Subscribe with custom protocol parameter
    let sub = bcast.subscribe_with(sink, stream, EchoProtocol);
    // ... rest of the code
}
```

## Y-WebRTC and Signaling Service

In addition to performing its role as a [y-websocket](https://docs.yjs.dev/ecosystem/connection-provider/y-websocket) server, `yrs-ax` also provides a signaling server implementation used by [y-webrtc](https://github.com/yjs/y-webrtc) clients to exchange information necessary to connect WebRTC peers together and make them subscribe/unsubscribe from specific rooms.

```rust
use axum::{
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Router,
};
use yrs_ax::signaling::{SignalingService, signaling_conn};

#[tokio::main]
async fn main() {
    let signaling = SignalingService::new();

    let app = Router::new()
        .route(
            "/signaling",
            get({
                let signaling = signaling.clone();
                move |ws: WebSocketUpgrade| ws_handler(ws, signaling.clone())
            }),
        );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, svc: SignalingService) -> impl IntoResponse {
    ws.on_upgrade(move |socket| peer(socket, svc))
}

async fn peer(ws: WebSocket, svc: SignalingService) {
    match signaling_conn(ws, svc).await {
        Ok(_) => println!("signaling connection stopped"),
        Err(e) => eprintln!("signaling connection failed: {}", e),
    }
}
```

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
yrs-ax = "0.0.1"
axum = { version = "0.8", features = ["ws"] }
tokio = { version = "1", features = ["full"] }
yrs = "0.25"
```

## Related Projects

- **[yrs-warp](https://github.com/y-crdt/yrs-warp)** - Original warp-based implementation
- **[Yrs](https://github.com/y-crdt/y-crdt)** - Rust port of Yjs CRDT library
- **[Yjs](https://github.com/yjs/yjs)** - JavaScript CRDT framework

## License

MIT License - see [LICENSE](LICENSE) file for details.
