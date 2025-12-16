use axum::{
    Router,
    extract::ws::{WebSocket, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
};
use std::net::SocketAddr;
use tower_http::services::{ServeDir, ServeFile};
use yrs_ax::signaling::{SignalingService, signaling_conn};

const STATIC_FILES_DIR: &str = "frontend/dist";

#[tokio::main]
async fn main() {
    let signaling = SignalingService::new();

    let static_dir = ServeDir::new(STATIC_FILES_DIR); // Serves files from the 'assets' folder

    // Define a fallback for when ServeDir itself can't find a file (e.g., for SPA routing)
    let fallback_file = ServeFile::new("frontend/dist/index.html");

    let app = Router::new()
        .route(
            "/signaling",
            get({
                let signaling = signaling.clone();
                move |ws: WebSocketUpgrade| ws_handler(ws, signaling.clone())
            }),
        )
        .fallback_service(static_dir.not_found_service(fallback_file));

    let addr: SocketAddr = ([0, 0, 0, 0], 8000).into();
    println!("serving frontend from {STATIC_FILES_DIR} at http://{addr}");

    if let Err(e) = axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app).await {
        eprintln!("server error: {e}");
    }
}

async fn ws_handler(ws: WebSocketUpgrade, svc: SignalingService) -> impl IntoResponse {
    ws.on_upgrade(move |socket| peer(socket, svc))
}

async fn peer(ws: WebSocket, svc: SignalingService) {
    println!("new incoming signaling connection");
    match signaling_conn(ws, svc).await {
        Ok(_) => println!("signaling connection stopped"),
        Err(e) => eprintln!("signaling connection failed: {e}"),
    }
}
