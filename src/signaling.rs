use axum::Error;
use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;

const PING_TIMEOUT: Duration = Duration::from_secs(30);

/// Signaling service is used by y-webrtc protocol in order to exchange WebRTC offerings between
/// clients subscribing to particular rooms.
///
/// # Example
///
/// ```rust
/// use axum::{
///     extract::ws::{WebSocket, WebSocketUpgrade},
///     response::IntoResponse,
///     routing::get,
///     Router,
/// };
/// use yrs_ax::signaling::{SignalingService, signaling_conn};
///
/// #[tokio::main]
/// async fn main() {
///     let signaling = SignalingService::new();
///     let app: Router = Router::new()
///         .route(
///             "/signaling",
///             get({
///                 let signaling = signaling.clone();
///                 move |ws: WebSocketUpgrade| ws_handler(ws, signaling.clone())
///             }),
///         );
///     // axum::serve(tokio::net::TcpListener::bind("0.0.0.0:8000").await.unwrap(), app).await.unwrap();
/// }
///
/// async fn ws_handler(ws: WebSocketUpgrade, svc: SignalingService) -> impl IntoResponse {
///     ws.on_upgrade(move |socket| peer(socket, svc))
/// }
///
/// async fn peer(ws: WebSocket, svc: SignalingService) {
///     match signaling_conn(ws, svc).await {
///         Ok(_) => println!("signaling connection stopped"),
///         Err(e) => eprintln!("signaling connection failed: {}", e),
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SignalingService(Topics);

impl SignalingService {
    pub fn new() -> Self {
        SignalingService(Arc::new(RwLock::new(Default::default())))
    }

    pub async fn publish(&self, topic: &str, msg: Message) -> Result<(), Error> {
        let mut failed = Vec::new();
        {
            let topics = self.0.read().await;
            if let Some(subs) = topics.get(topic) {
                let client_count = subs.len();
                tracing::info!("publishing message to {client_count} clients: {msg:?}");
                for sub in subs {
                    if let Err(e) = sub.try_send(msg.clone()).await {
                        tracing::info!("failed to send {msg:?}: {e}");
                        failed.push(sub.clone());
                    }
                }
            }
        }
        if !failed.is_empty() {
            let mut topics = self.0.write().await;
            if let Some(subs) = topics.get_mut(topic) {
                for f in failed {
                    subs.remove(&f);
                }
            }
        }
        Ok(())
    }

    pub async fn close_topic(&self, topic: &str) -> Result<(), Error> {
        let mut topics = self.0.write().await;
        if let Some(subs) = topics.remove(topic) {
            for sub in subs {
                if let Err(e) = sub.close().await {
                    tracing::warn!("failed to close connection on topic '{topic}': {e}");
                }
            }
        }
        Ok(())
    }

    pub async fn close(self) -> Result<(), Error> {
        let mut topics = self.0.write_owned().await;
        let mut all_conns = HashSet::new();
        for (_, subs) in topics.drain() {
            for sub in subs {
                all_conns.insert(sub);
            }
        }

        for conn in all_conns {
            if let Err(e) = conn.close().await {
                tracing::warn!("failed to close connection: {e}");
            }
        }

        Ok(())
    }
}

impl Default for SignalingService {
    fn default() -> Self {
        Self::new()
    }
}

type Topics = Arc<RwLock<HashMap<Arc<str>, HashSet<WsSink>>>>;

#[derive(Debug, Clone)]
struct WsSink(Arc<Mutex<SplitSink<WebSocket, Message>>>);

impl WsSink {
    fn new(sink: SplitSink<WebSocket, Message>) -> Self {
        WsSink(Arc::new(Mutex::new(sink)))
    }

    async fn try_send(&self, msg: Message) -> Result<(), Error> {
        let mut sink = self.0.lock().await;
        if let Err(e) = sink.send(msg).await {
            sink.close().await?;
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let mut sink = self.0.lock().await;
        sink.close().await
    }
}

impl Hash for WsSink {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = Arc::as_ptr(&self.0) as usize;
        ptr.hash(state);
    }
}

impl PartialEq<Self> for WsSink {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for WsSink {}

/// Handle incoming signaling connection - it's a websocket connection used by y-webrtc protocol
/// to exchange offering metadata between y-webrtc peers. It also manages topic/room access.
pub async fn signaling_conn(ws: WebSocket, service: SignalingService) -> Result<(), Error> {
    let mut topics: Topics = service.0;
    let (sink, mut stream) = ws.split();
    let ws = WsSink::new(sink);
    let mut ping_interval = interval(PING_TIMEOUT);
    let mut state = ConnState::default();
    loop {
        select! {
            _ = ping_interval.tick() => {
                if !state.pong_received {
                    ws.close().await?;
                    drop(ping_interval);
                    return Ok(());
                } else {
                    state.pong_received = false;
                    if let Err(e) = ws.try_send(Message::Ping(bytes::Bytes::from_static(b""))).await {
                        ws.close().await?;
                        return Err(e);
                    }
                }
            },
            res = stream.next() => {
                match res {
                    None => {
                        ws.close().await?;
                        return Ok(());
                    },
                    Some(Err(e)) => {
                        ws.close().await?;
                        return Err(e);
                    },
                    Some(Ok(msg)) => {
                        process_msg(msg, &ws, &mut state, &mut topics).await?;
                    }
                }
            }
        }
    }
}

async fn process_msg(
    msg: Message,
    ws: &WsSink,
    state: &mut ConnState,
    topics: &mut Topics,
) -> Result<(), Error> {
    match msg {
        Message::Text(json) => {
            let msg = serde_json::from_str(&json).unwrap();
            match msg {
                Signal::Subscribe {
                    topics: topic_names,
                } => {
                    if !topic_names.is_empty() {
                        let mut topics = topics.write().await;
                        for topic in topic_names {
                            tracing::trace!("subscribing new client to '{topic}'");
                            if let Some((key, _)) = topics.get_key_value(topic) {
                                state.subscribed_topics.insert(key.clone());
                                let subs = topics.get_mut(topic).unwrap();
                                subs.insert(ws.clone());
                            } else {
                                let topic: Arc<str> = topic.into();
                                state.subscribed_topics.insert(topic.clone());
                                let mut subs = HashSet::new();
                                subs.insert(ws.clone());
                                topics.insert(topic, subs);
                            };
                        }
                    }
                }
                Signal::Unsubscribe {
                    topics: topic_names,
                } => {
                    if !topic_names.is_empty() {
                        let mut topics = topics.write().await;
                        for topic in topic_names {
                            if let Some(subs) = topics.get_mut(topic) {
                                tracing::trace!("unsubscribing client from '{topic}'");
                                subs.remove(ws);
                            }
                        }
                    }
                }
                Signal::Publish { topic } => {
                    let mut failed = Vec::new();
                    {
                        let topics = topics.read().await;
                        if let Some(receivers) = topics.get(topic) {
                            let client_count = receivers.len();
                            tracing::trace!(
                                "publishing on {client_count} clients at '{topic}': {json}"
                            );
                            for receiver in receivers.iter() {
                                if let Err(e) = receiver.try_send(Message::text(json.clone())).await
                                {
                                    tracing::info!(
                                        "failed to publish message {json} on '{topic}': {e}"
                                    );
                                    failed.push(receiver.clone());
                                }
                            }
                        }
                    }
                    if !failed.is_empty() {
                        let mut topics = topics.write().await;
                        if let Some(receivers) = topics.get_mut(topic) {
                            for f in failed {
                                receivers.remove(&f);
                            }
                        }
                    }
                }
            }
        }
        Message::Close(_) => {
            let mut topics = topics.write().await;
            for topic in state.subscribed_topics.drain() {
                if let Some(subs) = topics.get_mut(&topic) {
                    subs.remove(ws);
                    if subs.is_empty() {
                        topics.remove(&topic);
                    }
                }
            }
            state.closed = true;
        }
        Message::Ping(payload) => {
            ws.try_send(Message::Pong(payload)).await?;
        }
        Message::Pong(_) => {
            state.pong_received = true;
        }
        _ => {}
    }
    Ok(())
}

#[derive(Debug)]
struct ConnState {
    closed: bool,
    pong_received: bool,
    subscribed_topics: HashSet<Arc<str>>,
}

impl Default for ConnState {
    fn default() -> Self {
        ConnState {
            closed: false,
            pong_received: true,
            subscribed_topics: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum Signal<'a> {
    #[serde(rename = "publish")]
    Publish { topic: &'a str },
    #[serde(rename = "subscribe")]
    Subscribe { topics: Vec<&'a str> },
    #[serde(rename = "unsubscribe")]
    Unsubscribe { topics: Vec<&'a str> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_subscribe_serialization() {
        let subscribe = Signal::Subscribe {
            topics: vec!["room1", "room2"],
        };
        let json = serde_json::to_string(&subscribe).unwrap();
        assert!(json.contains("\"type\":\"subscribe\""));
        assert!(json.contains("\"topics\""));
        assert!(json.contains("room1"));
        assert!(json.contains("room2"));
    }

    #[test]
    fn test_signal_unsubscribe_serialization() {
        let unsubscribe = Signal::Unsubscribe {
            topics: vec!["room1"],
        };
        let json = serde_json::to_string(&unsubscribe).unwrap();
        assert!(json.contains("\"type\":\"unsubscribe\""));
        assert!(json.contains("\"topics\""));
        assert!(json.contains("room1"));
    }

    #[test]
    fn test_signal_publish_serialization() {
        let publish = Signal::Publish { topic: "room1" };
        let json = serde_json::to_string(&publish).unwrap();
        assert!(json.contains("\"type\":\"publish\""));
        assert!(json.contains("\"topic\":\"room1\""));
    }

    #[test]
    fn test_signal_subscribe_deserialization() {
        let json = r#"{"type":"subscribe","topics":["room1","room2"]}"#;
        let signal: Signal = serde_json::from_str(json).unwrap();
        assert_eq!(
            signal,
            Signal::Subscribe {
                topics: vec!["room1", "room2"]
            }
        );
    }

    #[test]
    fn test_signal_unsubscribe_deserialization() {
        let json = r#"{"type":"unsubscribe","topics":["room1"]}"#;
        let signal: Signal = serde_json::from_str(json).unwrap();
        assert_eq!(
            signal,
            Signal::Unsubscribe {
                topics: vec!["room1"]
            }
        );
    }

    #[test]
    fn test_signal_publish_deserialization() {
        let json = r#"{"type":"publish","topic":"room1"}"#;
        let signal: Signal = serde_json::from_str(json).unwrap();
        assert_eq!(signal, Signal::Publish { topic: "room1" });
    }

    #[test]
    fn test_signal_invalid_type_deserialization() {
        let json = r#"{"type":"invalid","topics":[]}"#;
        let result: Result<Signal, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_signal_missing_field_deserialization() {
        // Missing topics field for subscribe
        let json = r#"{"type":"subscribe"}"#;
        let result: Result<Signal, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_signaling_service_new() {
        let service = SignalingService::new();
        let topics = service.0.read().await;
        assert_eq!(topics.len(), 0);
    }

    #[tokio::test]
    async fn test_signaling_service_default() {
        let service = SignalingService::default();
        let topics = service.0.read().await;
        assert_eq!(topics.len(), 0);
    }

    #[tokio::test]
    async fn test_signaling_service_clone() {
        let service = SignalingService::new();
        let cloned = service.clone();

        // Both should point to the same underlying data
        let topics1 = service.0.read().await;
        let topics2 = cloned.0.read().await;
        assert_eq!(topics1.len(), topics2.len());
    }

    #[test]
    fn test_conn_state_default() {
        let state = ConnState::default();
        assert!(!state.closed);
        assert!(state.pong_received);
        assert_eq!(state.subscribed_topics.len(), 0);
    }

    #[test]
    fn test_conn_state_subscribed_topics() {
        let mut state = ConnState::default();

        // Add topics
        let topic1: Arc<str> = "room1".into();
        let topic2: Arc<str> = "room2".into();

        state.subscribed_topics.insert(topic1.clone());
        state.subscribed_topics.insert(topic2.clone());

        assert_eq!(state.subscribed_topics.len(), 2);
        assert!(state.subscribed_topics.contains(&topic1));
        assert!(state.subscribed_topics.contains(&topic2));

        // Drain topics
        let drained: Vec<_> = state.subscribed_topics.drain().collect();
        assert_eq!(drained.len(), 2);
        assert_eq!(state.subscribed_topics.len(), 0);
    }

    #[test]
    fn test_ws_sink_hash_equality() {
        use std::collections::hash_map::DefaultHasher;

        // Test that Arc pointer-based equality works correctly
        let arc1 = Arc::new(Mutex::new(()));
        let arc2 = Arc::new(Mutex::new(()));
        let arc1_clone = arc1.clone();

        let ptr1 = Arc::as_ptr(&arc1) as usize;
        let ptr2 = Arc::as_ptr(&arc2) as usize;
        let ptr1_clone = Arc::as_ptr(&arc1_clone) as usize;

        // Same Arc should have same pointer
        assert_eq!(ptr1, ptr1_clone);
        // Different Arc should have different pointer
        assert_ne!(ptr1, ptr2);

        // Test Hash consistency
        let mut hasher1 = DefaultHasher::new();
        ptr1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        ptr1_clone.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_ping_timeout_constant() {
        assert_eq!(PING_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn test_signal_roundtrip() {
        // Test that serialization and deserialization are inverses
        let signals = vec![
            Signal::Subscribe {
                topics: vec!["room1", "room2", "room3"],
            },
            Signal::Unsubscribe {
                topics: vec!["room1"],
            },
            Signal::Publish { topic: "test-room" },
        ];

        for original in signals {
            let json = serde_json::to_string(&original).unwrap();
            let deserialized: Signal = serde_json::from_str(&json).unwrap();
            assert_eq!(original, deserialized);
        }
    }

    #[test]
    fn test_signal_with_empty_topics() {
        // Subscribe with empty topics
        let subscribe = Signal::Subscribe { topics: vec![] };
        let json = serde_json::to_string(&subscribe).unwrap();
        let deserialized: Signal = serde_json::from_str(&json).unwrap();
        assert_eq!(subscribe, deserialized);

        // Unsubscribe with empty topics
        let unsubscribe = Signal::Unsubscribe { topics: vec![] };
        let json = serde_json::to_string(&unsubscribe).unwrap();
        let deserialized: Signal = serde_json::from_str(&json).unwrap();
        assert_eq!(unsubscribe, deserialized);
    }

    #[test]
    fn test_signal_with_special_characters() {
        // Test that topic names with special characters are handled correctly
        let topics = vec!["room-1", "room_2", "room.3", "room:4"];
        let subscribe = Signal::Subscribe {
            topics: topics.clone(),
        };
        let json = serde_json::to_string(&subscribe).unwrap();
        let deserialized: Signal = serde_json::from_str(&json).unwrap();
        assert_eq!(subscribe, deserialized);
    }

    #[tokio::test]
    async fn test_topics_concurrent_access() {
        // Test that Topics (Arc<RwLock<...>>) can be accessed concurrently
        let topics: Topics = Arc::new(RwLock::new(HashMap::new()));

        let topics1 = topics.clone();
        let topics2 = topics.clone();

        // Spawn two tasks that access the topics concurrently
        let handle1 = tokio::spawn(async move {
            let read_guard = topics1.read().await;
            read_guard.len()
        });

        let handle2 = tokio::spawn(async move {
            let read_guard = topics2.read().await;
            read_guard.len()
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        assert_eq!(result1, 0);
        assert_eq!(result2, 0);
    }

    #[tokio::test]
    async fn test_topics_write_access() {
        // Test that we can write to topics
        let topics: Topics = Arc::new(RwLock::new(HashMap::new()));

        {
            let mut write_guard = topics.write().await;
            let topic: Arc<str> = "test-room".into();
            write_guard.insert(topic, HashSet::new());
        }

        let read_guard = topics.read().await;
        assert_eq!(read_guard.len(), 1);
        assert!(read_guard.contains_key("test-room"));
    }
}
