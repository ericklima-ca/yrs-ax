use std::sync::Arc;

pub mod broadcast;
pub mod conn;
pub mod signaling;
pub mod ws;

pub type AwarenessRef = Arc<yrs::sync::Awareness>;
