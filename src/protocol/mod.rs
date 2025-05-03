//! Client-server communication protocol.
//!
//! This module defines the communication protocol used between Cryo clients and servers,
//! including message formats, encoding strategies, and transport abstractions. It provides
//! the foundational types and logic required to serialize, deserialize, and interpret
//! requests and responses over the network.
//!
//! # Overview
//!
//! The protocol layer is responsible for defining how structured queries and control
//! commands are exchanged between the client and the Cryo server. It ensures compatibility,
//! extensibility, and robustness of communication across distributed components.
//!
//! Messages are encoded using a binary format optimized for low-latency parsing and minimal
//! overhead. This module includes both low-level message definitions and higher-level
//! abstractions for sending and receiving protocol messages over a network stream.
//!
//! # Key Components
//!
//! - [`Message`]: Structure representing a message from either a Client or Server.
//! - [`ProtocolTransport`]: Abstraction over a bidirectional transport (e.g., TCP, TLS) used to exchange messages.
//!
//! # Binary Format
//!
//! Protocol messages are serialized with a compact framing format:
//!
//! - Each message begins with a fixed-size header, containing the message type and payload length.
//! - The payload follows, encoded according to message-specific rules (e.g., row sets, error codes).
//! - All integers are little-endian by default.
//!
//! This format allows efficient streaming and multiplexing of messages over a single connection.
//!
//! # Extensibility
//!
//! The protocol is designed to be versioned and forward-compatible:
//!
//! - Message enums are tagged with discriminants for stable wire representation.
//! - Unknown or unsupported message types can be ignored or handled gracefully.
//! - Future versions can extend the format while preserving backward compatibility.
//!
//! # See Also
//!
//! - [`storage`](crate::storage): Data layer that ultimately executes protocol-level queries.
mod request;
mod response;
mod server;
mod thread;
mod transport;

use thread::ThreadPool;

pub use request::Request;
pub use response::Response;
pub use server::StorageServer;
pub use transport::ProtocolTransport;
