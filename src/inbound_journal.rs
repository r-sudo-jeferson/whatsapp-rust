//! Durable boundary for consumers that cannot acknowledge an in-memory enqueue.

use crate::types::message::MessageInfo;
use async_trait::async_trait;
use wacore_binary::node::Node;
use waproto::whatsapp::Message;

#[async_trait]
pub trait InboundJournal: Send + Sync {
    /// Retain the exact stanza before decryption. Return true only if this exact
    /// source already completed durably, so the Signal ratchet need not replay it.
    async fn begin(&self, node: &Node, info: Option<&MessageInfo>) -> anyhow::Result<bool>;

    /// Retain the complete decoded message before projections or protocol edits.
    /// A failure must also make complete() fail for the current source.
    async fn decoded(&self, info: &MessageInfo, message: &Message) -> anyhow::Result<()>;

    /// Commit completion (including a visible unsupported/undecodable source).
    /// Return whether decoded content was retained. Errors prohibit both ACK and
    /// delivery receipt; this is independent of the later application inbox ACK.
    async fn complete(&self, node: &Node) -> anyhow::Result<bool>;
}
