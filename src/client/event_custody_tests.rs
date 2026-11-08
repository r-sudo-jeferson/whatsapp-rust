use super::*;
use crate::inbound_journal::InboundJournal;
use crate::socket::noise_socket::NoiseSocket;
use crate::types::message::MessageInfo;
use async_trait::async_trait;
use tokio::sync::{Notify, Semaphore};
use wacore::handshake::NoiseCipher;
use wacore_binary::builder::NodeBuilder;
use waproto::whatsapp;

// The external persistence boundary is controlled; Client routing, actual
// NoiseSocket framing, ACK selection and the external transport remain real.
struct EventJournal {
    started: Notify,
    commit: Semaphore,
    complete: AtomicBool,
    fail: bool,
    original: std::sync::Mutex<Vec<u8>>,
}
#[async_trait]
impl InboundJournal for EventJournal {
    async fn begin(&self, node: &Node, _: Option<&MessageInfo>) -> anyhow::Result<bool> {
        self.started.notify_one();
        self.commit.acquire().await?.forget();
        anyhow::ensure!(!self.fail, "synthetic custody failure");
        *self.original.lock().unwrap() = serde_json::to_vec(&serde_json::to_value(node)?)?;
        Ok(false)
    }
    async fn decoded(&self, _: &MessageInfo, _: &whatsapp::Message) -> anyhow::Result<()> {
        anyhow::bail!("non-message or malformed source must not invent decoded bytes")
    }
    async fn complete(&self, _: &Node) -> anyhow::Result<bool> {
        anyhow::ensure!(!self.fail, "synthetic custody failure");
        self.complete.store(true, Ordering::Release);
        Ok(false)
    }
}
struct EventWire {
    journal: Arc<EventJournal>,
    writes: AtomicUsize,
    wrote: Notify,
}
#[async_trait]
impl crate::transport::Transport for EventWire {
    async fn send(&self, _: Vec<u8>) -> anyhow::Result<()> {
        self.writes.fetch_add(1, Ordering::AcqRel);
        self.wrote.notify_one();
        assert!(
            self.journal.complete.load(Ordering::Acquire),
            "ACK preceded durable event custody"
        );
        Ok(())
    }
    async fn disconnect(&self) {}
}

#[tokio::test]
async fn ac_cap_map_events_unknown_and_malformed_sources_are_retained_before_dispatch_or_ack() {
    for tag in [
        "receipt",
        "notification",
        "call",
        "presence",
        "chatstate",
        "synthetic_future",
        "message",
    ] {
        for fail in [false, true] {
            let client = crate::test_utils::create_test_client_with_name("event-custody").await;
            let journal = Arc::new(EventJournal {
                started: Notify::new(),
                commit: Semaphore::new(0),
                complete: AtomicBool::new(false),
                fail,
                original: std::sync::Mutex::new(Vec::new()),
            });
            client.set_inbound_journal(journal.clone()).unwrap();
            let wire = Arc::new(EventWire {
                journal: journal.clone(),
                writes: AtomicUsize::new(0),
                wrote: Notify::new(),
            });
            let key = [0x76; 32];
            *client.noise_socket.lock().await = Some(Arc::new(NoiseSocket::new(
                client.runtime.clone(),
                wire.clone(),
                NoiseCipher::new(&key).unwrap(),
                NoiseCipher::new(&key).unwrap(),
            )));
            client.is_connected.store(true, Ordering::Release);
            let mut builder = NodeBuilder::new(tag)
                .attr("id", "synthetic-event")
                .attr("t", "1794000000")
                .attr("type", "synthetic_unhandled");
            // A message with no source currently exits before its chat mailbox.
            // The exact malformed source must still be retained, never ACKed.
            if tag != "message" {
                builder = builder.attr("from", "15550000055@s.whatsapp.net");
            }
            let node = builder.build();
            let expected = serde_json::to_vec(&serde_json::to_value(&node).unwrap()).unwrap();
            let handler = client.clone();
            let task = tokio::spawn(async move { handler.process_node(Arc::new(node)).await });
            tokio::time::timeout(
                std::time::Duration::from_secs(3),
                journal.started.notified(),
            )
            .await
            .expect("source bypassed durable journal");
            assert_eq!(
                wire.writes.load(Ordering::Acquire),
                0,
                "network write escaped while custody was pending: {tag}"
            );
            journal.commit.add_permits(1);
            tokio::time::timeout(std::time::Duration::from_secs(3), task)
                .await
                .unwrap()
                .unwrap();
            if fail {
                assert!(journal.original.lock().unwrap().is_empty());
                assert!(
                    tokio::time::timeout(std::time::Duration::from_secs(3), wire.wrote.notified())
                        .await
                        .is_err(),
                    "failed source was acknowledged: {tag}"
                );
            } else {
                assert_eq!(
                    *journal.original.lock().unwrap(),
                    expected,
                    "source changed before custody: {tag}"
                );
                if matches!(tag, "receipt" | "notification" | "call") {
                    tokio::time::timeout(std::time::Duration::from_secs(3), wire.wrote.notified())
                        .await
                        .unwrap();
                    assert_eq!(wire.writes.load(Ordering::Acquire), 1);
                } else {
                    assert_eq!(wire.writes.load(Ordering::Acquire), 0);
                }
            }
            client.disconnect().await;
        }
    }
}
