use super::*;
use crate::inbound_journal::InboundJournal;
use crate::socket::noise_socket::NoiseSocket;
use crate::types::message::MessageInfo;
use async_trait::async_trait;
use prost::Message;
use tokio::sync::{Notify, Semaphore};
use wacore::handshake::NoiseCipher;
use wacore_binary::builder::NodeBuilder;
use waproto::whatsapp;

struct Journal {
    raw_started: Notify,
    raw_commit: Semaphore,
    decoded_started: Notify,
    decoded_commit: Semaphore,
    finished: Notify,
    complete: AtomicBool,
    fail: bool,
    payload: std::sync::Mutex<Vec<u8>>,
}

#[async_trait]
impl InboundJournal for Journal {
    async fn begin(&self, _: &Node, _: Option<&MessageInfo>) -> anyhow::Result<bool> {
        self.raw_started.notify_one();
        self.raw_commit.acquire().await?.forget();
        Ok(false)
    }
    async fn decoded(&self, _: &MessageInfo, message: &whatsapp::Message) -> anyhow::Result<()> {
        *self.payload.lock().unwrap() = message.encode_to_vec();
        self.decoded_started.notify_one();
        self.decoded_commit.acquire().await?.forget();
        anyhow::ensure!(!self.fail, "synthetic durable commit failure");
        Ok(())
    }
    async fn complete(&self, _: &Node) -> anyhow::Result<bool> {
        self.complete.store(!self.fail, Ordering::Release);
        self.finished.notify_one();
        anyhow::ensure!(!self.fail, "synthetic durable commit failure");
        Ok(true)
    }
}

struct Wire {
    journal: Arc<Journal>,
    writes: AtomicUsize,
    wrote: Notify,
}
#[async_trait]
impl crate::transport::Transport for Wire {
    async fn send(&self, _: Vec<u8>) -> anyhow::Result<()> {
        assert!(
            self.journal.complete.load(Ordering::Acquire),
            "network ACK preceded the durable commit"
        );
        self.writes.fetch_add(1, Ordering::AcqRel);
        self.wrote.notify_one();
        Ok(())
    }
    async fn disconnect(&self) {}
}

#[tokio::test]
async fn ac_m0_11_ingress_protocol_ack_waits_for_raw_and_decoded_commit() {
    for fail in [false, true] {
        let client = crate::test_utils::create_test_client().await;
        let journal = Arc::new(Journal {
            raw_started: Notify::new(),
            raw_commit: Semaphore::new(0),
            decoded_started: Notify::new(),
            decoded_commit: Semaphore::new(0),
            finished: Notify::new(),
            complete: AtomicBool::new(false),
            fail,
            payload: std::sync::Mutex::new(Vec::new()),
        });
        client.set_inbound_journal(journal.clone()).unwrap();
        let wire = Arc::new(Wire {
            journal: journal.clone(),
            writes: AtomicUsize::new(0),
            wrote: Notify::new(),
        });
        let key = [0x73; 32];
        *client.noise_socket.lock().await = Some(Arc::new(NoiseSocket::new(
            client.runtime.clone(),
            wire.clone(),
            NoiseCipher::new(&key).unwrap(),
            NoiseCipher::new(&key).unwrap(),
        )));
        client.is_connected.store(true, Ordering::Release);
        let message = whatsapp::Message {
            conversation: Some("synthetic private text".into()),
            ..Default::default()
        };
        let expected = message.encode_to_vec();
        let node = NodeBuilder::new("message")
            .attr("from", "120363000000000001@newsletter")
            .attr("id", "synthetic-source-a")
            .attr("t", "1794000000")
            .attr("type", "text")
            .children([NodeBuilder::new("plaintext")
                .bytes(expected.clone())
                .build()])
            .build();
        client.process_node(Arc::new(node)).await;
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            journal.raw_started.notified(),
        )
        .await
        .unwrap();
        assert_eq!(wire.writes.load(Ordering::Acquire), 0);
        journal.raw_commit.add_permits(1);
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            journal.decoded_started.notified(),
        )
        .await
        .unwrap();
        assert_eq!(wire.writes.load(Ordering::Acquire), 0);
        journal.decoded_commit.add_permits(1);
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            journal.finished.notified(),
        )
        .await
        .unwrap();
        assert_eq!(*journal.payload.lock().unwrap(), expected);
        if !fail {
            tokio::time::timeout(std::time::Duration::from_secs(3), wire.wrote.notified())
                .await
                .unwrap();
            assert_eq!(wire.writes.load(Ordering::Acquire), 1);
        } else {
            // The processing completion signal follows the failed commit. No
            // transport ACK or delivery receipt is allowed on this path.
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(3), wire.wrote.notified())
                    .await
                    .is_err()
            );
            assert_eq!(wire.writes.load(Ordering::Acquire), 0);
        }
    }
}
