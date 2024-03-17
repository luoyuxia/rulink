use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use crate::stream::Barrier;
use std::collections::{BTreeMap, HashMap, HashSet};
use futures::{SinkExt};
use tokio::sync::{oneshot};
use tokio::sync::oneshot::Sender;

#[derive(Debug)]
pub struct BarrierCompletion {
    pub epoch: u64
}

pub struct BarrierManager {
    senders: HashMap<u32, Vec<UnboundedSender<Barrier>>>,
    all_actors: HashSet<u32>,

    epoch_barrier_remain_actors: BTreeMap<u64, HashSet<u32>>,
    barrier_complete_receiver: HashMap<u64, oneshot::Receiver<BarrierCompletion>>,
    barrier_complete_sender: HashMap<u64, Sender<BarrierCompletion>>,
    receiver: Option<oneshot::Receiver<BarrierCompletion>>,
    sender: Option<Sender<BarrierCompletion>>,
}

impl BarrierManager {
    pub fn new() -> Self {
        let (tx, rx) =  oneshot::channel();
        Self {
            senders: HashMap::new(),
            all_actors: HashSet::new(),
            epoch_barrier_remain_actors: BTreeMap::new(),
            barrier_complete_receiver: HashMap::new(),
            barrier_complete_sender: HashMap::new(),
            sender: Some(tx),
            receiver: Some(rx)
        }
    }

    pub fn register_sender(&mut self, actor_id: u32, sender: UnboundedSender<Barrier>) {
        self.senders.entry(actor_id).or_default().push(sender);
    }

    pub fn send_barrier(&mut self, barrier: Barrier) {
        for (_, senders) in &self.senders {
            for sender in senders {
                if let Err(e) = sender.send(barrier.clone()) {
                    // panic!("Fail to send barrier: {}.", e);
                }
            }
        }

        self.register_barrier(barrier.epoch);
        let (tx, rx) =  oneshot::channel();

        self.barrier_complete_sender.insert(barrier.epoch, tx);
        self.barrier_complete_receiver.insert(barrier.epoch, rx);
    }

    pub fn register_actor(&mut self, actor_id: u32) {
        self.all_actors.insert(actor_id);
    }

    pub fn register_barrier(&mut self, epoch: u64) {
        self.epoch_barrier_remain_actors.insert(epoch, self.all_actors.clone());
    }

    pub fn notify_barrier_complete(&mut self, epoch: u64, actor_id: u32) {
        match self.epoch_barrier_remain_actors.get_mut(&epoch) {
            None => {
                panic!("Fail to get remain to be completed actors for epoch {}", epoch);
            }
            Some(value) => {
                value.remove(&actor_id);
                if value.is_empty() {
                    self.barrier_complete_sender.remove(&epoch)
                        .expect("Fail to remove complete sender for epoch")
                        .send(BarrierCompletion { epoch }).expect("TODO: panic message");
                }
            }
        }
    }
}

pub struct BarrierService {
    barrier_manager: Arc<Mutex<BarrierManager>>,
    current_epoch: u64
}

impl BarrierService {

    pub fn new(barrier_manager: Arc<Mutex<BarrierManager>>) -> Self {
        Self {
            barrier_manager,
            current_epoch: 0
        }
    }

    pub fn send_barrier(&mut self) {
        self.current_epoch = self.current_epoch + 1;

        // send barrier
        self.barrier_manager.lock().unwrap().send_barrier(Barrier { epoch: self.current_epoch, timestamp: 0 });
    }

    pub async fn collect_barrier(&self) -> BarrierCompletion {
        let complete_receiver =
            self.barrier_manager.lock().unwrap().barrier_complete_receiver.remove(&self.current_epoch)
                .expect("failed to get complete receiver");
        complete_receiver.await.expect("failed to get collect barrier")
    }
}