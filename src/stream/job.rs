use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tokio::task::JoinHandle;
use crate::checkpoint::{BarrierManager, BarrierService};
use crate::Error;
use crate::executor::BoxedExecutor;
use crate::stream::Message;
use futures::{TryStreamExt};

pub struct Job {
    pub current_job_id: String,
    pub barrier_manager: Arc<Mutex<BarrierManager>>,
    pub result_executor: BoxedExecutor,

    pub is_ddl_job: bool,
}

pub struct StreamRunningJob {
    pub current_job_id: String,
    pub job_running_handle: Option<JoinHandle<Result<(), Error>>>,
    pub job_checkpoint_handle: Option<JoinHandle<()>>,
}

impl Job {
    pub async fn run(self) -> Option<StreamRunningJob> {
        if self.is_ddl_job {
            self.running_ddl_job().await;
            None
        } else {
            Some(self.running_stream_job())
        }
    }

    async fn running_ddl_job(self) {
        let mut executor = self.result_executor;
        tokio::spawn(async move {
            // may follow actor::run_consumer
            while let Some(chunk) = executor.try_next().await? {
                match chunk {
                    _ => {
                        // do nothing
                    }
                }
            }
            Ok(()) as Result<(), Error>
        });
    }

    fn running_stream_job(self) -> StreamRunningJob {
        let mut executor = self.result_executor;
        // the task for running
        let job_task = tokio::spawn(async move{
            // may follow actor::run_consumer
            while let Some(chunk) = executor.try_next().await? {
                match chunk {
                    Message::Chunk(chunk) => {
                        println!("{}", &chunk);
                    }
                    Message::Barrier(_) => {}
                }
            }
            Ok(()) as Result<(), Error>
        });
        // the task for checkpoint
        let mut sender_service = BarrierService::new(self.barrier_manager.clone());
        let checkpoint_task =
            tokio::spawn(async move {
                // trigger one barrier
                sender_service.send_barrier();
                loop {
                    let mut min_interval = tokio::time::interval(Duration::from_secs(100));
                    min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    tokio::select! {
                                    barrier = sender_service.collect_barrier() => {
                                        // println!("receive checkpoint ack of epoch {}.", barrier.epoch);
                                        thread::sleep(Duration::from_secs(2));
                                        sender_service.send_barrier();
                                    }
                                }
                }
            });
       StreamRunningJob {
           current_job_id: self.current_job_id,
           job_running_handle: Some(job_task),
           job_checkpoint_handle: Some(checkpoint_task)
       }
    }
}

impl StreamRunningJob {
    pub async fn stop(self) {
        // job thread
        if let Some(job_running_handle) = self.job_running_handle {
            job_running_handle.abort();
            let result = job_running_handle.await;
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }

        // checkpoint thread
        if let Some(job_checkpoint_handle) = self.job_checkpoint_handle {
            job_checkpoint_handle.abort();
            let result = job_checkpoint_handle.await;
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }
    }
}