use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use crate::array::data_chunk::DataChunk;
use crate::binder::{Binder, BindError};
use crate::catalog::{CatalogRef, DatabaseCatalog};
use crate::executor::{ExecutorBuilder, ExecuteError};
use crate::parser::{parse, ParserError};
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::checkpoint::BarrierManager;
use crate::stream::StreamRunningJob;
use {
    once_cell::sync::Lazy,
    regex::Regex,
};
use crate::planner::Optimizer;

// one for the actual jobs, one for the thread that send checkpoint
type RunningJob = (JoinHandle<Result<(), Error>>, JoinHandle<()>);

pub struct RunResult {
    pub result_chunk: DataChunk,
    pub job_id: Option<String>
}

impl RunResult {
    pub fn new(chunk: DataChunk, job_id: Option<String>) -> Self {
        RunResult {
            result_chunk: chunk,
            job_id,
        }
    }
}

static KILL_JOB_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^kill job ([\da-fA-F-]+);*$").unwrap());
static SHOW_JOB_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^show jobs;*$").unwrap());

pub struct Database {
    catalog: CatalogRef,
    barrier_manager: Arc<Mutex<BarrierManager>>,
    running_jobs: HashMap<String, StreamRunningJob>,
    last_running_job_id: Option<String>
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

impl Database {
    pub fn new() -> Self {
        let catalog = Arc::new(DatabaseCatalog::new());

        let barrier_manager = Arc::new(
            Mutex::new(BarrierManager::new()));

        Database { catalog: catalog.clone(),
            barrier_manager,
            running_jobs: Default::default(),
            last_running_job_id: None
        }
    }

    pub fn get_last_running_job_id(&mut self) -> Option<String> {
        self.last_running_job_id.clone()
    }

    pub fn set_last_running_job_id(&mut self, running_job_id: String) {
        self.last_running_job_id = Some(running_job_id);
    }

    pub fn reset_last_running_job_id(&mut self)  {
        self.last_running_job_id = None;
    }

    async fn try_run_in_extend_executor(&mut self, sql: &str) -> Option<DataChunk> {
        if let Some(cap) = KILL_JOB_RE.captures(sql.trim()) {
            let job_id = cap.get(1).unwrap().as_str();
            return if self.stop_job(job_id).await {
                Some(DataChunk::single_str(format!("Kill job {} successfully", job_id).as_str()))
            } else {
                Some(DataChunk::single_str(format!("Kill job {} successfully", job_id).as_str()))
            }
        }
        if SHOW_JOB_RE.is_match(sql.trim()) {
            let running_job_ids: Vec<String> = self.running_jobs.keys().map(|job_id|
                job_id.clone()).collect();
            return Some(DataChunk::from_strs(running_job_ids));
        }
        return None;
    }

    pub async fn stop_job(&mut self, job_id: &str) -> bool {
        if let Some(job) = self.running_jobs.remove(job_id) {
            println!("stopping job {}.", job_id);
            job.stop().await;
            true
        } else {
            false
        }
    }

    pub async fn run(&mut self, sql: &str) -> Result<RunResult, Error> {
        if let Some(data_chunk) = self.try_run_in_extend_executor(sql).await {
            return Ok(RunResult::new(data_chunk, None));
        }
        // parse sql
        let stmts = parse(sql)?;
        assert_eq!(stmts.len(), 1, "only support to handle one statement at once.");
        let stmt = stmts.get(0).unwrap().to_owned();

        let mut binder = Binder::new(self.catalog.clone());
        let optimizer = Optimizer::new(self.catalog.clone());

        let bound = binder.bind(stmt)?;

        let optimized = optimizer.optimize(&bound);

        let mut executor_builder = ExecutorBuilder::new(
            self.catalog.clone(), &optimized);


        let job_id = Uuid::new_v4();
        let job = executor_builder.build_job(job_id.to_string());
        return if let Some(running_job) = job.run().await {
            self.running_jobs.insert(job_id.to_string(), running_job);
            let s = format!("running job id: {}", job_id);
            Ok(RunResult::new(
                DataChunk::single_str(s.as_str()),
                Some(job_id.to_string())))
        } else {
            Ok(RunResult::new(
                DataChunk::single_str("execute successfully"), None))
        }
    }
}

/// The error type of database operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parse error: {0}")]
    Parse(#[from] ParserError),
    #[error("bind error: {0}")]
    Bind(#[from] BindError),
    #[error("execute error: {0}")]
    Execute(#[from] ExecuteError),
}