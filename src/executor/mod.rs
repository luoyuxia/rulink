mod create;
pub(crate) mod evaluator;
mod projection;
mod table_scan;
mod executor;
mod insert;
mod hash_agg;
mod drop;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use egg::{Id, Language};
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use crate::catalog::{CatalogRef, ColumnId, TableCatalog, TableRefId};
use crate::checkpoint::BarrierManager;
use crate::executor::executor::WrapExecutor;
use crate::executor::create::CreateTableExecutor;
use crate::stream::{Message, Job};
use crate::types::{ColumnIndex, ConvertError, DataType};
use crate::connector::data_gen::DataGenSource;
use crate::connector::{BlackHole, FileSystemConnector, Print, StreamConnector, ValueConnector};
use crate::executor::drop::DropExecutor;
use crate::executor::insert::TableInsertExecutor;
use crate::executor::hash_agg::HashAggExecutor;
use crate::executor::projection::ProjectionExecutor;
use crate::executor::table_scan::TableScanExecutor;
use crate::planner::{RecExpr, TypeSchemaAnalysis};



use crate::planner::Expr;


/// The maximum chunk length produced by executor at a time.
pub const PROCESSING_WINDOW_SIZE: usize = 1024;

#[derive(thiserror::Error, Debug)]
pub enum ExecuteError {
    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
}

pub type BoxedExecutor = BoxStream<'static, Result<Message, ExecuteError>>;
pub type BarrierManagerRef = Arc<Mutex<BarrierManager>>;

#[derive(thiserror::Error, Debug)]
pub enum ExecutorError {

    #[error("conversion error: {0}")]
    Convert(#[from] ConvertError),
}

pub struct ExecutorBuilder {
    catalog: CatalogRef,
    current_executor_id: u32,
    egraph: egg::EGraph<Expr, TypeSchemaAnalysis>,
    root: Id,
}

impl ExecutorBuilder {
    pub fn new(
        catalog: CatalogRef,
        plan: &RecExpr
    ) -> ExecutorBuilder {
        let mut egraph = egg::EGraph::new(TypeSchemaAnalysis {
            catalog: catalog.clone(),
        });
        let root = egraph.add_expr(plan);
        ExecutorBuilder {
            catalog: catalog.clone(),
            current_executor_id: 0,
            egraph,
            root
        }
    }

    pub fn build_job(&mut self, job_id: String) -> Job {
        let is_ddl_job = self.is_ddl(self.node(self.root));
        let barrier_manager = Arc::new(Mutex::new(BarrierManager::new()));
        let executor = self.build(self.root,
                                  0,
                                  barrier_manager.clone());
        Job {
            current_job_id: job_id,
            barrier_manager: barrier_manager.clone(),
            result_executor: executor,
            is_ddl_job
        }
    }

    pub fn build(&mut self, id: Id, executor_id: u32, barrier_manager: BarrierManagerRef) -> BoxedExecutor {
        use Expr::*;
        match self.node(id).clone() {
            CreateTable(plan) => {
                CreateTableExecutor {
                    plan,
                    catalog: self.catalog.clone()
                }.execute()
            },
            Drop(plan) => {
                DropExecutor {
                    plan,
                    catalog: self.catalog.clone(),
                }.execute()
            },

            Values(rows) =>
                {
                    let column_types = self.plan_types(id).to_vec();
                    let values = rows.iter()
                        .map(|row| {
                            (self.node(*row).as_list().iter())
                                .map(|id| self.recexpr(*id))
                                .collect()
                        }).collect();
                    let connector = ValueConnector {
                        column_types,
                        values
                    };
                    self.build_table_scan_executor(executor_id,
                                                   barrier_manager.clone(),
                                                   Box::new(connector))
                },
            Limit([limit, offset, child]) => {
                // todo: implement limit/offset
                self.build(child, executor_id +1, barrier_manager.clone())
            }

            Insert([table, cols, child]) => {
                let child = self.build(child,
                                       executor_id +1,
                                       barrier_manager.clone());
                self.build_executor(|| {
                    let table =
                        self.catalog.get_table(self.node(table)
                        .as_table());
                    let column_ids =  self.column_ids(cols);
                    let connector = self.get_connector(
                        table.clone().unwrap(),
                        column_ids,
                        table.clone().unwrap().get_options(), false);
                    TableInsertExecutor {
                        sink_connector: connector,
                        child,
                    }.execute()
                }, executor_id,barrier_manager.clone())
            },

            Scan([table, cols, filter]) => {
                self.build_executor(|| {
                    let table = self.catalog.get_table(self.node(table).as_table());
                    let column_ids =  self.column_ids(cols);
                    let connector =
                        self.get_connector(
                        table.clone().unwrap(),
                        column_ids,
                        table.clone().unwrap().get_options(),
                        true);
                    self.build_table_scan_executor(executor_id,
                                                   barrier_manager.clone(),
                                                   connector)
                }, executor_id, barrier_manager.clone())
            },

            Proj([projs, child]) => {
                let child_executor = self.build(child,
                                                executor_id +1, barrier_manager.clone());
                self.build_executor(|| {
                    ProjectionExecutor {
                        exprs: self.resolve_column_index(projs, child),
                        child: child_executor,
                    }.execute()
                }, executor_id, barrier_manager.clone())
            },

            Filter([cond, child]) => {
                self.build(child, executor_id + 1, barrier_manager.clone())
            }

            Agg([aggs, group_keys, child]) => {
                let aggs = self.resolve_column_index(aggs, child);
                let group_keys = self.resolve_column_index(group_keys, child);
                let child_executor = self.build(child,
                                                executor_id +1, barrier_manager.clone());
                self.build_executor(|| {
                    HashAggExecutor::new(
                        aggs,
                        group_keys,
                        self.plan_types(id).to_vec(),
                        child_executor,
                    ).execute()
                }, executor_id, barrier_manager.clone())

            },
            _ => {
                println!("{:?}", self.node(id).clone().to_string());
                todo!()
            }
        }
    }

    fn build_table_scan_executor(&self, executor_id: u32,
                                 barrier_manager: BarrierManagerRef,
                                 connector: Box<dyn StreamConnector + Sync + Send>) -> BoxedExecutor {
        let (sender, rx) = mpsc::unbounded_channel();
        barrier_manager.lock().unwrap().register_sender(executor_id, sender);
        self.build_executor(|| {
            TableScanExecutor {
                data_source: connector,
                rx: Some(rx),
            }.execute()
        }, executor_id,barrier_manager.clone())
    }

    fn column_ids(&self, col_id: Id) -> Vec<ColumnId> {
        self.node(col_id)
            .as_list().iter().map(|id| self.node(*id).as_column().column_id)
            .collect()
    }

    pub fn build_executor<F> (&self,inner_executor_f: F, executor_id: u32, barrier_manager: Arc<Mutex<BarrierManager>>) -> BoxedExecutor
        where F: FnOnce() -> BoxedExecutor {
        barrier_manager.lock().unwrap().register_actor(executor_id);
        let inner =  inner_executor_f();
        self.wrap_executor(inner,executor_id, barrier_manager.clone())
    }

    pub fn is_ddl(&self, expr: &Expr) -> bool {
        match expr {
            Expr::CreateTable(_) => true,
            Expr::Drop(_) => true,
            _ => false
        }
    }

    fn plan_types(&self, id: Id) -> &[DataType] {
        let ty = self.egraph[id].data.type_.as_ref().unwrap();
        ty.kind.as_struct()
    }

    fn node(&self, id: Id) -> &Expr {
        &self.egraph[id].nodes[0]
    }

    fn recexpr(&self, id: Id) -> RecExpr {
        self.node(id).build_recexpr(|id| self.node(id).clone())
    }

    fn resolve_column_index(&self, expr: Id, plan: Id) -> RecExpr {
        let schema = &self.egraph[plan].data.schema;
        self.node(expr).build_recexpr(|id| {
            if let Some(idx) = schema.iter().position(|x| *x == id) {
                return Expr::ColumnIndex(ColumnIndex(idx as _))
            }
            match self.node(id) {
                Expr::Column(c) => panic!("column {c} not found from input"),
                e => e.clone()
            }
        })
    }

    fn get_connector(&self, table: Arc<TableCatalog>, column_ids: Vec<ColumnId>,
                     options: HashMap<String, String>, is_source: bool) -> Box<dyn StreamConnector + Send + Sync> {
        let value = table.clone().get_option("connector").unwrap();
        match value.to_lowercase().as_str() {
            "datagen" => {
                Box::new(DataGenSource {
                    column_ids,
                    table,
                })
            },
            "print" => {
                Box::new(Print {
                    column_ids,
                    table
                })
            },
            "blackhole" => {
              Box::new(BlackHole {
                  column_ids,
                  table,
              })
            },
            "filesystem" => {
                match is_source {
                    true => {
                        Box::new(FileSystemConnector::new_source(column_ids, table, options))
                    }
                    false => {
                        Box::new(FileSystemConnector::new_sink(column_ids, table, options))
                    }
                }
            },
            _ => {
                unimplemented!("connector {} is not supported", value);
            }
        }
    }

    fn wrap_executor(&self, inner_executor: BoxedExecutor, actor_id: u32,
                     barrier_manager: Arc<Mutex<BarrierManager>> ) -> BoxedExecutor{
        WrapExecutor::new(inner_executor, actor_id, barrier_manager).execute()
    }

    fn build_stream_source(&self, table_ref_id: TableRefId, column_ids: Vec<ColumnId>) -> DataGenSource {
        let table = self.catalog.get_table(table_ref_id);
        return DataGenSource {
            column_ids,
            table: table.unwrap()
        };
    }
}
