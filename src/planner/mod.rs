mod type_;
mod schema;
mod agg;

use std::collections::HashSet;
use egg::{Analysis, define_language, DidMerge, EGraph};

use crate::binder::CreateTable;
use crate::binder::BoundDrop;
use crate::catalog::{CatalogRef};
use egg::Id;
use crate::types::{DataType, DataValue, ColumnIndex};
use crate::catalog::{TableRefId, ColumnRefId};

pub use crate::planner::type_::TypeError;



pub type RecExpr = egg::RecExpr<Expr>;

define_language! {
    pub enum Expr {
        // values;
        Constant(DataValue),
        Type(DataType),
        Column(ColumnRefId),
        ColumnIndex(ColumnIndex),       // #0, #1, ...
        Table(TableRefId),

        "list" = List(Box<[Id]>),       // (list ...)

         "over" = Over([Id; 3]),

        // utilities
        "ref" = Ref(Id),                // (ref expr)

        // plans
        "scan" = Scan([Id; 3]), // (scan table [column..] filter)
        "value" = Values(Box<[Id]>),           // (values [expr..]..)
        "proj" = Proj([Id; 2]),                 // (proj [expr..] child)
        "limit" = Limit([Id; 3]),               // (limit limit offset child)
        "filter" = Filter([Id; 2]),             // (filter expr child)

        "order" = Order([Id; 2]),               // (order [order_key..] child)
            "asc" = Asc(Id),                        // (asc key)
            "desc" = Desc(Id),                      // (desc key)

        "agg" = Agg([Id; 3]),                   // (agg aggs=[expr..] group_keys=[expr..] child)

        // aggregations
        "count" = Count(Id),
        "sum" = Sum(Id),

        CreateTable(CreateTable),
        Drop(BoundDrop),
        "insert" = Insert([Id; 3]),             // (insert table [column..] child)
    }
}

impl Expr {

    pub const fn true_() -> Self {
        Self::Constant(DataValue::Bool(true))
    }

    pub const fn null() -> Self {
        Self::Constant(DataValue::Null)
    }

    pub const fn zero() -> Self {
        Self::Constant(DataValue::Int32(0))
    }

    pub fn as_list(&self) -> &[Id] {
        let Self::List(l) = self else { panic!("not a list: {self}") };
        l
    }

    pub fn as_table(&self) -> TableRefId {
        let Self::Table(t) = self else { panic!("not a table: {self}") };
        *t
    }

    pub fn as_column(&self) -> ColumnRefId {
        let Self::Column(c) = self else { panic!("not a column: {self}") };
        *c
    }

    pub const fn is_aggregate_function(&self) -> bool {
        use Expr::*;
        matches!(
            self,
             Sum(_) | Count(_)
        )
    }
}

/// Analysis used in binding and building executor.
#[derive(Default)]
pub struct TypeSchemaAnalysis {
    pub catalog: CatalogRef,
}

#[derive(Debug, Clone)]
pub struct TypeSchema {

    /// Data type of the expression.
    pub type_: type_::Type,

    /// The schema for plan node: a list of expressions.
    ///
    /// For non-plan node, it always be None.
    /// For plan node, it may be None if the schema is unknown due to unresolved `prune`.
    pub schema: schema::Schema,

    /// All aggragations in the tree.
    pub aggs: agg::AggSet,
}

impl Analysis<Expr> for TypeSchemaAnalysis{
    type Data = TypeSchema;

    fn make(egraph: &EGraph<Expr, Self>, enode: &Expr) -> Self::Data {
        TypeSchema {
            type_: type_::analyze_type(enode,
            |i| egraph[*i].data.type_.clone(),
            egraph.analysis.catalog.clone()),
            schema: schema::analyze_schema(enode, |i| egraph[*i].data.schema.clone()),
            aggs: agg::analyze_aggs(enode, |i| egraph[*i].data.aggs.clone()),
        }
    }

    fn merge(&mut self, to: &mut Self::Data, from: Self::Data) -> DidMerge {
        let merge_type = egg::merge_max(&mut to.type_, from.type_);
        let merge_schema = egg::merge_max(&mut to.schema, from.schema);
        let merge_aggs = egg::merge_max(&mut to.aggs, from.aggs);
        merge_type | merge_schema | merge_aggs
    }
}

/// Plan optimizer
pub struct Optimizer {
    catalog: CatalogRef,
    disable_rules: HashSet<String>
}

impl Optimizer {

    pub fn new(catalog: CatalogRef) -> Self {
        Self {
            catalog,
            disable_rules: HashSet::default(),
        }
    }


    pub fn optimize(&self, expr: &RecExpr) -> RecExpr {
        let expr = expr.clone();

        // todo, add rule
        expr.clone()
    }

}