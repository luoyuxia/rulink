use std::fmt::{Display, Formatter};
use egg::{Id, Language};
use crate::array::{ArrayBuilderImpl, ArrayImpl, DataChunk};
use crate::planner::{Expr, RecExpr};
use crate::types::{ConvertError, DataValue};



pub struct Evaluator<'a> {
    expr: &'a RecExpr,
    id: Id
}

impl Display for Evaluator<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let recexpr = self.node().build_recexpr(|id| self.expr[id].clone());
        write!(f, "{recexpr}")
    }
}

impl<'a> Evaluator<'a> {

    pub fn new(expr: &'a RecExpr) -> Self {
        Self {
            expr,
            id: Id::from(expr.as_ref().len() - 1),
        }
    }

    fn node(&self) -> &Expr {
        &self.expr[self.id]
    }

    fn next(&self, id: Id) -> Self {
        Self {
            expr: self.expr,
            id,
        }
    }

    pub fn eval(&self, chunk: &DataChunk) -> Result<ArrayImpl, ConvertError> {
        use Expr::*;
        match self.node() {
            Constant(v) => {
                let mut builder =
                    ArrayBuilderImpl::with_capacity(chunk.cardinality(), &v.datatype().unwrap());
                builder.push_n(chunk.cardinality(), v);
                Ok(builder.finish())
            },
            ColumnIndex(idx) => {
                Ok(chunk.array_at(idx.0 as _).clone())
            },
            Count(a) | Sum(a) => self.next(*a).eval(chunk),
            Asc(a) | Desc(a) | Ref(a) => self.next(*a).eval(chunk),
            _ => todo!("{}", self.node())
        }
    }

    pub fn eval_list(&self, chunk: &DataChunk) -> Result<DataChunk, ConvertError> {
        let list = self.node().as_list();
        if list.is_empty() {
            return Ok(DataChunk::no_column())
        }
        list.iter().map(|id| self.next(*id).eval(chunk)).collect()
    }

    /// Returns the initial aggregation states.
    pub fn init_agg_states<B: FromIterator<DataValue>>(&self) -> B {
        (self.node().as_list().iter())
            .map(|id| self.next(*id).init_agg_state())
            .collect()
    }

    /// Append a list of values to a list of agg states.
    pub fn agg_list_append(
        &self,
        states: &mut [DataValue],
        values: impl Iterator<Item = DataValue>,
    ) {
        let list = self.node().as_list();
        for ((state, id), value) in states.iter_mut().zip(list).zip(values) {
            *state = self.next(*id).agg_append(state.clone(), value);
        }
    }

    fn init_agg_state(&self) -> DataValue {
        use Expr::*;
        match self.node() {
            Over([window, _, _]) => self.next(*window).init_agg_state(),
            Count(_) => DataValue::Int32(0),
            Sum(_)  => DataValue::Null,
            t => panic!("not aggregation: {t}"),
        }
    }

    fn agg_append(&self, state: DataValue, value: DataValue) -> DataValue {
        use Expr::*;
        match self.node() {
            Count(_) => state.add(DataValue::Int32(!value.is_null() as _)),
            Sum(_) => state.add(value),
            t => panic!("not aggregation: {t}"),
        }
    }

}