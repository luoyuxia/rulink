use egg::Id;
use itertools::Itertools;
use crate::binder::{Binder, BinderContext, BindError};
use crate::planner::{Expr as Node};

use egg::Language;
use sqlparser::ast::{Expr, OrderByExpr, Query, Select, SelectItem, SetExpr, TableWithJoins, Values};
use super::*;


impl Binder {
    pub fn bind_query(&mut self, query: Query) -> Result<(Id, BinderContext)> {
        self.contexts.push(BinderContext::default());
        let ret = self.bind_query_internal(query);
        let ctx = self.contexts.pop().unwrap();
        ret.map(|id| (id, ctx))
    }

    pub(in crate::binder) fn bind_query_internal(&mut self, query: Query) -> Result {
        let child = match *query.body {
            SetExpr::Select(select) => self.bind_insert_select_from(*select, query.order_by)?,
            SetExpr::Values(values) => self.bind_values(values)?,
            _ => todo!("handle query: "),
        };
        let limit = match query.limit {
            None => self.egraph.add(Node::null()),
            Some(expr) =>  self.bind_expr(expr)?
        };
        let offset = match query.offset {
            None => self.egraph.add(Node::zero()),
            Some(offset) =>  self.bind_expr(offset.value)?,
        };
        Ok(self.egraph.add(Node::Limit([limit, offset, child])))
    }

    pub fn bind_insert_select_from(
        &mut self,
        select_stmt: Select, order_by: Vec<OrderByExpr>) -> Result {
        let from = self.bind_from(select_stmt.from)?;
        let proj = self.bind_proj(select_stmt.projection, from)?;
        let where_ = self.bind_where(select_stmt.selection)?;
        let groupby = self.bind_groupby(select_stmt.group_by)?;
        let having = self.bind_having(select_stmt.having)?;
        let orderby = self.bind_orderby(order_by)?;
        let distinct = match select_stmt.distinct {
            // TODO: distinct on
            true => proj,
            false => self.egraph.add(Node::List([].into())),
        };

        let mut plan = self.egraph.add(Node::Filter([where_, from]));
        let mut to_rewrite = [proj, distinct, having, orderby];
        plan = self.plan_agg(&mut to_rewrite, groupby, plan)?;
        let [proj, distinct, having, orderby] = to_rewrite;

        plan = self.egraph.add(Node::Proj([proj, plan]));
        Ok(plan)
    }


    pub(in crate::binder) fn bind_where(&mut self, selection: Option<Expr>) -> Result {
        let id = self.bind_selection(selection)?;
        // todo!("check where")
        Ok(id)
    }

    pub(in crate::binder) fn bind_orderby(&mut self, order_by: Vec<OrderByExpr>) -> Result {
        let mut orderby = Vec::with_capacity(order_by.len());
        for e in order_by {
            let expr = self.bind_expr(e.expr)?;
            let key = self.egraph.add(match e.asc {
                Some(true) | None => Node::Asc(expr),
                Some(false) => Node::Desc(expr),
            });
            orderby.push(key);
        }
        Ok(self.egraph.add(Node::List(orderby.into())))
    }

    fn bind_having(&mut self, selection: Option<Expr>) -> Result {
        let id = self.bind_selection(selection)?;
        // todo!("check having")
        Ok(id)
    }


    fn bind_selection(&mut self, selection: Option<Expr>) -> Result {
        Ok(match selection {
            Some(expr) => self.bind_expr(expr)?,
            None => self.egraph.add(Node::true_()),
        })
    }



    pub fn bind_from(&mut self, tables: Vec<TableWithJoins>) -> Result {
        let node = None;
        for table in tables {
            let table_node = self.bind_table_with_joins(table)?;
            if let Some(node) = node {
                unimplemented!("Not support multiple tables")
            } else {
                return Ok(table_node);
            }
        }

        if let Some(node) = node {
            Ok(node)
        } else {
            unimplemented!()
        }
    }

    pub fn bind_proj(&mut self, projection: Vec<SelectItem>, from: Id) -> Result {
        let mut select_list = vec![];
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let ident = if let Expr::Identifier(ident) = &expr {
                        Some(ident.value.clone())
                    } else {
                        None
                    };
                    let id = self.bind_expr(expr)?;
                    if let Some(ident) = ident {
                        self.current_ctx_mut().output_aliases.insert(ident, id);
                    }
                    select_list.push(id)
                },
                SelectItem::Wildcard(_) => {
                    select_list.append(&mut self.schema(from))
                },
                _ => todo!("bind select list"),
            }
        }
        Ok(self.egraph.add(Node::List(select_list.into())))
    }

    pub fn bind_groupby(&mut self, group_by: Vec<Expr>) -> Result {
        let id = self.bind_exprs(group_by)?;
        Ok(id)
    }

    /// Rewrites the expression `id` with aggs wrapped in a [`Ref`](Node::Ref) node.
    /// Returns the new expression.
    ///
    /// # Example
    /// ```text
    /// id:         (+ (sum a) (+ b 1))
    /// schema:     (sum a), (+ b 1)
    /// output:     (+ (ref (sum a)) (ref (+ b 1)))
    ///
    /// so that `id` won't be optimized to:
    ///             (+ b (+ (sum a) 1))
    /// which can not be composed by `schema`
    /// ```
    pub(in crate::binder) fn rewrite_agg_in_expr(&mut self, id: Id, schema: &[Id]) -> Result {
        let mut expr = self.node(id).clone();
        if schema.contains(&id) {
            // found agg, wrap it with Ref
            return Ok(self.egraph.add(Node::Ref(id)));
        }
        if let Node::Column(cid) = &expr {
            let name = self.catalog.get_column(cid).unwrap().name().to_string();
            return Err(BindError::ColumnNotInAgg(name));
        }
        for child in expr.children_mut() {
            *child = self.rewrite_agg_in_expr(*child, schema)?;
        }
        Ok(self.egraph.add(expr))
    }


    fn plan_agg(&mut self, exprs: &mut [Id], groupby: Id, plan: Id) -> Result {
        let expr_list = self.egraph.add(Node::List(exprs.to_vec().into()));
        let aggs = self.aggs(expr_list).to_vec();
        if aggs.is_empty() && self.node(groupby).as_list().is_empty() {
            return Ok(plan);
        }
        let mut list: Vec<_> = aggs.into_iter().map(|agg| self.egraph.add(agg)).collect();
        // make sure the order of the aggs is deterministic
        list.sort();
        list.dedup();
        let aggs = self.egraph.add(Node::List(list.into()));
        let plan = self.egraph.add(Node::Agg([aggs, groupby, plan]));

        // check for not aggregated columns
        // rewrite the expressions with a wrapper over agg or group keys
        let schema = self.schema(plan);
        for id in exprs {
            *id = self.rewrite_agg_in_expr(*id, &schema)?;
        }
        Ok(plan)
    }

    pub(in crate::binder) fn bind_values(&mut self, values: Values) -> Result {
        let values = values.rows;
        let mut bound_values = Vec::with_capacity(values.len());
        if values.is_empty() {
            return Ok(self.egraph.add(Node::Values([].into())));
        }

        let column_len = values[0].len();
        for row in values {
            if row.len() != column_len {
                return Err(BindError::InvalidExpression(
                    "VALUES lists must all be the same length".into(),
                ));
            }
            bound_values.push(self.bind_exprs(row)?);
        }

        let id = self.egraph.add(Node::Values(bound_values.into()));
        self.check_type(id)?;
        Ok(id)
    }
}