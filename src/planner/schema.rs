use egg::Id;
use super::*;

/// The data type of schema analysis.
pub type Schema = Vec<Id>;

/// Returns the output expressions for plan node.
pub fn analyze_schema(enode: &Expr, x: impl Fn(&Id) -> Schema) -> Schema {
    use Expr::*;
    let concat = |v1: Vec<Id>, v2: Vec<Id>| v1.into_iter().chain(v2.into_iter()).collect();
    match enode {
        // equal to child
        Filter([_, c]) | Order([_, c]) | Limit([_, _, c]) => x(c),

        // // concat 2 children
        // Join([_, _, l, r]) | HashJoin([_, _, _, l, r]) => concat(x(l), x(r)),

        // list is the source for the following nodes
        List(ids) => ids.to_vec(),
        Agg([exprs, group_keys, _]) => {
            concat(x(exprs), x(group_keys))
        }

        // plans that change schema
        Scan([_, columns, _]) => x(columns),
        Values(vs) => x(&vs[0]),
        Proj([exprs, _]) => x(exprs),
        // not plan node
        _ => vec![],
    }
}

