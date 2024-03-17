use egg::Language;
use super::*;

/// The data type of aggragation analysis.
pub type AggSet = Vec<Expr>;


pub fn analyze_aggs(enode: &Expr, x: impl Fn(&Id) -> AggSet) -> AggSet {
    use Expr::*;
    match enode {
        _ if enode.is_aggregate_function() => vec![enode.clone()],
        Over(_) | Ref(_) => vec![],
        // merge the set from all children
        _ => enode.children().iter().flat_map(x).collect(),
    }
}