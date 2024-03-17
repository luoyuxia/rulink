use egg::Id;
use itertools::Itertools;
use crate::catalog::CatalogRef;
use crate::planner::Expr;
use crate::types::{DataType, DataTypeKind};

/// The data type of type analysis.
pub type Type = Result<DataType, TypeError>;

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TypeError {
    #[error("type is not available for node {0:?}")]
    Unavailable(String),
    #[error("no function for {op}{operands:?}")]
    NoFunction { op: String, operands: Vec<DataTypeKind> },
    #[error("no cast {from} -> {to}")]
    NoCast { from: DataTypeKind, to: DataTypeKind },
}


pub fn analyze_type(enode: &Expr, x: impl Fn(&Id) -> Type, catalog: CatalogRef) -> Type {
    use Expr::*;

    let concat_struct = |t1: DataType, t2: DataType| match (t1.kind, t2.kind) {
        (DataTypeKind::Struct(l), DataTypeKind::Struct(r)) => {
            Ok(DataType::new(DataTypeKind::Struct(l.into_iter().chain(r).collect()), false))
        }
        _ => panic!("not struct type"),
    };

    match enode {
        Constant(v) => {
            Ok(v.datatype().unwrap())
        }
        Type(data_type) => {
            Ok(data_type.clone())
        }
        Column(col) => {
            Ok(catalog.get_column(col)
                .ok_or_else(|| TypeError::Unavailable(enode.to_string()))?
                .datatype())
        }
        List(list) => {
            let types = list.iter().map(x).try_collect()?;
            Ok(DataType::new(DataTypeKind::Struct(types), false))
        }
        Values(rows) => {
            if rows.is_empty() {
                return Ok(DataType::new(DataTypeKind::Null, false));
            }
            let mut type_ = x(&rows[0])?;
            for row in rows.iter().skip(1) {
                let ty = x(row)?;
                type_ = type_.union(&ty).ok_or(TypeError::NoCast {
                    from: ty.kind,
                    to: type_.kind,
                })?;
            }
            Ok(type_)
        },
        Sum(a) => check(enode, x(a)?, |a| a.is_number()),
        Agg([exprs, group_keys, _]) => concat_struct(x(exprs)?, x(group_keys)?),
        _ => Err(TypeError::Unavailable(enode.to_string())),
    }
}

fn check(enode: &Expr, a: DataType, check: impl FnOnce(DataTypeKind) -> bool) -> Type {
    if check(a.kind()) {
        Ok(a)
    } else {
        Err(TypeError::NoFunction {
            op: enode.to_string(),
            operands: vec![a.kind()],
        })
    }
}
