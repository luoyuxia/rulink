use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr};
use crate::binder::Binder;

use crate::binder::{Result, Node};


impl Binder {
    pub fn bind_exprs(&mut self, exprs: Vec<Expr>) -> Result {
        let list = exprs
            .into_iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;
        Ok(self.egraph.add(Node::List(list)))
    }

    pub fn bind_expr(&mut self, expr: Expr) -> Result {
        let id = match expr {
            Expr::Value(v) => Ok(self.egraph.add(Node::Constant((&v).into()))),
            Expr::Identifier(ident) =>
                self.bind_colum_ref(std::slice::from_ref(&ident)),
            Expr::CompoundIdentifier(idents) =>
                self.bind_colum_ref(&idents),
            Expr::Function(func) =>
                self.bind_function(func),
            _ => todo!("bind expression: {:?}", expr),
        }?;
        self.check_type(id)?;
        Ok(id)
    }

    fn bind_function(&mut self, func: Function) -> Result {
        let mut args = vec![];
        for arg in func.args {
            // ignore argument name
            let arg = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg {
                FunctionArgExpr::Expr(expr) => args.push(self.bind_expr(expr)?),
                FunctionArgExpr::Wildcard => {
                    // No argument in row count
                    args.clear();
                    break;
                }
                FunctionArgExpr::QualifiedWildcard(_) => todo!("support qualified wildcard"),
            }
        }

        let node = match func.name.to_string().to_lowercase().as_str() {
            "count" => Node::Count(args[0]),
            "sum" => Node::Sum(args[0]),
            name => todo!("Unsupported function: {}", name),
        };
        let id = self.egraph.add(node);
        Ok(id)
    }
}
