use sqlparser::{
    ast::{visit_expressions, visit_relations, Expr},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::ops::ControlFlow;

pub fn unknown_table(sql: &str, allowed_tables: &[&str]) -> eyre::Result<bool> {
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    let mut unknown = false;
    visit_relations(&ast, |relation| {
        if !allowed_tables.iter().any(|t| *t == relation.to_string()) {
            unknown = true;
            ControlFlow::<()>::Break(())
        } else {
            ControlFlow::<()>::Continue(())
        }
    });
    Ok(unknown)
}

pub fn unknown_function(sql: &str, allowed_functions: &[&str]) -> eyre::Result<bool> {
    let ast = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
    let mut unknown = false;
    visit_expressions(&ast, |expr| {
        if let Expr::Function(f) = expr {
            if !allowed_functions.iter().any(|t| *t == f.name.to_string()) {
                unknown = true;
                return ControlFlow::<()>::Break(());
            }
        }
        ControlFlow::<()>::Continue(())
    });
    Ok(unknown)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unkown_tables() {
        let sql = "SELECT a FROM foo where x IN (SELECT y FROM bar)";
        assert!(unknown_table(sql, &[]).unwrap());
        assert!(!unknown_table(sql, &vec!["foo", "bar"]).unwrap());

        let sql = "
            SELECT foo.a, baz.b FROM foo, baz
            where x IN (SELECT y FROM bar) and foo.z = baz.z
        ";
        assert!(unknown_table(sql, &vec!["foo", "bar"]).unwrap());
        assert!(!unknown_table(sql, &vec!["foo", "bar", "baz"]).unwrap());
    }

    #[test]
    fn test_unkown_expressions() {
        let sql = "SELECT foo(a) FROM baz where bar(x) = 1";
        assert!(unknown_function(sql, &vec![]).unwrap());
        assert!(unknown_function(sql, &vec!["foo"]).unwrap());
        assert!(!unknown_function(sql, &vec!["foo", "bar"]).unwrap());
    }
}
