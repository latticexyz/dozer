use std::collections::HashMap;

use eyre::Result;
use sqlparser::{ast, dialect::PostgreSqlDialect, parser::Parser};

use crate::{api, mud_schema};

const PG: &PostgreSqlDialect = &PostgreSqlDialect {};

macro_rules! no {
    ($e:expr) => {
        Err(api::Error::User(format!("{} not supported", $e)))
    };
}

pub struct Validator<'a> {
    schemas: HashMap<&'a str, &'a mud_schema::Schema>,
}

impl<'a> Validator<'a> {
    pub fn validate(&self, query: &str) -> Result<(), api::Error> {
        let stmts = Parser::parse_sql(PG, query).map_err(|e| api::Error::User(e.to_string()))?;
        for stmt in stmts.iter() {
            match stmt {
                ast::Statement::Query(q) => self.validate_query(q),
                _ => Err(api::Error::User("select queries only".to_string())),
            }?;
        }
        Ok(())
    }

    fn validate_query(&self, query: &ast::Query) -> Result<(), api::Error> {
        match query {
            ast::Query { with: Some(_), .. } => no!("with"),
            ast::Query { locks, .. } if locks.len() > 0 => no!("for update"),
            ast::Query { body, .. } => self.validate_query_body(body),
        }
    }

    fn validate_query_body(&self, body: &ast::SetExpr) -> Result<(), api::Error> {
        match body {
            ast::SetExpr::Select(select_query) => self.validate_select(select_query),
            _ => no!("invalid query body"),
        }
    }

    fn validate_select(&self, select: &ast::Select) -> Result<(), api::Error> {
        match select {
            ast::Select { top: Some(_), .. } => no!("top"),
            ast::Select { into: Some(_), .. } => no!("into"),
            ast::Select {
                having: Some(_), ..
            } => no!("having"),
            ast::Select {
                qualify: Some(_), ..
            } => no!("qualify"),
            ast::Select {
                value_table_mode: Some(_),
                ..
            } => no!("value_table_mode"),
            ast::Select {
                lateral_views: l, ..
            } if l.len() > 0 => no!("lateral"),
            ast::Select {
                distribute_by: d, ..
            } if d.len() > 0 => no!("distribute_by"),
            ast::Select { cluster_by: d, .. } if d.len() > 0 => no!("cluster_by"),
            ast::Select {
                named_window: w, ..
            } if w.len() > 0 => no!("named_window"),
            ast::Select { from, .. } if from.len() == 0 => no!("empty tables"),
            ast::Select {
                distinct,
                projection,
                from,
                selection,
                group_by,
                sort_by,
                ..
            } => {
                if let Some(ast::Distinct::On(exprs)) = distinct {
                    self.validate_expressions(&exprs)?;
                }
                if let Some(expr) = selection {
                    self.validate_expression(&expr)?;
                }
                if let ast::GroupByExpr::Expressions(exprs) = group_by {
                    self.validate_expressions(&exprs)?;
                }
                for projection_item in projection.iter() {
                    match projection_item {
                        ast::SelectItem::UnnamedExpr(expr) => self.validate_expression(expr),
                        ast::SelectItem::ExprWithAlias { expr, alias: _ } => {
                            self.validate_expression(expr)
                        }
                        _ => no!(projection_item),
                    }?;
                }
                self.validate_expressions(&sort_by)?;
                for table_with_join in from {
                    self.validate_table(&table_with_join)?;
                }
                Ok(())
            }
        }
    }

    fn validate_expressions(&self, exprs: &[ast::Expr]) -> Result<(), api::Error> {
        for expr in exprs.iter() {
            self.validate_expression(expr)?;
        }
        Ok(())
    }

    fn validate_expression(&self, expr: &ast::Expr) -> Result<(), api::Error> {
        match expr {
            ast::Expr::Identifier(_) => Ok(()),
            ast::Expr::CompoundIdentifier(_) => Ok(()),
            ast::Expr::IsFalse(_) => Ok(()),
            ast::Expr::IsNotFalse(_) => Ok(()),
            ast::Expr::IsTrue(_) => Ok(()),
            ast::Expr::IsNotTrue(_) => Ok(()),
            ast::Expr::IsNull(_) => Ok(()),
            ast::Expr::IsNotNull(_) => Ok(()),
            ast::Expr::Ceil { expr, field: _ } => self.validate_expression(expr),
            ast::Expr::Floor { expr, field: _ } => self.validate_expression(expr),
            ast::Expr::Value(_) => Ok(()),
            ast::Expr::Exists { subquery, .. } => self.validate_query(subquery),
            ast::Expr::Subquery(subquery) => self.validate_query(subquery),
            ast::Expr::Tuple(exprs) => self.validate_expressions(exprs),
            _ => no!(expr),
        }
    }

    fn validate_table(&self, tbl_with_joins: &ast::TableWithJoins) -> Result<(), api::Error> {
        if !tbl_with_joins.joins.is_empty() {
            return no!("joins");
        }
        match &tbl_with_joins.relation {
            ast::TableFactor::Table { with_hints: h, .. } if h.len() > 0 => no!("with_hints"),
            ast::TableFactor::Table { args: Some(_), .. } => no!("args"),
            ast::TableFactor::Table {
                version: Some(_), ..
            } => no!("version"),
            ast::TableFactor::Table {
                name: ast::ObjectName(name_parts),
                ..
            } => {
                if name_parts.len() != 1 {
                    return Err(api::Error::User(format!(
                        "table {} has multiple parts; only unqualified table names supported",
                        tbl_with_joins.relation
                    )));
                }
                if !self
                    .schemas
                    .values()
                    .map(|s| s.table_name())
                    .collect::<Vec<String>>()
                    .contains(&name_parts[0].value.to_string())
                {
                    return no!(name_parts[0]);
                }
                return Ok(());
            }
            _ => no!(tbl_with_joins.relation),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_query(schema: &mud_schema::Schema, query: &str, want: Option<&str>) {
        let v = Validator {
            schemas: HashMap::from([("foo", schema)]),
        };
        match want {
            Some(msg) => match v.validate(query) {
                Ok(_) => {
                    panic!("wanted error got none")
                }
                Err(api::Error::User(e)) => {
                    assert_eq!(e.to_string(), msg)
                }
                Err(e) => panic!("unkown error: {:?}", e),
            },
            None => match v.validate(query) {
                Ok(_) => {}
                Err(e) => {
                    panic!("wanted no error got: {:?}", e)
                }
            },
        }
    }

    fn test_schema(table_name: &str) -> mud_schema::Schema {
        let mut s = mud_schema::Schema::default();
        s.set_name(table_name);
        s
    }

    #[test]
    fn test_unsupported_statements() {
        vec![
            (
                test_schema("foo"),
                "truncate foo",
                Some("select queries only"),
            ),
            (
                test_schema("foo"),
                "with foo as (select 1) select * from foo",
                Some("with not supported"),
            ),
            (
                test_schema("foo"),
                "select col from foo for update",
                Some("for update not supported"),
            ),
            (
                test_schema("foo"),
                "select foo",
                Some("empty tables not supported"),
            ),
            (test_schema("foo"), "select col from foo", None),
            (
                test_schema("foo"),
                "select col from bar",
                Some("bar not supported"),
            ),
        ]
        .iter()
        .for_each(|c| check_query(&c.0, c.1, c.2))
    }
}
