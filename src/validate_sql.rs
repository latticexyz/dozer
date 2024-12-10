use std::collections::{HashMap, HashSet};

use eyre::Result;
use itertools::Itertools;
use sqlparser::{ast, dialect::PostgreSqlDialect, parser::Parser};

use crate::{api, mud_schema};

const PG: &PostgreSqlDialect = &PostgreSqlDialect {};

macro_rules! no {
    ($e:expr) => {
        Err(api::Error::User(format!("{} not supported", $e)))
    };
}

struct Validator {
    pub schemas: HashMap<String, mud_schema::Schema>,
}

pub fn validate(
    query: &str,
    schemas: Vec<mud_schema::Schema>,
) -> Result<Vec<mud_schema::Schema>, api::Error> {
    let schemas = schemas
        .into_iter()
        .map(|mut s| {
            s.select_list = Some(HashSet::new());
            (s.full_name(), s)
        })
        .collect();
    let mut validator = Validator { schemas };
    validator.validate(query)?;
    Ok(validator.schemas.into_values().collect())
}

impl Validator {
    fn validate(&mut self, query: &str) -> Result<(), api::Error> {
        let stmts = Parser::parse_sql(PG, query).map_err(|e| api::Error::User(e.to_string()))?;
        for stmt in stmts.iter() {
            match stmt {
                ast::Statement::Query(q) => self.validate_query(q),
                _ => Err(api::Error::User("select queries only".to_string())),
            }?;
        }
        Ok(())
    }

    fn validate_query(&mut self, query: &ast::Query) -> Result<(), api::Error> {
        match query {
            ast::Query { with: Some(_), .. } => no!("with"),
            ast::Query { locks, .. } if !locks.is_empty() => no!("for update"),
            ast::Query { body, .. } => self.validate_query_body(body),
        }
    }

    fn validate_query_body(&mut self, body: &ast::SetExpr) -> Result<(), api::Error> {
        match body {
            ast::SetExpr::Select(select_query) => self.validate_select(select_query),
            _ => no!("invalid query body"),
        }
    }

    fn validate_select(&mut self, select: &ast::Select) -> Result<(), api::Error> {
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
            } if !l.is_empty() => no!("lateral"),
            ast::Select {
                distribute_by: d, ..
            } if !d.is_empty() => no!("distribute_by"),
            ast::Select { cluster_by: d, .. } if !d.is_empty() => no!("cluster_by"),
            ast::Select {
                named_window: w, ..
            } if !w.is_empty() => no!("named_window"),
            ast::Select { from, .. } if from.is_empty() => no!("empty tables"),
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
                    self.validate_expressions(exprs)?;
                }
                if let Some(expr) = selection {
                    self.validate_expression(expr)?;
                }
                if let ast::GroupByExpr::Expressions(exprs) = group_by {
                    self.validate_expressions(exprs)?;
                }
                for projection_item in projection.iter() {
                    match projection_item {
                        ast::SelectItem::UnnamedExpr(expr) => self.validate_expression(expr),
                        ast::SelectItem::ExprWithAlias { expr, alias: _ } => {
                            self.validate_expression(expr)
                        }
                        _ => {
                            no!(projection_item)
                        }
                    }?;
                }
                self.validate_expressions(sort_by)?;
                for table_with_join in from {
                    self.validate_table(table_with_join)?;
                }
                Ok(())
            }
        }
    }

    fn validate_expressions(&mut self, exprs: &[ast::Expr]) -> Result<(), api::Error> {
        for expr in exprs.iter() {
            self.validate_expression(expr)?;
        }
        Ok(())
    }

    fn validate_expression(&mut self, expr: &ast::Expr) -> Result<(), api::Error> {
        match expr {
            ast::Expr::Identifier(id) => self.validate_column(id),
            ast::Expr::CompoundIdentifier(ids) => self.validate_compound_column(ids),
            ast::Expr::IsFalse(_) => Ok(()),
            ast::Expr::IsNotFalse(_) => Ok(()),
            ast::Expr::IsTrue(_) => Ok(()),
            ast::Expr::IsNotTrue(_) => Ok(()),
            ast::Expr::IsNull(_) => Ok(()),
            ast::Expr::IsNotNull(_) => Ok(()),
            ast::Expr::UnaryOp { .. } => Ok(()),
            ast::Expr::Function(function) => self.validate_function(function),
            ast::Expr::Ceil { expr, field: _ } => self.validate_expression(expr),
            ast::Expr::Floor { expr, field: _ } => self.validate_expression(expr),
            ast::Expr::Value(_) => Ok(()),
            ast::Expr::Exists { subquery, .. } => self.validate_query(subquery),
            ast::Expr::Subquery(subquery) => self.validate_query(subquery),
            ast::Expr::Tuple(exprs) => self.validate_expressions(exprs),
            ast::Expr::BinaryOp { left, right, .. } => {
                self.validate_expression(left)?;
                self.validate_expression(right)
            }
            ast::Expr::InList { expr, list, .. } => {
                for e in list {
                    self.validate_expression(e)?;
                }
                self.validate_expression(expr)
            }
            _ => no!(expr),
        }
    }

    fn validate_compound_column(&mut self, id: &[ast::Ident]) -> Result<(), api::Error> {
        let (table_name, col_name) = match id.len() {
            3 => (id[0..2].iter().join("."), id[2].to_string()),
            2 => (id[0].to_string(), id[1].to_string()),
            _ => {
                return Err(api::Error::User(format!(
                    "compound column id must be of form: table.column got: {}",
                    id.iter().join(" ")
                )))
            }
        };
        match self.schemas.get_mut(&table_name) {
            Some(schema) => {
                if schema.has_column(&col_name) {
                    schema
                        .select_list
                        .as_mut()
                        .map(|sl| Some(sl.insert(col_name)));
                    Ok(())
                } else {
                    Err(api::Error::User(format!(
                        "column {} not defined in table {}",
                        col_name, table_name,
                    )))
                }
            }
            None => Err(api::Error::User(format!(
                "table {} not defined in query",
                table_name
            ))),
        }
    }

    fn validate_column(&mut self, id: &ast::Ident) -> Result<(), api::Error> {
        let matched_schemas: Vec<&mud_schema::Schema> = self
            .schemas
            .values()
            .filter(|s| s.has_column(&id.value))
            .collect();
        match matched_schemas.len() {
            1 => {
                let qname = matched_schemas.first().unwrap().full_name();
                if let Some(schema) = self.schemas.get_mut(&qname) {
                    schema
                        .select_list
                        .as_mut()
                        .map(|sl| Some(sl.insert(id.to_string())));
                }
                Ok(())
            }
            0 => Err(api::Error::User(format!(
                "column {} not found in {}",
                id.value,
                self.schemas.values().map(|s| s.full_name()).join(","),
            ))),
            _ => Err(api::Error::User(format!(
                "{} references more than one table: {}",
                id.value,
                matched_schemas
                    .iter()
                    .map(|s| s.full_name())
                    .sorted()
                    .join(","),
            ))),
        }
    }

    fn validate_table(&self, tbl_with_joins: &ast::TableWithJoins) -> Result<(), api::Error> {
        if !tbl_with_joins.joins.is_empty() {
            return no!("joins");
        }
        match &tbl_with_joins.relation {
            ast::TableFactor::Table { with_hints: h, .. } if !h.is_empty() => no!("with_hints"),
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
                let name = name_parts[0].value.to_string();
                if !self.schemas.values().any(|s| s.has_name(&name)) {
                    return Err(api::Error::User(format!(
                        "no schema found for table: {}",
                        name_parts[0],
                    )));
                }
                Ok(())
            }
            _ => no!(tbl_with_joins.relation),
        }
    }

    fn validate_function(&mut self, function: &ast::Function) -> Result<(), api::Error> {
        let name = function.name.to_string();
        const VALID_FUNCS: [&str; 30] = [
            "decode",
            // Aggregate Functions
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "array_agg",
            "string_agg",
            // String Functions
            "concat",
            "concat_ws",
            "lower",
            "upper",
            "trim",
            "replace",
            "substring",
            "length",
            "split_part",
            // Date/Time Functions
            "now",
            "current_timestamp",
            "current_date",
            "date_trunc",
            "extract",
            "to_timestamp",
            "age",
            "date_part",
            // Type Conversion/Null Handling
            "cast",
            "to_char",
            "coalesce",
            "nullif",
            // Math Functions
            "round",
        ];

        if !VALID_FUNCS.contains(&name.as_str()) {
            return no!(format!("function {}", name));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_query(schemas: Vec<mud_schema::Schema>, query: &str, want: Option<&str>) {
        let mut v = Validator {
            schemas: schemas.into_iter().map(|s| (s.full_name(), s)).collect(),
        };
        match want {
            Some(msg) => match v.validate(query) {
                Ok(_) => {
                    panic!("query: {}\n wanted error got none", query)
                }
                Err(api::Error::User(e)) => {
                    if e != msg {
                        panic!("query: {}\n unkown want: {:?} got: {:?}", query, msg, e);
                    }
                }
                Err(e) => panic!("query: {}\n unkown error: {:?}", query, e),
            },
            None => match v.validate(query) {
                Ok(_) => {}
                Err(e) => {
                    panic!("query: {}\n wanted no error got: {:?}", query, e)
                }
            },
        }
    }

    fn test_schema(table_name: &str, cols: Vec<&str>) -> mud_schema::Schema {
        let mut s = mud_schema::Schema::default();

        let mut name = table_name.as_bytes().to_vec();
        name.truncate(16);
        name.resize(16, 0);
        s.table_id[16..].copy_from_slice(&name);

        s.val_names.extend(cols.into_iter().map(|s| s.to_string()));
        s
    }

    #[test]
    fn test_select_list() {
        let schemas = validate("select c from foo", vec![test_schema("foo", vec!["c"])])
            .expect("validating query");
        assert_eq!(schemas.len(), 1);

        let select_list = schemas[0]
            .select_list
            .as_ref()
            .expect("no select list")
            .iter()
            .collect_vec();
        assert_eq!(select_list, vec!["c"]);
    }

    #[test]
    fn test_supported_statements() {
        vec![
            (
                vec![test_schema("foo", vec!["c"])],
                "select c from foo",
                None,
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                r#"select "c" from foo"#,
                None,
            ),
            (
                vec![test_schema("foo", vec!["c"]), test_schema("bar", vec!["c"])],
                r#"select foo.c, bar."c" from foo, bar where foo."c" = bar.c"#,
                None,
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select c from foo where c in ('foo', 'bar')",
                None,
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select c from foo where c = 42 or c = -42",
                None,
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select count(*) from foo",
                None,
            ),
            (
                vec![test_schema("foo", vec!["c"]), test_schema("bar", vec!["c"])],
                "select foo.c, bar.c from foo, bar where foo.c = bar.c",
                None,
            ),
            (
                vec![test_schema("foo", vec!["exists"])],
                "select \"exists\" from foo",
                None,
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select count(*) from foo",
                None,
            ),
        ]
        .into_iter()
        .for_each(|c| check_query(c.0, c.1, c.2))
    }

    #[test]
    fn test_unsupported_statements() {
        vec![
            (
                vec![test_schema("foo", vec![])],
                "truncate foo",
                Some("select queries only"),
            ),
            (
                vec![test_schema("foo", vec![])],
                "with foo as (select 1) select * from foo",
                Some("with not supported"),
            ),
            (
                vec![test_schema("foo", vec![])],
                "select col from foo for update",
                Some("for update not supported"),
            ),
            (
                vec![test_schema("foo", vec![])],
                "select bar",
                Some("empty tables not supported"),
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select c from bar",
                Some("no schema found for table: bar"),
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select d from foo",
                Some("column d not found in foo"),
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select c from bar",
                Some("no schema found for table: bar"),
            ),
            (
                vec![test_schema("foo", vec!["c"]), test_schema("bar", vec!["c"])],
                "select c from foo, bar",
                Some("c references more than one table: bar,foo"),
            ),
            (
                vec![test_schema("foo", vec!["c"]), test_schema("bar", vec!["c"])],
                "select foo.c, bar.c, baz.d from foo, bar",
                Some("table baz not defined in query"),
            ),
            (
                vec![test_schema("foo", vec!["c"])],
                "select any('123') from foo",
                Some("function any not supported"),
            ),
        ]
        .into_iter()
        .for_each(|c| check_query(c.0, c.1, c.2))
    }
}
