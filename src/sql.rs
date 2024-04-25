use crate::{api_error::ApiError, schema::Schema};
use alloy::hex;
use eyre::Result;
use sqlparser::{
    ast::{visit_expressions, visit_relations, Expr, Statement},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::ops::ControlFlow;

pub struct ParsedQuery(Vec<Statement>);

impl ParsedQuery {
    pub fn new(sql: &str) -> Result<Self> {
        Ok(ParsedQuery {
            0: Parser::parse_sql(&PostgreSqlDialect {}, sql)?,
        })
    }

    pub fn columns(&self) -> Result<Vec<String>> {
        let mut res = Vec::new();
        visit_expressions(&self.0, |expr| {
            match expr {
                Expr::Identifier(id) => res.push(id.value.clone()),
                Expr::CompoundIdentifier(id) => res.push(
                    id.iter()
                        .map(|part| part.value.to_string())
                        .collect::<Vec<String>>()
                        .join("."),
                ),
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
        Ok(res)
    }

    pub fn tables(&self) -> Result<Vec<String>, ApiError> {
        let mut res = Vec::new();
        visit_relations(&self.0, |relation| {
            res.push(relation.to_string());
            ControlFlow::<()>::Continue(())
        });
        Ok(res)
    }

    pub fn enhance(&self, schema: &Schema) -> Result<String, ApiError> {
        let mut res = String::new();
        let (tables, columns) = (self.tables()?, self.columns()?);
        for t in tables {
            res += &format!("with {} as (", t);
            res += "select ";
            for c in &columns {
                res += &schema.get_col_sql(&c)?;
            }
            res += &format!(
                r#" from records where expired_block_num is null and table_id = '\x{}') "#,
                hex::encode(schema.table_id)
            );
        }
        Ok((res + &self.0.first().unwrap().to_string()).to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::fixed_bytes;

    #[test]
    fn test_columns() {
        let pq = ParsedQuery::new("SELECT baz.a FROM baz where b = 1").unwrap();
        assert_eq!(pq.columns().unwrap(), vec!["baz.a", "b"]);
        assert_eq!(pq.tables().unwrap(), vec!["baz"]);
    }

    #[test]
    fn test_enhance() {
        let pq = ParsedQuery::new("SELECT value FROM counter").unwrap();
        assert_eq!(
            pq.enhance(
                &Schema {
                    table_id: fixed_bytes!("74620000000000000000000000000000436f756e746572000000000000000000"),
                    key_names: vec![],
                    val_names: vec![String::from("value")],
                    key_schema: fixed_bytes!(),
                    val_schema: fixed_bytes!("0004010003000000000000000000000000000000000000000000000000000000"),
                }
            )
            .unwrap(),
            "with counter as (select b2n(sdec(static_data, 0, 4)) as value from records where expired_block_num is null and table_id = '\\x74620000000000000000000000000000436f756e746572000000000000000000') select value from counter"
        )
    }
}
