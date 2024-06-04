use crate::{api_error::ApiError, schema::Schema};
use alloy::{hex, primitives::FixedBytes};
use eyre::{Context, Result};
use itertools::Itertools;
use sqlparser::{
    ast::{visit_expressions, visit_relations, Expr},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

#[derive(Debug)]
pub struct QueryItem {
    schema: Schema,
    selected: HashSet<String>,
}

pub async fn schemas(
    address: FixedBytes<20>,
    query: String,
    pg: &tokio_postgres::Client,
) -> Result<HashMap<String, QueryItem>, ApiError> {
    let query = Parser::parse_sql(&PostgreSqlDialect {}, &query).wrap_err("parsing query")?;
    let mut tables = HashSet::new();
    visit_relations(&query, |relation| {
        tables.insert(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    Ok(Schema::from_pg(pg, address, tables.into_iter().collect())
        .await?
        .into_iter()
        .map(|s| {
            (
                s.table_name(),
                QueryItem {
                    schema: s,
                    selected: HashSet::new(),
                },
            )
        })
        .collect::<HashMap<String, QueryItem>>())
}

pub fn enhance(
    address: FixedBytes<20>,
    schemas: &mut HashMap<String, QueryItem>,
    query: String,
) -> Result<String, ApiError> {
    let query = Parser::parse_sql(&PostgreSqlDialect {}, &query).wrap_err("parsing query")?;
    let mut tables = HashSet::new();
    visit_relations(&query, |relation| {
        tables.insert(relation.to_string());
        ControlFlow::<()>::Continue(())
    });
    visit_expressions(&query, |expr| {
        match expr {
            Expr::Identifier(id) => {
                schemas
                    .values_mut()
                    .next()
                    .expect(&format!("missing schema for {:?}", id))
                    .selected
                    .insert(id.to_string());
            }
            Expr::CompoundIdentifier(id) => {
                schemas
                    .get_mut(&id[0].to_string())
                    .expect(&format!("missing schema for {:?}", id))
                    .selected
                    .insert(id[1].to_string());
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    let mut res = String::from("with ");
    res += &schemas
        .values()
        .into_iter()
        .sorted_by_key(|s| s.schema.table_name())
        .map(|s| {
            let mut inner = String::new();
            inner += &format!("{} as (", s.schema.table_name());
            inner += "select ";
            inner += &s
                .selected
                .iter()
                .sorted()
                .filter_map(|col_name| s.schema.get_col_sql(&col_name))
                .collect::<Vec<String>>()
                .join(",");
            inner += &format!(
                r#" from records where address = '\x{}' and table_id = '\x{}' and not expired and not deleted) "#,
                hex::encode(address),
                hex::encode(s.schema.table_id),
            );
            Ok(inner)
        })
        .collect::<Result<Vec<String>>>()?
        .join(",");
    Ok((res + &query.first().unwrap().to_string()).to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::fixed_bytes;

    #[test]
    fn test_enhance() {
        let pq = enhance(
            FixedBytes::<20>::ZERO,
            &mut HashMap::from([
                (
                    String::from("foo"),
                    QueryItem {
                        selected: HashSet::new(),
                        schema: Schema {
                            address: fixed_bytes!(),
                            table_id: fixed_bytes!(
                                "74620000000000000000000000000000666f6f00000000000000000000000000"
                            ),
                            key_names: vec![],
                            val_names: vec![String::from("value")],
                            key_schema: fixed_bytes!(),
                            val_schema: fixed_bytes!(
                                "0004010003000000000000000000000000000000000000000000000000000000"
                            ),
                        },
                    },
                ),
                (
                    String::from("bar"),
                    QueryItem {
                        selected: HashSet::new(),
                        schema: Schema {
                            address: fixed_bytes!(),
                            table_id: fixed_bytes!(
                                "7462000000000000000000000000000062617200000000000000000000000000"
                            ),
                            key_names: vec![],
                            val_names: vec![String::from("value")],
                            key_schema: fixed_bytes!(),
                            val_schema: fixed_bytes!(
                                "0004010003000000000000000000000000000000000000000000000000000000"
                            ),
                        },
                    },
                ),
            ]),
            String::from("select foo.value, bar.value from foo, bar where foo.value = bar.value"),
        );
        assert_eq!(
            pq.unwrap(),
            "with bar as (select b2n(sdec(static_data, 0, 4)) as value from records where address = '\\x0000000000000000000000000000000000000000' and table_id = '\\x7462000000000000000000000000000062617200000000000000000000000000' and not expired and not deleted) ,foo as (select b2n(sdec(static_data, 0, 4)) as value from records where address = '\\x0000000000000000000000000000000000000000' and table_id = '\\x74620000000000000000000000000000666f6f00000000000000000000000000' and not expired and not deleted) select foo.value, bar.value from foo, bar where foo.value = bar.value"
        )
    }
}
