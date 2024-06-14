use crate::api;

use alloy::{
    hex,
    primitives::{b256, FixedBytes, B256},
    sol,
    sol_types::SolType,
};
use eyre::{Result, WrapErr};
use itertools::Itertools;
use ruint::aliases::U64;
use serde::Serialize;
use tokio_postgres::{Client, Row, Transaction};

use crate::mud_encoding;

pub mod query {
    use crate::api;

    use alloy::primitives::FixedBytes;
    use eyre::Result;
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

    use super::Schema;

    const PG: &PostgreSqlDialect = &PostgreSqlDialect {};

    pub async fn enhance(
        pg: &tokio_postgres::Client,
        address: FixedBytes<20>,
        user_query: &str,
    ) -> Result<String, api::Error> {
        let parsed_query =
            Parser::parse_sql(PG, user_query).map_err(|e| api::Error::User(e.to_string()))?;
        let mut schemas = load_schemas(pg, address, &parsed_query).await?;
        if schemas.len() == 0 {
            return Err(api::Error::User("no tables found in query".to_string()));
        }
        build_sql(user_query, &parsed_query, &mut schemas)
    }

    struct SelectItem {
        schema: Schema,
        columns: HashSet<String>,
    }

    type Schemas = HashMap<String, SelectItem>;

    async fn load_schemas(
        pg: &tokio_postgres::Client,
        address: FixedBytes<20>,
        query: &Vec<sqlparser::ast::Statement>,
    ) -> Result<Schemas> {
        let mut table_names = HashSet::new();
        visit_relations(query, |relation| {
            table_names.insert(relation.to_string());
            ControlFlow::<()>::Continue(())
        });
        Ok(
            Schema::from_pg(pg, address, table_names.into_iter().collect())
                .await?
                .into_iter()
                .map(|s| {
                    (
                        s.table_name(),
                        SelectItem {
                            schema: s,
                            columns: HashSet::new(),
                        },
                    )
                })
                .collect(),
        )
    }

    fn build_sql(
        user_query: &str,
        parsed_query: &Vec<sqlparser::ast::Statement>,
        schemas: &mut Schemas,
    ) -> Result<String, api::Error> {
        let col_search = visit_expressions(parsed_query, |expr| match expr {
            Expr::Identifier(id) => match schemas.values_mut().next() {
                Some(s) => {
                    s.columns.insert(id.to_string());
                    ControlFlow::Continue(())
                }
                None => ControlFlow::Break(api::Error::User(format!(
                    "no schemas found for {}",
                    id.to_string()
                ))),
            },
            Expr::CompoundIdentifier(id) => match schemas.get_mut(&id[0].to_string()) {
                Some(s) => {
                    s.columns.insert(id[1].to_string());
                    ControlFlow::Continue(())
                }
                None => ControlFlow::Break(api::Error::User(format!(
                    "no schemas found for {}",
                    id[0].to_string()
                ))),
            },
            _ => ControlFlow::Continue(()),
        });
        if let ControlFlow::Break(err) = col_search {
            return Err(err);
        }
        let mut query = Vec::new();
        query.push("with".to_string());
        query.push(
            schemas
                .values()
                .sorted_by_key(|s| s.schema.table_name())
                .map(|s| {
                    s.schema
                        .cte_sql(s.columns.clone().into_iter().collect_vec())
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(","),
        );
        query.push(user_query.to_string());
        Ok(query.join(" "))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use alloy::primitives::fixed_bytes;

        fn fmt_sql(sql: &str) -> Result<String> {
            let ast = Parser::parse_sql(PG, sql)?;
            Ok(ast[0].to_string())
        }

        #[test]
        fn test_enhance() {
            let user_query = String::from(
                "select foo.value, bar.value from foo, bar where foo.value = bar.value",
            );
            let pq = build_sql(
                &user_query,
                &Parser::parse_sql(PG, &user_query).unwrap(),
                &mut HashMap::from([
                    (
                        String::from("foo"),
                        SelectItem {
                            columns: HashSet::new(),
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
                        SelectItem {
                            columns: HashSet::new(),
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
            );
            assert_eq!(
                fmt_sql(&pq.unwrap()).expect("parsing generated sql"),
                fmt_sql(r#"
                    with bar as (
                        select b2n(sdec(static_data, 0, 4)) as value
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x7462000000000000000000000000000062617200000000000000000000000000'
                        and not expired
                        and not deleted
                    ) ,foo as (
                        select b2n(sdec(static_data, 0, 4)) as value
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x74620000000000000000000000000000666f6f00000000000000000000000000'
                        and not expired
                        and not deleted
                    ) select foo.value, bar.value from foo, bar where foo.value = bar.value
                "#).unwrap()
        )
        }
    }
}

mod field {
    #[derive(Debug, PartialEq)]
    pub enum Kind {
        Static(Static),
        Dynamic(Dynamic),
    }
    #[derive(Debug, PartialEq)]
    pub enum Static {
        Numeric(usize),
        Bytea(usize),
    }
    #[derive(Debug, PartialEq)]
    pub enum Dynamic {
        Bytea,
        Text,
        Array(Static),
    }
    impl Kind {
        pub fn from_schema_type(t: u8) -> Option<Self> {
            Some(match t {
                n if t < 32 => Kind::Static(Static::Numeric(n as usize + 1)),
                n if t < 64 => Kind::Static(Static::Numeric(n as usize - 31)),
                n if t < 96 => Kind::Static(Static::Bytea(n as usize - 63)),
                _ if t == 96 => Kind::Static(Static::Bytea(1)),
                _ if t == 97 => Kind::Static(Static::Bytea(20)),
                n if t < 130 => Kind::Dynamic(Dynamic::Array(Static::Numeric(n as usize - 97))),
                n if t < 162 => Kind::Dynamic(Dynamic::Array(Static::Numeric(n as usize - 129))),
                n if t < 194 => Kind::Dynamic(Dynamic::Array(Static::Bytea(n as usize - 161))),
                _ if t == 194 => Kind::Dynamic(Dynamic::Array(Static::Bytea(1))),
                _ if t == 195 => Kind::Dynamic(Dynamic::Array(Static::Bytea(20))),
                _ if t == 196 => Kind::Dynamic(Dynamic::Bytea),
                _ if t == 197 => Kind::Dynamic(Dynamic::Text),
                _ => return None,
            })
        }

        pub fn size(&self) -> Option<usize> {
            match self {
                Kind::Static(Static::Bytea(s)) | Kind::Static(Static::Numeric(s)) => Some(*s),
                Kind::Dynamic(_) => None,
            }
        }

        pub fn to_sql(&self, pos: usize, name: &str) -> String {
            match self {
                Kind::Static(t) => match t {
                    Static::Numeric(size) => {
                        format!("b2n(sdec(static_data, {}, {})) as {}", pos, size, name)
                    }
                    Static::Bytea(size) => {
                        format!("sdec(static_data, {}, {}) as {}", pos, size, name)
                    }
                },
                Kind::Dynamic(t) => match t {
                    Dynamic::Bytea => {
                        format!("ddec(encoded_lengths, dynamic_data, {}) as {}", pos, name)
                    }
                    Dynamic::Text => {
                        format!(
                            r#"convert_from(rtrim(ddec(encoded_lengths, dynamic_data, {}), '\x00'), 'UTF8') as {}"#,
                            pos, name
                        )
                    }
                    Dynamic::Array(it) => match it {
                        Static::Bytea(size) => {
                            format!(
                                "b2ab(ddec(encoded_lengths, dynamic_data, {}), {}) as {}",
                                pos, size, name
                            )
                        }
                        Static::Numeric(size) => {
                            format!(
                                "b2an(ddec(encoded_lengths, dynamic_data, {}), {}) as {}",
                                pos, size, name
                            )
                        }
                    },
                },
            }
        }
    }
    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn test_from_schema_type() {
            assert_eq!(
                Kind::Static(Static::Numeric(32)),
                Kind::from_schema_type(0x1F).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Numeric(32)),
                Kind::from_schema_type(0x3f).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Bytea(32)),
                Kind::from_schema_type(0x5f).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Bytea(1)),
                Kind::from_schema_type(0x60).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Bytea(20)),
                Kind::from_schema_type(0x61).unwrap()
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Numeric(32))),
                Kind::from_schema_type(0x81).unwrap()
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Numeric(32))),
                Kind::from_schema_type(0xA1).unwrap()
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Bytea(32))),
                Kind::from_schema_type(0xC1).unwrap()
            );
        }
        #[test]
        fn test_to_sql() {
            assert_eq!(
                Kind::Static(Static::Numeric(32)).to_sql(1, "foo"),
                "b2n(sdec(static_data, 1, 32)) as foo"
            );
            assert_eq!(
                Kind::Static(Static::Bytea(32)).to_sql(1, "foo"),
                "sdec(static_data, 1, 32) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Bytea(32))).to_sql(0, "foo"),
                "b2ab(ddec(encoded_lengths, dynamic_data, 0), 32) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Numeric(32))).to_sql(0, "foo"),
                "b2an(ddec(encoded_lengths, dynamic_data, 0), 32) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Bytea).to_sql(0, "foo"),
                "ddec(encoded_lengths, dynamic_data, 0) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Text).to_sql(0, "foo"),
                r#"convert_from(rtrim(ddec(encoded_lengths, dynamic_data, 0), '\x00'), 'UTF8') as foo"#
            );
        }
        #[test]
        fn test_size() {
            assert_eq!(Kind::Static(Static::Bytea(1)).size(), Some(1));
            assert_eq!(Kind::Static(Static::Numeric(32)).size(), Some(32));
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Numeric(32))).size(),
                None
            );
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Schema {
    pub address: FixedBytes<20>,
    pub table_id: FixedBytes<32>,
    pub key_names: Vec<String>,
    pub val_names: Vec<String>,
    pub key_schema: FixedBytes<32>,
    pub val_schema: FixedBytes<32>,
}

impl Schema {
    pub const TABLES_TABLE_ID: B256 =
        b256!("746273746f72650000000000000000005461626c657300000000000000000000");

    pub fn from_data(
        address: FixedBytes<20>,
        table_id: FixedBytes<32>,
        data: &mud_encoding::Data,
    ) -> Result<Self> {
        type SolArrayOf<T> = sol! { T[] };
        let (key_names, val_names) = (
            data.get_dynamic(0)
                .expect("missing dynamic data for key_names"),
            data.get_dynamic(1)
                .expect("missing dynamic data for val_names"),
        );
        Ok(Schema {
            address: address,
            table_id: table_id,
            key_schema: data.get_static32(1),
            val_schema: data.get_static32(2),
            key_names: SolArrayOf::<sol!(string)>::abi_decode(key_names, false)?,
            val_names: SolArrayOf::<sol!(string)>::abi_decode(val_names, false)?,
        })
    }

    pub fn from_row(row: &Row) -> Result<Self, tokio_postgres::Error> {
        Ok(Schema {
            address: row.try_get("address")?,
            table_id: row.try_get("id")?,
            key_names: row.try_get("key_names")?,
            key_schema: row.try_get("key_schema")?,
            val_names: row.try_get("val_names")?,
            val_schema: row.try_get("val_schema")?,
        })
    }

    #[tracing::instrument]
    pub async fn from_pg(
        pg: &Client,
        address: FixedBytes<20>,
        tables: Vec<String>,
    ) -> Result<Vec<Self>, tokio_postgres::Error> {
        pg.query(
            r#"
                select address, id, key_names, key_schema, val_names, val_schema
                from tables
                where address = $1
                and name = ANY($2)
            "#,
            &[&address, &tables],
        )
        .await?
        .iter()
        .map(Schema::from_row)
        .collect::<Result<Vec<Schema>, _>>()
    }

    #[tracing::instrument(level="debug" skip_all)]
    pub async fn insert(
        &self,
        tx: &Transaction<'_>,
        block_num: u64,
        log_idx: u64,
        address: FixedBytes<20>,
    ) -> Result<()> {
        const Q: &str = r#"
            insert into tables(block_num, log_idx, address, id, name, key_schema, val_schema, key_names, val_names)
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#;
        tx.execute(
            Q,
            &[
                &U64::from(block_num),
                &U64::from(log_idx),
                &address,
                &self.table_id,
                &self.table_name(),
                &self.key_schema,
                &self.val_schema,
                &self.key_names,
                &self.val_names,
            ],
        )
        .await
        .map(|_| ())
        .wrap_err("inserting new table")
    }

    pub fn table_name(&self) -> String {
        let b: Vec<u8> = self.table_id[15..32]
            .iter()
            .map(|c| *c)
            .filter(|c| *c > 0 && *c < 255) //ascii table names
            .collect();
        String::from_utf8(b).unwrap()
    }

    pub fn cte_sql(&self, columns: Vec<String>) -> Result<String, api::Error> {
        let mut res: Vec<String> = Vec::new();
        res.push(format!("{} as (", self.table_name()));
        res.push("select".to_string());
        res.push(
            columns
                .iter()
                .map(|c| self.col_sql(c))
                .collect::<Result<Vec<_>, _>>()?
                .iter()
                .join(","),
        );
        res.push(format!(
            r#"from records where address = '\x{}' and table_id = '\x{}' and not expired and not deleted"#,
            hex::encode(self.address),
            hex::encode(self.table_id),
        ));
        res.push(")".to_string());
        Ok(res.join(" "))
    }

    pub fn col_sql(&self, name: &str) -> Result<String, api::Error> {
        if let Some(pos) = self.key_names.iter().position(|n| n == name) {
            return Ok(format!("sdec(key, {}, 32) as {}", pos * 32, name));
        }
        let mut pos = self
            .val_names
            .iter()
            .position(|n| n == name)
            .ok_or(api::Error::User(format!("column '{}' not found", name)))?;

        let schema_type = field::Kind::from_schema_type(self.val_schema[4 + pos]).unwrap();
        if matches!(schema_type, field::Kind::Static(_)) {
            pos = self
                .val_schema
                .iter()
                .skip(4)
                .take(pos)
                .map(|b| field::Kind::from_schema_type(*b).unwrap().size().unwrap())
                .sum()
        }
        Ok(schema_type.to_sql(pos, name))
    }
}

#[cfg(test)]
mod schema_tests {
    use super::*;
    use alloy::primitives::fixed_bytes;

    #[test]
    fn test_get_col_sql() {
        let schema = &Schema {
            address: fixed_bytes!(),
            table_id: fixed_bytes!(),
            key_names: vec![],
            val_names: vec![String::from("value")],
            key_schema: fixed_bytes!(),
            val_schema: fixed_bytes!(
                "0004010003000000000000000000000000000000000000000000000000000000"
            ),
        };
        assert_eq!(
            schema.col_sql("value").unwrap(),
            "b2n(sdec(static_data, 0, 4)) as value"
        )
    }
}
