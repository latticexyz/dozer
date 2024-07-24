use std::collections::HashSet;

use crate::api;

use alloy::{
    hex,
    primitives::{b256, Address, FixedBytes, B256},
    sol,
    sol_types::SolType,
};
use eyre::{Result, WrapErr};
use itertools::Itertools;
use ruint::aliases::U64;
use serde::{Deserialize, Serialize};
use tokio_postgres::{Row, Transaction};

use crate::mud_encoding;

pub mod query {
    use crate::{api, validate_sql};

    use alloy::primitives::Address;
    use eyre::Result;
    use itertools::Itertools;
    use sqlparser::{ast::visit_relations, dialect::PostgreSqlDialect, parser::Parser};
    use std::{collections::HashSet, ops::ControlFlow};
    use tokio_postgres::Transaction;

    use super::Schema;

    const PG: &PostgreSqlDialect = &PostgreSqlDialect {};

    pub async fn enhance(
        pgtx: &Transaction<'_>,
        address: Address,
        user_query: &str,
    ) -> Result<String, api::Error> {
        let parsed_query =
            Parser::parse_sql(PG, user_query).map_err(|e| api::Error::User(e.to_string()))?;
        let schemas = load_schemas(pgtx, address, &parsed_query).await?;
        if schemas.len() == 0 {
            return Err(api::Error::User("schemas not found".to_string()));
        }
        build_sql(user_query, schemas)
    }

    async fn load_schemas(
        pgtx: &Transaction<'_>,
        address: Address,
        query: &Vec<sqlparser::ast::Statement>,
    ) -> Result<Vec<Schema>, api::Error> {
        let mut table_names = HashSet::new();
        visit_relations(query, |relation| {
            let mut relname = relation.to_string();
            relname.truncate(30);
            table_names.insert(relname);
            ControlFlow::<()>::Continue(())
        });
        Ok(Schema::from_pg(pgtx, address, table_names.into_iter().collect()).await?)
    }

    fn build_sql(user_query: &str, schemas: Vec<Schema>) -> Result<String, api::Error> {
        let schemas = validate_sql::validate(user_query, schemas)?;
        let mut query: Vec<String> = Vec::new();
        query.push("with".to_string());
        query.push(
            schemas
                .iter()
                .sorted_by_key(|s| s.qname())
                .map(|s| s.cte_sql())
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
                "select __foo.value, __bar.value from __foo, __bar where __foo.value = __bar.value",
            );
            let pq = build_sql(
                &user_query,
                vec![
                    Schema {
                        address: fixed_bytes!(),
                        table_id: fixed_bytes!(
                            "00000000000000000000000000000000666f6f00000000000000000000000000"
                        ),
                        key_names: vec![],
                        val_names: vec![String::from("value")],
                        key_schema: fixed_bytes!(),
                        val_schema: fixed_bytes!(
                            "0004010003000000000000000000000000000000000000000000000000000000"
                        ),
                        select_list: None,
                    },
                    Schema {
                        address: fixed_bytes!(),
                        table_id: fixed_bytes!(
                            "0000000000000000000000000000000062617200000000000000000000000000"
                        ),
                        key_names: vec![],
                        val_names: vec![String::from("value")],
                        key_schema: fixed_bytes!(),
                        val_schema: fixed_bytes!(
                            "0004010003000000000000000000000000000000000000000000000000000000"
                        ),
                        select_list: None,
                    },
                ],
            );
            assert_eq!(
                fmt_sql(&pq.unwrap()).expect("parsing generated sql"),
                fmt_sql(r#"
                    with __bar as (
                        select b2n(sdec(static_data, 0, 4)) as value
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x0000000000000000000000000000000062617200000000000000000000000000'
                        and not expired
                        and not deleted
                    ), __foo as (
                        select b2n(sdec(static_data, 0, 4)) as value
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x00000000000000000000000000000000666f6f00000000000000000000000000'
                        and not expired
                        and not deleted
                    ) select __foo.value, __bar.value from __foo, __bar where __foo.value = __bar.value
                "#).unwrap()
        )
        }
    }
}

mod field {
    use crate::api;

    #[derive(Debug, PartialEq)]
    pub enum Desc {
        Address,
        Bool,
        Bytes,
        Int,
        Uint,
    }
    impl Desc {
        fn to_string(&self) -> &'static str {
            match self {
                Desc::Address => "address",
                Desc::Bool => "bool",
                Desc::Bytes => "bytes",
                Desc::Int => "int",
                Desc::Uint => "uint",
            }
        }
    }
    #[derive(Debug, PartialEq)]
    pub enum Kind {
        Static(Static),
        Dynamic(Dynamic),
    }
    #[derive(Debug, PartialEq)]
    pub enum Static {
        Num(u8, Desc),
        Bytea(u8, Desc),
    }
    impl Static {
        fn description(&self) -> String {
            match self {
                Static::Bytea(_, Desc::Address) => "address".to_string(),
                Static::Bytea(size, d) => {
                    format!("{}{}", d.to_string(), size)
                }
                Static::Num(size, d) => format!("{}{}", d.to_string(), 8 * (*size as u64).min(32)),
            }
        }
    }
    #[derive(Debug, PartialEq)]
    pub enum Dynamic {
        Bytea,
        Text,
        Array(Static),
    }
    impl Dynamic {
        fn description(&self) -> String {
            match self {
                Dynamic::Bytea => String::from("bytes"),
                Dynamic::Text => String::from("string"),
                Dynamic::Array(s) => s.description() + "[]",
            }
        }
    }
    impl Kind {
        pub fn from_schema_type(t: u8) -> Option<Self> {
            Some(match t {
                n if t < 32 => Kind::Static(Static::Num(n + 1, Desc::Uint)),
                n if t < 64 => Kind::Static(Static::Num(n - 31, Desc::Int)),
                n if t < 96 => Kind::Static(Static::Bytea(n - 63, Desc::Bytes)),
                _ if t == 96 => Kind::Static(Static::Bytea(1, Desc::Bool)),
                _ if t == 97 => Kind::Static(Static::Bytea(20, Desc::Address)),
                n if t < 130 => Kind::Dynamic(Dynamic::Array(Static::Num(n - 97, Desc::Uint))),
                n if t < 162 => Kind::Dynamic(Dynamic::Array(Static::Num(n - 129, Desc::Int))),
                n if t < 194 => Kind::Dynamic(Dynamic::Array(Static::Bytea(n - 161, Desc::Bytes))),
                _ if t == 194 => Kind::Dynamic(Dynamic::Array(Static::Bytea(1, Desc::Bool))),
                _ if t == 195 => Kind::Dynamic(Dynamic::Array(Static::Bytea(20, Desc::Address))),
                _ if t == 196 => Kind::Dynamic(Dynamic::Bytea),
                _ if t == 197 => Kind::Dynamic(Dynamic::Text),
                _ => return None,
            })
        }

        pub fn size(&self) -> Option<usize> {
            match self {
                Kind::Static(Static::Bytea(s, _)) | Kind::Static(Static::Num(s, _)) => {
                    Some(*s as usize)
                }
                Kind::Dynamic(_) => None,
            }
        }

        pub fn description(&self) -> String {
            match self {
                Kind::Static(s) => s.description(),
                Kind::Dynamic(d) => d.description(),
            }
        }

        pub fn key_sql(&self, pos: usize, name: &str) -> Result<String, api::Error> {
            match self {
                Kind::Static(t) => match t {
                    Static::Num(_, _) => {
                        Ok(format!("b2n(sdec(key, {}, 32)) as {}", 32 * pos, name))
                    }
                    Static::Bytea(_, Desc::Address) => Ok(format!(
                        "substring(sdec(key, {}, 32) from 13 for 20) as {}",
                        32 * pos,
                        name
                    )),
                    Static::Bytea(_, _) => Ok(format!("sdec(key, {}, 32) as {}", 32 * pos, name)),
                },
                _ => Err(api::Error::User("key must be static".to_string())),
            }
        }

        pub fn val_sql(&self, pos: usize, name: &str) -> String {
            match self {
                Kind::Static(t) => match t {
                    Static::Num(size, _) => {
                        format!("b2n(sdec(static_data, {}, {})) as {}", pos, size, name)
                    }
                    Static::Bytea(size, Desc::Address) => {
                        format!(
                            "substring(sdec(static_data, {}, {}) from 13 for 20) as {}",
                            pos, size, name
                        )
                    }
                    Static::Bytea(_, Desc::Bool) => {
                        format!("get_byte(static_data, {}) = 1 as {}", pos, name)
                    }
                    Static::Bytea(size, _) => {
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
                        Static::Bytea(size, _) => {
                            format!(
                                "b2ab(ddec(encoded_lengths, dynamic_data, {}), {}) as {}",
                                pos, size, name
                            )
                        }
                        Static::Num(size, _) => {
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
                Kind::Static(Static::Num(32, Desc::Uint)),
                Kind::from_schema_type(0x1F).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Num(32, Desc::Int)),
                Kind::from_schema_type(0x3f).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Bytea(32, Desc::Bytes)),
                Kind::from_schema_type(0x5f).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Bytea(1, Desc::Bool)),
                Kind::from_schema_type(0x60).unwrap()
            );
            assert_eq!(
                Kind::Static(Static::Bytea(20, Desc::Address)),
                Kind::from_schema_type(0x61).unwrap()
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Num(32, Desc::Uint))),
                Kind::from_schema_type(0x81).unwrap()
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Num(32, Desc::Int))),
                Kind::from_schema_type(0xA1).unwrap()
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Bytea(32, Desc::Bytes))),
                Kind::from_schema_type(0xC1).unwrap()
            );
        }
        #[test]
        fn test_to_sql() {
            assert_eq!(
                Kind::Static(Static::Num(32, Desc::Uint)).val_sql(1, "foo"),
                "b2n(sdec(static_data, 1, 32)) as foo"
            );
            assert_eq!(
                Kind::Static(Static::Bytea(32, Desc::Bytes)).val_sql(1, "foo"),
                "sdec(static_data, 1, 32) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Bytea(32, Desc::Bytes))).val_sql(0, "foo"),
                "b2ab(ddec(encoded_lengths, dynamic_data, 0), 32) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Num(32, Desc::Uint))).val_sql(0, "foo"),
                "b2an(ddec(encoded_lengths, dynamic_data, 0), 32) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Bytea).val_sql(0, "foo"),
                "ddec(encoded_lengths, dynamic_data, 0) as foo"
            );
            assert_eq!(
                Kind::Dynamic(Dynamic::Text).val_sql(0, "foo"),
                r#"convert_from(rtrim(ddec(encoded_lengths, dynamic_data, 0), '\x00'), 'UTF8') as foo"#
            );
        }
        #[test]
        fn test_size() {
            assert_eq!(Kind::Static(Static::Bytea(1, Desc::Bytes)).size(), Some(1));
            assert_eq!(Kind::Static(Static::Num(32, Desc::Uint)).size(), Some(32));
            assert_eq!(
                Kind::Dynamic(Dynamic::Array(Static::Num(32, Desc::Uint))).size(),
                None
            );
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Schema {
    pub address: FixedBytes<20>,
    pub table_id: FixedBytes<32>,
    pub key_names: Vec<String>,
    pub val_names: Vec<String>,
    pub key_schema: FixedBytes<32>,
    pub val_schema: FixedBytes<32>,

    #[serde(skip_serializing, skip_deserializing)]
    pub select_list: Option<HashSet<String>>,
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
            key_names: SolArrayOf::<sol!(string)>::abi_decode(key_names, false)
                .inspect_err(|e| {
                    tracing::error!("decoding key names: {} {}", e, hex::encode(key_names))
                })
                .unwrap_or_default(),
            val_names: SolArrayOf::<sol!(string)>::abi_decode(val_names, false)
                .inspect_err(|e| {
                    tracing::error!("decoding val names: {} {}", e, hex::encode(val_names))
                })
                .unwrap_or_default(),
            select_list: None,
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
            select_list: None,
        })
    }

    #[tracing::instrument(skip_all, fields(address, tables))]
    pub async fn from_pg(
        pgtx: &Transaction<'_>,
        address: Address,
        tables: Vec<String>,
    ) -> Result<Vec<Self>, tokio_postgres::Error> {
        fn parse_qname(qname: &str) -> FixedBytes<30> {
            let parts: Vec<&str> = qname.split("__").collect();
            let (mut ns, mut name) = (parts[0].as_bytes().to_vec(), parts[1].as_bytes().to_vec());
            ns.resize(14, 0x00);
            name.resize(16, 0x00);
            ns.extend(name);
            FixedBytes::<30>::from_slice(&ns)
        }
        let ids: Vec<FixedBytes<30>> = tables
            .iter()
            .filter(|name| name.contains("__"))
            .map(|name| parse_qname(name))
            .collect();
        pgtx.query(
            r#"
                select address, id, key_names, key_schema, val_names, val_schema
                from tables
                where address = $1
                and substring(id from 3 for 30) = any($2)
            "#,
            &[&address.0, &ids],
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

    pub fn description(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Name: {}", self.table_name()));
        lines.push(format!("\tId:\t{}", self.table_id));
        lines.push(format!("\tAddress:\t{}", self.address));
        lines.push(format!("\tNamespace:\t{}", self.namespace()));
        lines.push("\n\tKeys:".to_string());
        lines.extend(self.key_names.iter().enumerate().map(|(i, key_name)| {
            let kind = field::Kind::from_schema_type(self.key_schema[4 + i]).unwrap();
            format!("\t\t{}\t{}", key_name, kind.description())
        }));
        lines.push("\n\tValues:".to_string());
        lines.extend(self.val_names.iter().enumerate().map(|(i, val_name)| {
            let kind = field::Kind::from_schema_type(self.val_schema[4 + i]).unwrap();
            format!("\t\t{}\t{}", val_name, kind.description())
        }));
        lines.push("\r".to_string());
        lines.join("\n")
    }

    pub fn has_column(&self, col_name: &str) -> bool {
        self.key_names.iter().any(|name| name == col_name)
            || self.val_names.iter().any(|name| name == col_name)
    }

    pub fn qname(&self) -> String {
        format!("{}__{}", self.namespace(), self.table_name())
    }

    fn namespace(&self) -> String {
        let b: Vec<u8> = self.table_id[2..15]
            .iter()
            .map(|c| *c)
            .filter(|c| *c > 0 && *c < 255) //ascii table names
            .collect();
        String::from_utf8(b).unwrap()
    }

    fn table_name(&self) -> String {
        let b: Vec<u8> = self.table_id[15..32]
            .iter()
            .map(|c| *c)
            .filter(|c| *c > 0 && *c < 255) //ascii table names
            .collect();
        String::from_utf8(b).unwrap()
    }

    fn num_static(&self) -> usize {
        self.val_schema[2] as usize
    }

    pub fn cte_sql(&self) -> Result<String, api::Error> {
        let mut res: Vec<String> = Vec::new();
        res.push(format!("{} as (", self.qname()));
        res.push("select".to_string());
        if let Some(sl) = &self.select_list {
            res.push(
                sl.iter()
                    .map(|c| self.col_sql(c))
                    .collect::<Result<Vec<_>, _>>()?
                    .iter()
                    .join(","),
            );
        }
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
            let schema_type = field::Kind::from_schema_type(self.key_schema[4 + pos]).unwrap();
            return schema_type.key_sql(pos, name);
        }
        let pos = self
            .val_names
            .iter()
            .position(|n| n == name)
            .ok_or(api::Error::User(format!("column '{}' not found", name)))?;
        let schema_type = field::Kind::from_schema_type(self.val_schema[4 + pos]).unwrap();

        let pos = match schema_type {
            field::Kind::Static(_) => self
                .val_schema
                .iter()
                .skip(4)
                .take(pos)
                .map(|b| field::Kind::from_schema_type(*b).unwrap().size().unwrap())
                .sum(),
            field::Kind::Dynamic(_) => pos - self.num_static(),
        };
        Ok(schema_type.val_sql(pos, name))
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
            select_list: None,
        };
        assert_eq!(
            schema.col_sql("value").unwrap(),
            "b2n(sdec(static_data, 0, 4)) as value"
        )
    }
}
