use std::collections::{HashMap, HashSet};

use crate::api;

use alloy::{
    hex,
    primitives::{b256, Address, FixedBytes, B256},
    sol,
    sol_types::SolType,
};
use eyre::{bail, Result, WrapErr};
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
        block_height: Option<u64>,
        block_height_direction: Option<&str>,
        user_query: &str,
    ) -> Result<String, api::Error> {
        let parsed_query =
            Parser::parse_sql(PG, user_query).map_err(|e| api::Error::User(e.to_string()))?;
        let schemas = load_schemas(pgtx, address, &parsed_query).await?;
        if schemas.is_empty() {
            return Err(api::Error::User("schemas not found".to_string()));
        }
        build_sql(block_height, block_height_direction, user_query, schemas)
    }

    async fn load_schemas(
        pgtx: &Transaction<'_>,
        address: Address,
        query: &Vec<sqlparser::ast::Statement>,
    ) -> Result<Vec<Schema>, api::Error> {
        let mut table_names = HashSet::new();
        visit_relations(query, |relation| {
            let mut relname = relation.to_string();
            // 14 byte namespace, 16 byte name, 2 byte delimiter, 2 byte quote wrapping
            relname.truncate(34);
            table_names.insert(relname);
            ControlFlow::<()>::Continue(())
        });
        Schema::from_pg(pgtx, address, table_names.into_iter().collect()).await
    }

    fn build_sql(
        block_height: Option<u64>,
        block_height_direction: Option<&str>,
        user_query: &str,
        schemas: Vec<Schema>,
    ) -> Result<String, api::Error> {
        let schemas = validate_sql::validate(user_query, schemas)?;
        let query: Vec<String> = vec![
            "with".to_string(),
            schemas
                .iter()
                .sorted_by_key(|s| s.full_name())
                .map(|s| s.cte_sql(block_height, block_height_direction))
                .collect::<Result<Vec<_>, _>>()?
                .join(","),
            user_query.to_string(),
        ];
        Ok(query.join(" "))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::mud_schema::encode_resource_id;
        use crate::preformat_sql;
        use alloy::primitives::{fixed_bytes, FixedBytes};

        fn fmt_sql(sql: &str) -> Result<String> {
            let ast = Parser::parse_sql(PG, sql)?;
            Ok(ast[0].to_string())
        }

        fn test_schema(table_name: &str, col_name: &str) -> Schema {
            let mut table_id = FixedBytes::<32>::ZERO;
            table_id[2..].copy_from_slice(encode_resource_id(table_name).as_slice());
            Schema {
                query_name: None,
                select_list: None,
                address: fixed_bytes!(),
                table_id,
                key_names: vec![],
                val_names: vec![String::from(col_name)],
                key_schema: fixed_bytes!(),
                val_schema: fixed_bytes!(
                    "0004010003000000000000000000000000000000000000000000000000000000"
                ),
            }
        }

        #[test]
        fn test_enhance() {
            let user_query =
                "select \"foo\".value, \"bar\".value from \"foo\",\"bar\" where \"foo\".value = \"bar\".value";
            let preformatted_query = preformat_sql::preformat(user_query);
            let pq = build_sql(
                None,
                None,
                &preformatted_query,
                vec![test_schema("foo", "value"), test_schema("bar", "value")],
            );

            assert_eq!(
                fmt_sql(&pq.unwrap()).expect("parsing generated sql"),
                fmt_sql(r#"
                    with "bar" as (
                        select coalesce(b2n(sdec(static_data, 0, 4)), 0) as "value"
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x0000000000000000000000000000000062617200000000000000000000000000'
                        and not expired
                        and not deleted
                    ), "foo" as (
                        select coalesce(b2n(sdec(static_data, 0, 4)), 0) as "value"
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x00000000000000000000000000000000666f6f00000000000000000000000000'
                        and not expired
                        and not deleted
                    ) select "foo"."value", "bar"."value" from "foo","bar" where "foo"."value" = "bar"."value"
                "#).unwrap()
            )
        }

        #[test]
        fn test_enhance_unquoted() {
            let user_query = "select foo.value, bar.value from foo,bar where foo.value = bar.value";
            let preformatted_query = preformat_sql::preformat(user_query);
            let pq = build_sql(
                None,
                None,
                &preformatted_query,
                vec![test_schema("foo", "value"), test_schema("bar", "value")],
            );

            assert_eq!(
                fmt_sql(&pq.unwrap()).expect("parsing generated sql"),
                fmt_sql(r#"
                    with "bar" as (
                        select coalesce(b2n(sdec(static_data, 0, 4)), 0) as "value"
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x0000000000000000000000000000000062617200000000000000000000000000'
                        and not expired
                        and not deleted
                    ), "foo" as (
                        select coalesce(b2n(sdec(static_data, 0, 4)), 0) as "value"
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x00000000000000000000000000000000666f6f00000000000000000000000000'
                        and not expired
                        and not deleted
                    ) select "foo"."value", "bar"."value" from "foo","bar" where "foo"."value" = "bar"."value"
                "#).unwrap()
            )
        }

        #[test]
        fn test_enhance_block_height() {
            let user_query = "select value from foo";
            let preformatted_query = preformat_sql::preformat(user_query);
            let pq = build_sql(
                Some(42),
                None,
                &preformatted_query,
                vec![test_schema("foo", "value")],
            );
            assert_eq!(
                fmt_sql(&pq.unwrap()).expect("parsing generated sql"),
                fmt_sql(r#"
                    with "foo" as (
                        select coalesce(b2n(sdec(static_data, 0, 4)), 0) as value
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x00000000000000000000000000000000666f6f00000000000000000000000000'
                        and not expired
                        and not deleted
                        and block_num >= 42
                    ) select value from "foo"
                "#).unwrap()
            )
        }

        #[test]
        fn test_enhance_block_height_greater_equal_than() {
            let user_query = "select value from foo";
            let preformatted_query = preformat_sql::preformat(user_query);
            let pq = build_sql(
                Some(42),
                Some(">="),
                &preformatted_query,
                vec![test_schema("foo", "value")],
            );
            assert_eq!(
                fmt_sql(&pq.unwrap()).expect("parsing generated sql"),
                fmt_sql(r#"
                    with "foo" as (
                        select coalesce(b2n(sdec(static_data, 0, 4)), 0) as value
                        from records
                        where address = '\x0000000000000000000000000000000000000000'
                        and table_id = '\x00000000000000000000000000000000666f6f00000000000000000000000000'
                        and not expired
                        and not deleted
                        and block_num >= 42
                    ) select value from "foo"
                "#).unwrap()
            )
        }

        #[test]
        fn test_enhance_block_height_less_than() {
            let user_query = "select value from foo";
            let preformatted_query = preformat_sql::preformat(user_query);
            let pq = build_sql(
                Some(42),
                Some("<="),
                &preformatted_query,
                vec![test_schema("foo", "value")],
            );
            assert_eq!(
                fmt_sql(&pq.unwrap()).expect("parsing generated sql"),
                fmt_sql(r#"
                    with "foo" as (
                        select coalesce(b2n(sdec(static_data, 0, 4)), 0) as value
                        from (
                            select *,
                                row_number() over (
                                    partition by address, table_id, key
                                    order by block_num desc, log_idx desc
                                ) as row_number
                            from records
                            where address = '\x0000000000000000000000000000000000000000'
                            and table_id = '\x00000000000000000000000000000000666f6f00000000000000000000000000'
                            and block_num <= 42
                        ) latest_records
                        where row_number = 1
                        and not deleted
                    ) select value from "foo"
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
                    Static::Num(_, Desc::Int) => {
                        Ok(format!("b2sn(sdec(key, {}, 32)) as {}", 32 * pos, name))
                    }
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
                    Static::Num(size, Desc::Int) => {
                        format!(
                            "coalesce(b2sn(sdec(static_data, {}, {})), 0) as {}",
                            pos, size, name
                        )
                    }
                    Static::Num(size, _) => {
                        format!(
                            "coalesce(b2n(sdec(static_data, {}, {})), 0) as {}",
                            pos, size, name
                        )
                    }
                    Static::Bytea(_, Desc::Bool) => {
                        format!(
                            "coalesce(get_byte(static_data, {}), 0) = 1 as {}",
                            pos, name
                        )
                    }
                    Static::Bytea(size, _) => {
                        format!(
                            "coalesce(sdec(static_data, {}, {}), '\\x00') as {}",
                            pos, size, name
                        )
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
                Kind::Static(Static::Num(32, Desc::Uint))
                    .key_sql(0, "id")
                    .unwrap(),
                "b2n(sdec(key, 0, 32)) as id"
            );
            assert_eq!(
                Kind::Static(Static::Num(32, Desc::Int))
                    .key_sql(0, "id")
                    .unwrap(),
                "b2sn(sdec(key, 0, 32)) as id"
            );
            assert_eq!(
                Kind::Static(Static::Num(32, Desc::Uint)).val_sql(1, "foo"),
                "coalesce(b2n(sdec(static_data, 1, 32)), 0) as foo"
            );
            assert_eq!(
                Kind::Static(Static::Bytea(32, Desc::Bytes)).val_sql(1, "foo"),
                "coalesce(sdec(static_data, 1, 32), '\\x00') as foo"
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

fn encode_resource_id(name: &str) -> FixedBytes<30> {
    let name = name.strip_prefix("__").unwrap_or(name);
    let name = name.trim_matches('"');
    let (mut id, parts) = (FixedBytes::<30>::ZERO, name.split("__").collect_vec());
    match parts.len() {
        1 => {
            let mut name = parts[0].as_bytes().to_vec();
            name.resize(16, 0x00);
            id[14..].copy_from_slice(&name[..]);
            id
        }
        2 => {
            let mut ns = parts[0].as_bytes().to_vec();
            ns.resize(14, 0x00);
            id[..14].copy_from_slice(&ns[..]);
            let mut name = parts[1].as_bytes().to_vec();
            name.resize(16, 0x00);
            id[14..].copy_from_slice(&name[..]);
            id
        }
        _ => panic!("unable to parse table name: {}", name),
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
    pub query_name: Option<String>,
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
            address,
            table_id,
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
            query_name: None,
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
            query_name: None,
        })
    }

    #[tracing::instrument(skip_all, fields(address, tables))]
    pub async fn from_pg(
        pgtx: &Transaction<'_>,
        address: Address,
        tables: Vec<String>,
    ) -> Result<Vec<Self>, api::Error> {
        let idmap: HashMap<FixedBytes<30>, String> = tables
            .into_iter()
            .map(|name| (encode_resource_id(&name), name))
            .collect();
        let mut res = pgtx
            .query(
                r#"
                select address, id, key_names, key_schema, val_names, val_schema
                from tables
                where address = $1
                and substring(id from 3 for 30) = any($2)
                "#,
                &[&address.0, &idmap.keys().collect_vec()],
            )
            .await?
            .iter()
            .map(Schema::from_row)
            .collect::<Result<Vec<Schema>, _>>()?;
        res.iter_mut().for_each(|schema| {
            schema.query_name = idmap
                .get(&schema.table_id[2..])
                .map(|name| name.to_string())
        });
        Ok(res)
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
            on conflict (address, id) do nothing
        "#;
        let name = self.name();
        let inserted = tx
            .execute(
            Q,
            &[
                &U64::from(block_num),
                &U64::from(log_idx),
                &address,
                &self.table_id,
                &name,
                &self.key_schema,
                &self.val_schema,
                &self.key_names,
                &self.val_names,
            ],
        )
            .await
            .wrap_err("inserting new table")?;
        if inserted > 0 {
            return Ok(());
        }

        const EXISTING_Q: &str = r#"
            select name, key_schema, val_schema, key_names, val_names
            from tables
            where address = $1 and id = $2
        "#;
        let row = tx
            .query_opt(EXISTING_Q, &[&address, &self.table_id])
            .await
            .wrap_err("loading existing table schema after insert conflict")?
            .ok_or_else(|| {
                eyre::eyre!(
                    "insert conflict but existing row missing (address={}, id={})",
                    address,
                    self.table_id
                )
            })?;
        let existing_name = row.try_get::<&str, String>("name")?;
        let existing_key_schema = row.try_get::<&str, FixedBytes<32>>("key_schema")?;
        let existing_val_schema = row.try_get::<&str, FixedBytes<32>>("val_schema")?;
        let existing_key_names = row.try_get::<&str, Vec<String>>("key_names")?;
        let existing_val_names = row.try_get::<&str, Vec<String>>("val_names")?;
        let same = existing_name == name
            && existing_key_schema == self.key_schema
            && existing_val_schema == self.val_schema
            && existing_key_names == self.key_names
            && existing_val_names == self.val_names;
        if same {
            Ok(())
        } else {
            bail!(
                "table already exists with different schema (address={}, id={})\n  existing: name={}, key_schema={}, val_schema={}, key_names={:?}, val_names={:?}\n  new:      name={}, key_schema={}, val_schema={}, key_names={:?}, val_names={:?}",
                address,
                self.table_id,
                existing_name, existing_key_schema, existing_val_schema, existing_key_names, existing_val_names,
                name, self.key_schema, self.val_schema, self.key_names, self.val_names,
            );
        }
    }

    pub fn description(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Name: {}", self.name()));
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

    pub fn has_column(&self, name: &str) -> bool {
        self.key_pos(name).is_some() || self.val_pos(name).is_some()
    }

    pub fn key_pos(&self, name: &str) -> Option<usize> {
        let unquoted = name.replace('"', "");
        self.key_names.iter().position(|n| *n == unquoted)
    }

    pub fn val_pos(&self, name: &str) -> Option<usize> {
        let unquoted = name.replace('"', "");
        self.val_names.iter().position(|n| *n == unquoted)
    }

    pub fn has_name(&self, name: &str) -> bool {
        let name = name.split("__").last().unwrap();
        name.chars().take(16).collect::<String>() == self.name()
    }

    pub fn full_name(&self) -> String {
        if self.namespace().is_empty() {
            self.name()
        } else {
            [self.namespace(), self.name()].join("__")
        }
    }

    fn namespace(&self) -> String {
        String::from_utf8(self.table_id[2..16].to_vec())
            .expect("unable to utf8 decode namespace")
            .replace('\0', "")
    }

    fn name(&self) -> String {
        String::from_utf8(self.table_id[16..32].to_vec())
            .expect("unable to utf8 decode table name")
            .replace('\0', "")
    }

    fn num_static(&self) -> usize {
        self.val_schema[2] as usize
    }

    pub fn cte_sql(
        &self,
        block_height: Option<u64>,
        block_height_direction: Option<&str>,
    ) -> Result<String, api::Error> {
        let mut res: Vec<String> = Vec::new();
        res.push(format!("\"{}\" as (", self.full_name()));
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

        let query = if block_height.is_some()
            && block_height_direction.is_some()
            && block_height_direction.unwrap() != ">="
        {
            format!(
                r#"
                from (
                    select *,
                        row_number() over (
                            partition by address, table_id, key
                            order by block_num desc, log_idx desc
                        ) as row_number
                    from records
                    where address = '\x{}'
                    and table_id = '\x{}'
                    and block_num {} {}
                ) latest_records
                where row_number = 1
                and not deleted
                "#,
                hex::encode(self.address),
                hex::encode(self.table_id),
                block_height_direction.unwrap(),
                block_height.unwrap()
            )
        } else {
            format!(
                r#"
                from records
                where address = '\x{}'
                and table_id = '\x{}'
                and not expired
                and not deleted
                {}
                "#,
                hex::encode(self.address),
                hex::encode(self.table_id),
                block_height.map_or(String::new(), |h| format!("and block_num >= {}", h))
            )
        };

        res.push(query);
        res.push(")".to_string());
        Ok(res.join(" "))
    }

    pub fn col_sql(&self, name: &str) -> Result<String, api::Error> {
        if let Some(pos) = self.key_pos(name) {
            let schema_type = field::Kind::from_schema_type(self.key_schema[4 + pos]).unwrap();
            return schema_type.key_sql(pos, name);
        }
        let pos = self
            .val_pos(name)
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
            query_name: None,
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
            "coalesce(b2n(sdec(static_data, 0, 4)), 0) as value"
        )
    }
}
