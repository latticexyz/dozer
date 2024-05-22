use crate::api_error::ApiError;
use alloy::{primitives::FixedBytes, sol, sol_types::SolType};
use axum::http::StatusCode;
use eyre::{eyre, Result, WrapErr};
use ruint::aliases::U64;
use tokio_postgres::{Client, Row, Transaction};

pub mod field {
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
        pub fn from_schema_type(t: u8) -> eyre::Result<Self> {
            Ok(match t {
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
                _ => return Err(eyre::eyre!("unknown type: {:x}", t)),
            })
        }
        pub fn size(&self) -> Option<usize> {
            todo!()
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
                Kind::Dynamic(t) => {
                    match t {
                        Dynamic::Bytea => {
                            format!("ddec(encoded_lengths, dynamic_data, {}) as {}", pos, name)
                        }
                        Dynamic::Text => {
                            format!("convert_from(ddec(encoded_lengths, dynamic_data, {}), 'UTF8') as {}", pos, name)
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
                    }
                }
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
                "convert_from(ddec(encoded_lengths, dynamic_data, 0), 'UTF8') as foo"
            );
        }
    }
}

pub struct Schema {
    pub address: FixedBytes<20>,
    pub table_id: FixedBytes<32>,
    pub key_names: Vec<String>,
    pub val_names: Vec<String>,
    pub key_schema: FixedBytes<32>,
    pub val_schema: FixedBytes<32>,
}

impl Schema {
    pub fn from_data(
        address: FixedBytes<20>,
        table_id: FixedBytes<32>,
        data: &Data,
    ) -> Result<Self> {
        type SolArrayOf<T> = sol! { T[] };
        Ok(Schema {
            address: address,
            table_id: table_id,
            key_schema: FixedBytes::<32>::from_slice(data.s.get(32..64).unwrap()),
            val_schema: FixedBytes::<32>::from_slice(data.s.get(64..96).unwrap()),
            key_names: SolArrayOf::<sol!(string)>::abi_decode(
                data.d.f0.expect("missing dynamic field for key names"),
                false,
            )?,
            val_names: SolArrayOf::<sol!(string)>::abi_decode(
                data.d.f1.expect("missing dynamic field for val names"),
                false,
            )?,
        })
    }

    pub async fn from_pg(pg: &Client, table_names: Vec<String>) -> Result<Self, ApiError> {
        if table_names.len() != 1 {
            return Err(ApiError::User(
                StatusCode::BAD_REQUEST,
                String::from("must be 1 table for now"),
            ));
        }
        let schema = Self::from_row(
            &pg.query_one(
                "
                select id, key_names, key_schema, val_names, val_schema
                from tables
                where name = $1
                ",
                &[&table_names.first().unwrap()],
            )
            .await?,
        )?;
        Ok(schema)
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

    fn from_row(row: &Row) -> Result<Self, tokio_postgres::Error> {
        Ok(Schema {
            address: row.try_get("address")?,
            table_id: row.try_get("id")?,
            key_names: row.try_get("key_names")?,
            key_schema: row.try_get("key_schema")?,
            val_names: row.try_get("val_names")?,
            val_schema: row.try_get("val_schema")?,
        })
    }

    pub fn table_name(&self) -> String {
        let b: Vec<u8> = self.table_id[15..32]
            .iter()
            .map(|c| *c)
            .filter(|c| *c > 0 && *c < 255) //ascii table names
            .collect();
        String::from_utf8(b).unwrap()
    }

    pub fn get_col_sql(&self, name: &str) -> Result<String> {
        let mut pos = self
            .val_names
            .iter()
            .position(|n| n == name)
            .ok_or(eyre!(format!(
                "table: {} has no column named: {}",
                self.table_name(),
                name
            )))?;
        let schema_type = field::Kind::from_schema_type(self.val_schema[4 + pos])?;
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
            schema.get_col_sql("value").unwrap(),
            "b2n(sdec(static_data, 0, 4)) as value"
        )
    }
}
pub struct Data<'a> {
    d: DynamicData<'a>,
    s: &'a [u8],
}

impl<'a> Data<'a> {
    pub fn new(
        encoded_lengths: FixedBytes<32>,
        dynamic_data: &'a [u8],
        static_data: &'a [u8],
    ) -> Result<Self> {
        Ok(Data {
            d: DynamicData::new(dynamic_data, encoded_lengths)?,
            s: static_data,
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct DynamicData<'a> {
    f0: Option<&'a [u8]>,
    f1: Option<&'a [u8]>,
    f2: Option<&'a [u8]>,
    f3: Option<&'a [u8]>,
    f4: Option<&'a [u8]>,
}

impl<'a> DynamicData<'a> {
    pub fn new(data: &'a [u8], el: FixedBytes<32>) -> Result<Self> {
        fn dec(s: &[u8]) -> usize {
            s.into_iter().fold(0, |n, b| n << 8 | *b as usize)
        }
        let l4 = dec(&el[0..5]);
        let l3 = dec(&el[5..10]);
        let l2 = dec(&el[10..15]);
        let l1 = dec(&el[15..20]);
        let l0 = dec(&el[20..25]);
        let total = dec(&el[25..32]);
        if total != l0 + l1 + l2 + l3 + l4 {
            return Err(eyre!("corrupt dynamic data"));
        }
        Ok(DynamicData {
            f4: data.get(l3..l3 + l4).filter(|&sub| !sub.is_empty()),
            f3: data.get(l2..l2 + l3).filter(|&sub| !sub.is_empty()),
            f2: data.get(l1..l1 + l2).filter(|&sub| !sub.is_empty()),
            f1: data.get(l0..l0 + l1).filter(|&sub| !sub.is_empty()),
            f0: data.get(0..l0).filter(|&sub| !sub.is_empty()),
        })
    }
}

#[cfg(test)]
mod dynamic_data_test {
    use super::DynamicData;
    use alloy::primitives::fixed_bytes;
    #[test]
    fn test_new_error() {
        let el = fixed_bytes!("0000000000000000000000000000000000000000000000000000000000000020");
        let dd = &[1u8; 32];
        let dd = DynamicData::new(dd, el);
        assert!(dd.is_err());
    }
    #[test]
    fn test_new_empty() {
        let el = fixed_bytes!("0000000000000000000000000000000000000000000000000000000000000000");
        let dd = &[0u8];
        let dd = DynamicData::new(dd, el);
        assert!(dd.is_ok());

        let dd = dd.unwrap();
        assert!(dd.f0.is_none());
        assert!(dd.f1.is_none());
        assert!(dd.f2.is_none());
        assert!(dd.f3.is_none());
        assert!(dd.f4.is_none());
    }
    #[test]
    fn test_new_not_empty() {
        let el = fixed_bytes!("0000000000000000000000000000000000000000000000002000000000000020");
        let dd = &[1u8; 32];
        let dd = DynamicData::new(dd, el);
        assert!(dd.is_ok());

        let dd = dd.unwrap();
        assert!(dd.f0.is_some());
        assert!(dd.f1.is_none());
        assert!(dd.f2.is_none());
        assert!(dd.f3.is_none());
        assert!(dd.f4.is_none());

        assert_eq!(dd.f0.unwrap(), &[1u8; 32])
    }
}

#[cfg(test)]
mod pl_pgsql_test {
    use alloy::primitives::fixed_bytes;
    use postgresql_embedded::{PostgreSQL, Settings};
    use tokio_postgres::{Client, NoTls};

    static SCHEMA: &'static str = include_str!("./schema.sql");

    async fn test_pg(cstr: &str) -> Client {
        let (client, connection) = tokio_postgres::connect(cstr, NoTls)
            .await
            .expect("unable to start test database");
        tokio::spawn(connection);
        client
            .batch_execute(SCHEMA)
            .await
            .expect("resetting schema");
        client
    }

    #[tokio::test]
    async fn test_ddec_empty() {
        let mut db = PostgreSQL::new("16.2.3".parse().unwrap(), Settings::default());
        db.setup().await.expect("setting up pg");
        db.start().await.expect("starting pg");
        db.create_database("dozer-test")
            .await
            .expect("creating test db");
        let pg = test_pg(&db.settings().url("dozer-test")).await;

        let encoded_lengths =
            fixed_bytes!("0000000000000000000000000000000000000000000000000000000000000000");
        let dynamic_data = &[0u8; 0];
        let row = pg
            .query_one("select ddec($1, $2, 0)", &[&encoded_lengths, &dynamic_data])
            .await
            .expect("issue with query");
        let res: &[u8] = row.get(0);
        assert_eq!(&[0u8; 0], res)
    }

    #[tokio::test]
    async fn test_ddec() {
        let mut db = PostgreSQL::new("16.2.3".parse().unwrap(), Settings::default());
        db.setup().await.expect("setting up pg");
        db.start().await.expect("starting pg");
        db.create_database("dozer-test")
            .await
            .expect("creating test db");
        let pg = test_pg(&db.settings().url("dozer-test")).await;

        let encoded_lengths =
            fixed_bytes!("0000000000000000000000000000000000000020000000004000000000000060");
        let dynamic_data = &[1u8; 96];
        let row = pg
            .query_one("select ddec($1, $2, 0)", &[&encoded_lengths, &dynamic_data])
            .await
            .expect("issue with query");
        let res: &[u8] = row.get(0);
        assert_eq!(&[1u8; 64], res);

        let row = pg
            .query_one("select ddec($1, $2, 1)", &[&encoded_lengths, &dynamic_data])
            .await
            .expect("issue with query");
        let res: &[u8] = row.get(0);
        assert_eq!(&[1u8; 32], res)
    }
}
