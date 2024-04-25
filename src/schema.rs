use crate::api_error::ApiError;
use alloy::{primitives::FixedBytes, sol, sol_types::SolType};
use axum::http::StatusCode;
use eyre::{eyre, Result, WrapErr};
use ruint::aliases::U64;
use tokio_postgres::{Client, Row, Transaction};

pub struct Schema {
    pub table_id: FixedBytes<32>,
    pub key_names: Vec<String>,
    pub val_names: Vec<String>,
    pub key_schema: FixedBytes<32>,
    pub val_schema: FixedBytes<32>,
}

fn static_len(schema_type: u8) -> usize {
    match schema_type {
        _ if schema_type > 97 => 0,
        97 => 20,
        _ => (schema_type as usize & 31) + 1,
    }
}

impl Schema {
    pub fn from_data(table_id: FixedBytes<32>, data: &Data) -> Result<Self> {
        type SolArrayOf<T> = sol! { T[] };
        Ok(Schema {
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
    pub async fn insert(&self, tx: &Transaction<'_>, block_num: u64, log_idx: u64) -> Result<()> {
        const Q: &str = r#"
            insert into tables(block_num, log_idx, id, name, key_schema, val_schema, key_names, val_names)
            values ($1, $2, $3, $4, $5, $6, $7, $8)
        "#;
        tx.execute(
            Q,
            &[
                &U64::from(block_num),
                &U64::from(log_idx),
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

    pub fn nstatic(&self) -> usize {
        let mut b = [0u8; 2];
        b[0] = self.val_schema[0];
        b[1] = self.val_schema[1];
        i16::from_be_bytes(b) as usize
    }

    #[allow(dead_code)]
    pub fn ndynamic(&self) -> usize {
        self.val_schema[2] as usize
    }

    fn sstart(&self, pos: usize) -> usize {
        self.val_schema
            .iter()
            .skip(4)
            .take(pos)
            .map(|f| static_len(*f))
            .sum()
    }

    pub fn get_col_sql(&self, name: &str) -> Result<String> {
        let pos = self
            .val_names
            .iter()
            .position(|n| n == name)
            .ok_or(eyre!(format!(
                "table: {} has no column named: {}",
                self.table_name(),
                name
            )))?;
        if pos >= self.nstatic() {
            return Err(eyre!(format!(
                "{}/{} is dynamic. only static works for now",
                self.table_name(),
                name
            )));
        }
        Ok(format!(
            "b2n(sdec(static_data, {}, {})) as {}",
            self.sstart(pos) + 1, //sdec assumes 1-index
            static_len(self.val_schema[4 + pos]),
            name,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::fixed_bytes;

    #[test]
    fn test_byte_len() {
        assert_eq!(04, static_len(0x03));
        assert_eq!(32, static_len(0x1f));
        assert_eq!(32, static_len(0x3f));
        assert_eq!(01, static_len(0x60));
        assert_eq!(20, static_len(0x61));
    }

    #[test]
    fn test_sstart() {
        let schema = &Schema {
            table_id: fixed_bytes!(
                "74620000000000000000000000000000436f756e746572000000000000000000"
            ),
            key_names: vec![],
            val_names: vec![String::from("value")],
            key_schema: fixed_bytes!(),
            val_schema: fixed_bytes!(
                "0004010003000000000000000000000000000000000000000000000000000000"
            ),
        };
        assert_eq!(schema.sstart(0), 0)
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
