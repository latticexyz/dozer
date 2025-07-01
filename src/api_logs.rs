use std::convert::Infallible;

use crate::api;

use alloy::primitives::{fixed_bytes, Bytes, FixedBytes};
use axum::{
    extract::State,
    response::{
        sse::{Event, KeepAlive},
        Sse,
    },
    Json,
};
use axum_extra::extract::Form;
use eyre::{Context, Result};
use futures::Stream;
use itertools::Itertools;
use ruint::aliases::U64;
use serde::{Deserialize, Serialize, Serializer};
use tokio_postgres::{types::ToSql, Row};

fn u64_to_string<S>(x: &u64, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&x.to_string())
}

#[derive(Clone, Deserialize, Debug)]
pub struct LogsRequest {
    input: String,
    from_block_num: Option<u64>,
    to_block_num: Option<u64>,
    block_num: Option<u64>, // deprecated, but kept for backwards compat
    include_tx_hash: Option<bool>,
}

#[derive(Serialize, Debug)]
pub struct LogArg {
    #[serde(rename = "tableId")]
    table_id: FixedBytes<32>,
    #[serde(rename = "keyTuple")]
    key_tuple: Vec<FixedBytes<32>>,
    #[serde(rename = "staticData", skip_serializing_if = "Option::is_none")]
    static_data: Option<Bytes>,
    #[serde(rename = "encodedLengths", skip_serializing_if = "Option::is_none")]
    encoded_lengths: Option<Bytes>,
    #[serde(rename = "dynamicData", skip_serializing_if = "Option::is_none")]
    dynamic_data: Option<Bytes>,
}

#[derive(Serialize, Debug)]
pub struct Log {
    address: FixedBytes<20>,
    #[serde(rename = "eventName")]
    event_name: String,
    args: LogArg,

    #[serde(skip_serializing)]
    block_num: U64,
    #[serde(rename = "transactionHash", skip_serializing_if = "Option::is_none")]
    tx_hash: Option<FixedBytes<32>>,
    #[serde(skip_serializing)]
    log_idx: U64,
}

impl Log {
    fn from_row(row: &Row) -> Result<Self, tokio_postgres::Error> {
        let key: Vec<u8> = row.try_get("key")?;
        let key: Vec<FixedBytes<32>> = key
            .chunks(32)
            .map(|chunk| FixedBytes::<32>::from_slice(chunk))
            .collect();
        let (ename, sd, el, dd) = if row.get("deleted") {
            (String::from("Store_DeleteRecord"), None, None, None)
        } else {
            (
                String::from("Store_SetRecord"),
                Some(Bytes::from(row.try_get::<&str, Vec<u8>>("static_data")?)),
                Some(Bytes::from(
                    row.try_get::<&str, Vec<u8>>("encoded_lengths")?,
                )),
                Some(Bytes::from(row.try_get::<&str, Vec<u8>>("dynamic_data")?)),
            )
        };
        Ok(Log {
            address: row.try_get("address")?,
            event_name: ename,
            block_num: row.try_get("block_num")?,
            tx_hash: row.try_get("tx_hash").ok(),
            log_idx: row.try_get("log_idx")?,
            args: LogArg {
                table_id: row.try_get("table_id")?,
                key_tuple: key,
                static_data: sd,
                encoded_lengths: el,
                dynamic_data: dd,
            },
        })
    }
}

#[derive(Serialize, Debug)]
pub struct LogsResponse {
    #[serde(serialize_with = "u64_to_string", rename = "blockNumber")]
    block_num: u64,
    logs: Vec<Log>,
}

pub async fn handle_sse(
    State(conf): State<api::Config>,
    Form(req): Form<LogsRequest>,
) -> axum::response::Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut req = req.clone();
    req.include_tx_hash = Some(true);
    let mut rx = conf.broadcaster.add();
    let stream = async_stream::stream! {
        loop {
            let resp = handle(State(conf.clone()), Form(req.clone()))
                .await
                .expect("unable to make request");
            let last_block = resp.block_num;
            yield Ok(Event::default()
                .json_data(resp.0)
                .expect("unable to seralize json"));
            rx.recv().await.expect("unable to receive new block update");
            req.block_num = Some(last_block + 1);
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[tracing::instrument(skip_all)]
pub async fn handle(
    State(state): State<api::Config>,
    Form(query): Form<LogsRequest>,
) -> Result<Json<LogsResponse>, api::Error> {
    let include_tx_hash = query.include_tx_hash;
    let req_input: LogsRequestInput = serde_json::from_str(&query.input)?;

    let pg = state.pool.get().await.wrap_err("unable to get pg conn")?;

    // fall back to `block_num` for backwards-compatibility
    let from_block = query.from_block_num.or(query.block_num);
    let to_block = match query.to_block_num {
        Some(to) => Some(to),
        None => {
            let row = pg.query_one("select max(num) from blocks", &[]).await?;
            Some(row.get::<_, U64>(0).try_into().unwrap())
        }
    };

    let query = LogsQuery::new(from_block, to_block, req_input);

    let params: &[&(dyn ToSql + Sync)] = &query
        .params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>()[..];

    let res: Vec<Log> = pg
        .query(&query.to_sql(include_tx_hash.unwrap_or(false)), params)
        .await?
        .iter()
        .map(Log::from_row)
        .collect::<Result<Vec<Log>, _>>()?
        .into_iter()
        .sorted_by_key(|l| (l.block_num, l.log_idx))
        .collect_vec();

    Ok(Json(LogsResponse {
        block_num: to_block.unwrap().into(),
        logs: res,
    }))
}

#[derive(Debug, Deserialize)]
struct LogsRequestFilter {
    #[serde(rename = "tableId")]
    table_id: Option<FixedBytes<32>>,
    key0: Option<FixedBytes<32>>,
    key1: Option<FixedBytes<32>>,
}

#[derive(Debug, Deserialize)]
struct LogsRequestInput {
    #[serde(rename = "chainId")]
    pub _chain_id: Option<u64>,
    pub address: Option<FixedBytes<20>>,
    pub filters: Option<Vec<LogsRequestFilter>>,
}

type Param = (dyn ToSql + Sync + Send);

#[derive(Default, Debug)]
struct LogsQuery {
    from_block_num: Option<u64>,
    to_block_num: Option<u64>,
    or_predicates: Vec<String>,
    and_predicates: Vec<String>,
    num_params: i32,
    params: Vec<Box<Param>>,
}

impl LogsQuery {
    fn new(
        from_block_num: Option<u64>,
        to_block_num: Option<u64>,
        input: LogsRequestInput,
    ) -> Self {
        let mut query = LogsQuery {
            from_block_num,
            to_block_num,
            num_params: 0,
            and_predicates: vec![],
            or_predicates: vec![],
            params: vec![],
        };
        if input.address.is_some() {
            query.num_params += 1;
            query.params.push(Box::new(input.address))
        }
        if let Some(mut filters) = input.filters {
            if !filters.is_empty() {
                filters.push(LogsRequestFilter {
                    table_id: Some(fixed_bytes!(
                        "746273746f72650000000000000000005461626c657300000000000000000000"
                    )),
                    key0: None,
                    key1: None,
                });
            }
            filters.iter().for_each(|f| {
                if f.table_id.is_some() {
                    query.add_filter_field("table_id", Box::new(f.table_id));
                }
                if f.key0.is_some() {
                    query.add_filter_field("sdec(key, 0, 32)", Box::new(f.key0))
                }
                if f.key1.is_some() {
                    query.add_filter_field("sdec(key, 32, 32)", Box::new(f.key1))
                }
                query.add_filter();
            });
        }
        query
    }

    fn add_filter_field(&mut self, field: &str, param: Box<Param>) {
        self.params.push(param);
        self.num_params += 1;
        self.and_predicates
            .push(String::from(&format!("{} = ${}", field, self.num_params)));
    }

    fn add_filter(&mut self) {
        self.or_predicates
            .push(format!("({})", self.and_predicates.join(" and ")));
        self.and_predicates.clear();
    }

    fn filters_sql(&self) -> String {
        if !self.or_predicates.is_empty() {
            format!("and ({})", self.or_predicates.join(" or "))
        } else {
            "".to_string()
        }
    }

    fn to_sql(&self, include_tx_hash: bool) -> String {
        let to_block_predicate = if let Some(to_block) = self.to_block_num {
            format!("and block_num <= {}", to_block)
        } else {
            String::new()
        };

        let from_block_predicate = if let Some(from_block) = self.from_block_num {
            format!("and block_num >= {}", from_block)
        } else {
            String::new()
        };

        let tx_hash = if include_tx_hash {
            String::from("tx_hash,")
        } else {
            String::new()
        };

        let sql = format!(
            r#"
            select
                r.block_num,
                r.log_idx,
                {tx_hash}
                r.address,
                r.table_id,
                r.key,
                r.static_data,
                CASE
                    WHEN r.encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                    THEN '\x00'::bytea
                    ELSE r.encoded_lengths
                END AS encoded_lengths,
                CASE
                    WHEN r.encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                    THEN '\x'::bytea
                    ELSE substring(r.dynamic_data, 1,
                        (get_byte(r.encoded_lengths, 25) << 48) |
                        (get_byte(r.encoded_lengths, 26) << 40) |
                        (get_byte(r.encoded_lengths, 27) << 32) |
                        (get_byte(r.encoded_lengths, 28) << 24) |
                        (get_byte(r.encoded_lengths, 29) << 16) |
                        (get_byte(r.encoded_lengths, 30) << 8) |
                        get_byte(r.encoded_lengths, 31))
                END AS dynamic_data,
                r.deleted
            from (
                select distinct on (table_id, key)
                    address, table_id, key, block_num, log_idx
                from records
                where address = $1
                {to_block_predicate}
                {from_block_predicate}
                {filters}
                order by table_id, key, block_num desc, log_idx desc
            ) latest
            join records r using (address, table_id, key, block_num, log_idx)
            order by block_num, log_idx, address, table_id, key
            "#,
            tx_hash = tx_hash,
            to_block_predicate = to_block_predicate,
            from_block_predicate = from_block_predicate,
            filters = self.filters_sql()
        );
        tracing::info!("SQL:\n\n{}", sql);
        return sql;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;
    use alloy::hex;

    #[tokio::test]
    async fn test_api_logs_handle() {
        let (_pg_server, mut pg) = test_utils::test_pg().await;
        let pgtx = pg.transaction().await.expect("opening index tx");

        pgtx.execute(
            r#"
            INSERT INTO records (address, table_id, key, dynamic_data, static_data, encoded_lengths, block_num, log_idx, expired)
            VALUES
                ($1, $2, '\x0000000000000000000000000000000000000000000000000000000000000001', $3, $4, '\x0000000000000000000000000000000000000000000000000000000000000000', 0, 0, false),
                ($1, $2, '\x0000000000000000000000000000000000000000000000000000000000000002', $3, $4, '\x0000000000000000000000000000000000000000000000000000000000000008', 1, 0, false)"#,
            &[
                &hex!("0000000000000000000000000000000000000001").as_slice(), // address
                &hex!("74626170700000000000000000000000546573745461626c6500000000000000").as_slice(), // table_id
                &hex!("AAAABBBBCCCCDDDDEEEEFFFF11112222333344445555666677778888DEADBEEF").as_slice(), // dynamic_data
                &hex!("0000000000000000000000000000000000000000000000000000000000000001").as_slice(), // static_data
            ],
        )
        .await
        .expect("setting up records table");

        let query = LogsQuery::new(
            None,
            None,
            LogsRequestInput {
                _chain_id: Some(690),
                address: Some(FixedBytes::<20>::with_last_byte(1)),
                filters: Some(vec![]),
            },
        );

        let params: &[&(dyn ToSql + Sync)] = &query
            .params
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>()[..];

        let res: Vec<Log> = pgtx
            .query(&query.to_sql(false), params)
            .await
            .unwrap()
            .iter()
            .map(Log::from_row)
            .collect::<Result<Vec<Log>, _>>()
            .unwrap()
            .into_iter()
            .sorted_by_key(|l| (l.block_num, l.log_idx))
            .collect_vec();

        assert_eq!(
            res.first().unwrap().args.dynamic_data,
            Some(Bytes::from(FixedBytes::<0>::ZERO))
        );
        // dynamic_data is longer than encoded_lengths, and gets truncated by encoded_lengths
        assert_eq!(
            res.last().unwrap().args.dynamic_data,
            Some(Bytes::from(fixed_bytes!("AAAABBBBCCCCDDDD")))
        );
    }

    #[tokio::test]
    async fn test_next_to_index() {
        let (_pg_server, mut pg) = test_utils::test_pg().await;
        let pgtx = pg.transaction().await.expect("opening index tx");
        pgtx.execute(
            "insert into blocks(num, hash) values ($1, $2)",
            &[&U64::from(0), &FixedBytes::<32>::ZERO],
        )
        .await
        .expect("setting up blocks table");
    }

    #[test]
    fn test_logs_query_empty_filters() {
        let query = LogsQuery::new(
            None,
            None,
            LogsRequestInput {
                _chain_id: Some(690),
                address: Some(FixedBytes::<20>::with_last_byte(1)),
                filters: Some(vec![]),
            },
        );
        assert_eq!(
            test_utils::fmt_sql(&query.to_sql(false)).expect("invalid sql"),
            test_utils::fmt_sql(
                r#"
                select
                    block_num,
                    log_idx,
                    address,
                    table_id,
                    key,
                    static_data,
                    CASE
                        WHEN encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                        THEN '\x00'::bytea
                        ELSE encoded_lengths
                    END AS encoded_lengths,
                    CASE
                        WHEN encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                        THEN '\x'::bytea
                        ELSE substring(dynamic_data, 1,
                         (get_byte(encoded_lengths, 25) << 48) |
                         (get_byte(encoded_lengths, 26) << 40) |
                         (get_byte(encoded_lengths, 27) << 32) |
                         (get_byte(encoded_lengths, 28) << 24) |
                         (get_byte(encoded_lengths, 29) << 16) |
                         (get_byte(encoded_lengths, 30) << 8) |
                         get_byte(encoded_lengths, 31))
                    END AS dynamic_data,
                    deleted
                from records
                where not expired
                and address = $1
                and not deleted
                "#
            )
            .unwrap()
        );
    }

    #[test]
    fn test_logs_query_from_block_num() {
        let query = LogsQuery::new(
            None,
            Some(42),
            LogsRequestInput {
                _chain_id: Some(690),
                address: Some(FixedBytes::<20>::with_last_byte(1)),
                filters: Some(vec![]),
            },
        );
        assert_eq!(
            test_utils::fmt_sql(&query.to_sql(false)).expect("invalid sql"),
            test_utils::fmt_sql(
                r#"
                select
                    block_num,
                    log_idx,
                    address,
                    table_id,
                    key,
                    static_data,
                    CASE
                        WHEN encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                        THEN '\x00'::bytea
                        ELSE encoded_lengths
                    END AS encoded_lengths,
                    CASE
                        WHEN encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                        THEN '\x'::bytea
                        ELSE substring(dynamic_data, 1,
                         (get_byte(encoded_lengths, 25) << 48) |
                         (get_byte(encoded_lengths, 26) << 40) |
                         (get_byte(encoded_lengths, 27) << 32) |
                         (get_byte(encoded_lengths, 28) << 24) |
                         (get_byte(encoded_lengths, 29) << 16) |
                         (get_byte(encoded_lengths, 30) << 8) |
                         get_byte(encoded_lengths, 31))
                    END AS dynamic_data,
                    deleted
                from records
                where not expired
                and address = $1
                and block_num >= 42
                "#
            )
            .unwrap()
        );
    }

    #[test]
    fn test_logs_query() {
        let query = LogsQuery::new(
            None,
            None,
            LogsRequestInput {
                _chain_id: Some(690),
                address: Some(FixedBytes::<20>::with_last_byte(1)),
                filters: Some(vec![LogsRequestFilter {
                    table_id: Some(FixedBytes::<32>::with_last_byte(1)),
                    key0: Some(FixedBytes::<32>::with_last_byte(1)),
                    key1: Some(FixedBytes::<32>::with_last_byte(1)),
                }]),
            },
        );
        assert_eq!(query.params.len(), 5);
        assert_eq!(
            test_utils::fmt_sql(&query.to_sql(false)).unwrap(),
            test_utils::fmt_sql(r#"
            SELECT
                block_num,
                log_idx,
                address,
                table_id,
                key,
                static_data,
                CASE
                    WHEN encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                    THEN '\x00'::bytea
                    ELSE encoded_lengths
                END AS encoded_lengths,
                CASE
                    WHEN encoded_lengths = '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
                    THEN '\x'::bytea
                    ELSE substring(dynamic_data, 1,
                         (get_byte(encoded_lengths, 25) << 48) |
                         (get_byte(encoded_lengths, 26) << 40) |
                         (get_byte(encoded_lengths, 27) << 32) |
                         (get_byte(encoded_lengths, 28) << 24) |
                         (get_byte(encoded_lengths, 29) << 16) |
                         (get_byte(encoded_lengths, 30) << 8) |
                         get_byte(encoded_lengths, 31))
                END AS dynamic_data,
                deleted
            FROM records
            WHERE NOT expired
            AND address = $1
            AND NOT deleted
            AND (
                (table_id = $2 AND sdec(key, 0, 32) = $3 AND sdec(key, 32, 32) = $4)
                OR
                (table_id = $5)
        )"#).unwrap());
    }
}
