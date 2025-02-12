use std::convert::Infallible;

use crate::{api, mud_schema, preformat_sql};

use alloy::{
    hex,
    primitives::{Address, Bytes},
};
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
use ruint::aliases::{U256, U64};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::Type;

type Row = Vec<Value>;
type Rows = Vec<Row>;

#[derive(Clone, Deserialize, Serialize)]
pub struct Request {
    pub block_height: Option<u64>,
    pub address: Address,
    pub query: String,
}

#[derive(Deserialize, Serialize)]
pub struct Response {
    pub block_height: u64,
    pub result: Vec<Rows>,
}

pub async fn handle_sse(
    State(conf): State<api::Config>,
    Form(req): Form<Request>,
) -> axum::response::Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let mut req = req.clone();
    let mut rx = conf.broadcaster.add();
    let stream = async_stream::stream! {
        loop {
            let resp = handle(State(conf.clone()), api::Json(vec![req.clone()])).await.expect("unable to make request");
            let last_block = resp.0.block_height;
            yield Ok(Event::default().json_data(resp.0).expect("unable to seralize json"));
            rx.recv().await.expect("unable to receive new block update");
            req.block_height = Some(last_block+1);
        }
    };
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[tracing::instrument(skip_all)]
pub async fn handle(
    State(state): State<api::Config>,
    api::Json(req): api::Json<Vec<Request>>,
) -> Result<Json<Response>, api::Error> {
    let mut pg = state.pool.get().await.wrap_err("getting conn from pool")?;
    let pgtx = pg
        .build_transaction()
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await
        .wrap_err("starting sql api read tx")?;
    let mut res: Vec<Rows> = Vec::new();
    for r in req {
        let preformatted_query = preformat_sql::preformat(&r.query);
        let query =
            mud_schema::query::enhance(&pgtx, r.address, r.block_height, &preformatted_query)
                .await?;

        res.push(handle_rows(pgtx.query(&dbg!(query), &[]).await?)?);
    }
    Ok(Json(Response {
        block_height: pgtx
            .query_one("select max(num)::text from blocks", &[])
            .await?
            .get::<usize, U64>(0)
            .to::<u64>(),
        result: res,
    }))
}

fn handle_rows(rows: Vec<tokio_postgres::Row>) -> Result<Rows, api::Error> {
    let mut result: Rows = Vec::new();
    if let Some(first) = rows.first() {
        result.push(
            first
                .columns()
                .iter()
                .map(|c| Value::String(c.name().to_string()))
                .collect(),
        );
    }
    for row in rows {
        let mut json_row: Vec<Value> = Vec::new();
        for (idx, column) in row.columns().iter().enumerate() {
            let value = match *column.type_() {
                Type::BOOL => {
                    let b: bool = row.get(idx);
                    Value::Bool(b)
                }
                Type::NUMERIC => {
                    let s: pg_bigdecimal::PgNumeric = row.get(idx);
                    Value::String(s.n.unwrap().to_string())
                }
                Type::INT2 | Type::INT4 | Type::INT8 => {
                    let n: i64 = match column.type_() {
                        &Type::INT4 => row.get::<_, i32>(idx) as i64,
                        _ => row.get(idx),
                    };
                    Value::Number(n.into())
                }
                Type::BYTEA => {
                    let b: &[u8] = row.get(idx);
                    Value::String(hex::encode_prefixed(b))
                }
                Type::TEXT => {
                    let s: String = row.get(idx);
                    Value::String(s)
                }
                Type::NUMERIC_ARRAY => {
                    let nums: Vec<U256> = row.get(idx);
                    serde_json::json!(nums.iter().map(|n| n.to_string()).collect::<Vec<String>>())
                }
                Type::BYTEA_ARRAY => {
                    let arrays: Vec<Vec<u8>> = row.get::<usize, Vec<Vec<u8>>>(idx);
                    serde_json::json!(arrays
                        .iter()
                        .map(|array| Bytes::copy_from_slice(array))
                        .collect_vec())
                }
                _ => Value::Null,
            };
            json_row.push(value);
        }
        result.push(json_row)
    }
    Ok(result)
}

pub mod cli {
    use crate::api::client_post;
    use alloy::primitives::Address;
    use clap::Args;
    use eyre::Result;
    use itertools::Itertools;
    use reqwest::Client;
    use serde_json::Value;
    use std::io::Write;
    use url::Url;

    pub const HELP: &str = include_str!("./cli-help/query.txt");

    #[derive(Args, Debug)]
    pub struct Request {
        #[arg(from_global)]
        url: Url,

        pub query: String,

        #[arg(short, long, help = "world address", env = "DOZER_ADDRESS")]
        pub address: Address,

        #[arg(short = 'b', help = "print block height at query")]
        pub block_height: bool,
    }

    pub async fn request(http_client: &Client, args: Request) -> Result<()> {
        let req_body = super::Request {
            block_height: None,
            address: args.address,
            query: args.query,
        };

        let mut req_path = args.url.clone();
        req_path.set_path("/q");
        let res = client_post::<super::Response, _>(http_client, req_path, &vec![req_body]).await?;
        let rows = res.result.first().expect("no rows returned");

        if args.block_height {
            println!("block height: {}", res.block_height)
        }
        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        let out = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|r| match r {
                        Value::Array(_) => r
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|item| item.as_str().unwrap_or_default().to_string())
                            .join(","),
                        Value::Bool(b) => format!("{}", b),
                        _ => r.as_str().unwrap_or_default().to_string(),
                    })
                    .collect::<Vec<String>>()
                    .join("\t")
            })
            .join("\n");
        writeln!(tw, "{}", out).expect("unable to write to stdout");
        tw.flush().expect("unable to write to stdout");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;

    #[tokio::test]
    async fn test_api_sql_handle() {
        let (_pg_server, mut pg) = test_utils::test_pg().await;
        let pgtx = pg.transaction().await.expect("opening index tx");

        // Testing that dynamic_data is truncated by encoded_lengths. The tested scenarios include:
        // 1. encoded_lengths: empty, dynamic_data: empty
        // 2. encoded_lengths: empty, dynamic_data: non-empty
        // 3. encoded_lengths: non-empty, dynamic_data: non-empty
        let address = hex!("0000000000000000000000000000000000000001").as_slice();
        let table_id =
            hex!("74626170700000000000000000000000546573745461626c6500000000000000").as_slice();

        pgtx.execute(
            r#"INSERT INTO records (address, table_id, key, dynamic_data, static_data, encoded_lengths, block_num, log_idx, expired)
            VALUES
            ($1, $2, '\x0000000000000000000000000000000000000000000000000000000000000001', '\x', $4, '\x0000000000000000000000000000000000000000000000000000000000000000', 0, 0, false),
            ($1, $2, '\x0000000000000000000000000000000000000000000000000000000000000002', $3, $4, '\x0000000000000000000000000000000000000000000000000000000000000000', 1, 0, false),
            ($1, $2, '\x0000000000000000000000000000000000000000000000000000000000000003', $3, $4, '\x0000000000000000000000000000000000000000000000004000000000000040', 1, 0, false)"#,
            &[
                &address,
                &table_id,
                &hex!("00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000002").as_slice(), // dynamic_data
                &hex!("0000000000000000000000000000000000000000000000000000000000000002").as_slice(), // static_data
            ],
        )
        .await
        .expect("setting up records table");

        pgtx.execute(
            r#"
            INSERT INTO tables (
                block_num,
                log_idx,
                address,
                id,
                name,
                key_schema,
                val_schema,
                key_names,
                val_names
            ) VALUES (
                0,
                0,
                $1,
                $2,
                'TestTable',
                '\x002001001F000000000000000000000000000000000000000000000000000000',
                '\x002001011F810000000000000000000000000000000000000000000000000000',
                '{column1}',
                '{column2,column3}'
            )"#,
            &[&address, &table_id],
        )
        .await
        .expect("setting up tables table");

        let req = vec![Request {
            block_height: None,
            address: Address::from_slice(address),
            query: "select column1, column2, column3 from app__TestTable".to_string(),
        }];

        let mut res: Vec<Rows> = Vec::new();
        for r in req {
            let preformatted_query = preformat_sql::preformat(&r.query);
            let query =
                mud_schema::query::enhance(&pgtx, r.address, r.block_height, &preformatted_query)
                    .await
                    .expect("enhancing query");

            res.push(
                handle_rows(pgtx.query(&dbg!(query), &[]).await.expect("querying"))
                    .expect("handling rows"),
            );
        }

        let query_res = &res[0];
        let columns = &query_res[0];
        assert_eq!(columns[0].as_str().unwrap(), "column1");
        assert_eq!(columns[1].as_str().unwrap(), "column2");
        assert_eq!(columns[2].as_str().unwrap(), "column3");

        let empty_dynamic_data = &query_res[1];
        assert_eq!(empty_dynamic_data[0].as_str().unwrap(), "1");
        assert_eq!(empty_dynamic_data[1].as_str().unwrap(), "2");
        assert_eq!(empty_dynamic_data[2], Value::Array(vec![]));

        let mismatched_dynamic_data = &query_res[2];
        assert_eq!(mismatched_dynamic_data[0].as_str().unwrap(), "2");
        assert_eq!(mismatched_dynamic_data[1].as_str().unwrap(), "2");
        assert_eq!(mismatched_dynamic_data[2], Value::Array(vec![]));

        let non_empty_dynamic_data = &query_res[3];
        assert_eq!(non_empty_dynamic_data[0].as_str().unwrap(), "3");
        assert_eq!(non_empty_dynamic_data[1].as_str().unwrap(), "2");
        assert_eq!(
            non_empty_dynamic_data[2],
            Value::Array(vec![
                Value::String("1".to_string()),
                Value::String("2".to_string())
            ])
        );
    }
}
