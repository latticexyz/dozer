use crate::{api, mud_schema};

use alloy::{hex, primitives::Address};
use axum::{extract::State, Json};
use eyre::{Context, Result};
use itertools::Itertools;
use ruint::aliases::{U256, U64};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::{ToSql, Type};

#[derive(Deserialize, Serialize)]
pub struct Request {
    pub address: Address,
    pub query: String,
    pub values: Vec<Value>,
}

#[tracing::instrument(skip_all)]
pub async fn handle(
    State(state): State<api::Config>,
    api::Json(req): api::Json<Request>,
) -> Result<Json<Vec<Vec<Value>>>, api::Error> {
    let mut vals = Vec::<Box<dyn ToSql + Sync + Send>>::new();
    for val in req.values {
        match val {
            Value::Number(i) => vals.push(Box::new(U64::from(i.as_u64().unwrap()))),
            Value::String(s) => vals.push(Box::new(hex::decode(s).unwrap())),
            _ => {
                return Err(api::Error::User(
                    "values must be string or number".to_string(),
                ))
            }
        }
    }

    let pg = state.pool.get().await.wrap_err("getting conn from pool")?;
    let query = mud_schema::query::enhance(&pg, req.address, &req.query).await?;

    let rows = pg
        .query(
            &dbg!(query),
            &vals
                .iter()
                .map(|x| x.as_ref() as &(dyn ToSql + Sync))
                .collect_vec(),
        )
        .await
        .wrap_err("querying records table")?;

    let mut result: Vec<Vec<Value>> = Vec::new();
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
                Type::NUMERIC => {
                    let n: U256 = row.get(idx);
                    Value::String(n.to_string())
                }
                Type::INT2 | Type::INT4 | Type::INT8 => {
                    let n: i64 = row.get(idx);
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
                _ => Value::Null,
            };
            json_row.push(value);
        }
        result.push(json_row)
    }
    Ok(Json(result))
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

    #[derive(Args, Debug)]
    pub struct Request {
        #[clap(
            short,
            long,
            global = true,
            env = "DOZER_URL",
            default_value = "http://0.0.0.0:8000"
        )]
        dozer_url: Url,

        pub query: String,

        #[arg(short, long, env = "DOZER_ADDRESS")]
        pub address: Address,
    }

    pub async fn request(http_client: &Client, args: Request) -> Result<()> {
        let req_body = super::Request {
            address: args.address,
            query: args.query,
            values: vec![],
        };

        let mut req_path = args.dozer_url.clone();
        req_path.set_path("/q");
        let res = client_post::<Vec<Vec<Value>>, _>(&http_client, req_path, &req_body).await?;

        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        let out = res
            .iter()
            .map(|row| {
                row.iter()
                    .map(|r| r.as_str().unwrap_or_default().to_string())
                    .collect::<Vec<String>>()
                    .join("\t")
            })
            .join("\n");
        writeln!(tw, "{}", out).expect("unable to write to stdout");
        tw.flush().expect("unable to write to stdout");
        Ok(())
    }
}
