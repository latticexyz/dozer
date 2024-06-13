use crate::{api, mud_schema};

use alloy::{hex, primitives::FixedBytes};
use axum::{extract::State, Json};
use eyre::{Context, Result};
use itertools::Itertools;
use ruint::aliases::{U256, U64};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::types::{ToSql, Type};

#[derive(Deserialize, Serialize)]
pub struct GetRecsReq {
    pub address: FixedBytes<20>,
    pub query: String,
    pub values: Vec<Value>,
}

#[tracing::instrument(skip_all)]
pub async fn handle(
    State(state): State<api::Config>,
    api::Json(req): api::Json<GetRecsReq>,
) -> Result<Json<Vec<Value>>, api::Error> {
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
    let rows = pg
        .query(
            &mud_schema::query::enhance(&pg, req.address, &req.query).await?,
            &vals
                .iter()
                .map(|x| x.as_ref() as &(dyn ToSql + Sync))
                .collect_vec(),
        )
        .await
        .wrap_err("querying records table")?;

    let mut result: Vec<Value> = Vec::new();
    for row in rows {
        let mut row_json = serde_json::Map::new();
        for (idx, column) in row.columns().iter().enumerate() {
            let key = column.name().to_string();
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
                    Value::String(hex::encode(b))
                }
                Type::TEXT => {
                    let s: String = row.get(idx);
                    Value::String(s)
                }
                _ => Value::Null,
            };
            row_json.insert(key, value);
        }
        result.push(Value::Object(row_json))
    }
    Ok(Json(result))
}
