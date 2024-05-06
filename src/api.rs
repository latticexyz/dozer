use crate::api_error::ApiError;
use crate::schema::Schema;
use crate::sql::ParsedQuery;

use alloy::hex::{self};
use axum::{extract::State, http::StatusCode, Json};
use deadpool_postgres::Pool;
use eyre::Context;
use ruint::aliases::{U256, U64};
use serde::Deserialize;
use serde_json::Value;
use tokio_postgres::types::{ToSql, Type};

#[derive(Clone, Debug)]
pub struct Config {
    pub pool: Pool,
}

#[derive(Deserialize)]
pub struct GetRecsReq {
    pub query: String,
    pub values: Vec<Value>,
}

pub async fn query(
    State(state): State<Config>,
    Json(req): Json<GetRecsReq>,
) -> Result<Json<Vec<Value>>, ApiError> {
    let pg = state.pool.get().await.wrap_err("getting conn from pool")?;
    let parsed_query = ParsedQuery::new(&req.query)?;
    let schema = Schema::from_pg(&pg, parsed_query.tables()?).await?;
    let query = parsed_query.enhance(&schema)?;

    let mut vals = Vec::<Box<dyn ToSql + Sync + Send>>::new();
    for val in req.values {
        match val {
            Value::Number(i) => vals.push(Box::new(U64::from(i.as_u64().unwrap()))),
            Value::String(s) => vals.push(Box::new(hex::decode(s).unwrap())),
            _ => {
                return Err(ApiError::User(
                    StatusCode::BAD_REQUEST,
                    String::from("values must be string or number"),
                ))
            }
        }
    }
    let conn = state.pool.get().await.wrap_err("getting conn from pool")?;
    let vals = vals
        .iter()
        .map(|x| x.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    let rows = conn
        .query(dbg!(&query), &vals[..])
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
                _ => Value::Null,
            };
            row_json.insert(key, value);
        }
        result.push(Value::Object(row_json))
    }
    Ok(Json(result))
}
