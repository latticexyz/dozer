use crate::api_error::ApiError;
use crate::schema::Schema;
use crate::sql::ParsedQuery;

use alloy::{
    hex,
    primitives::{Bytes, FixedBytes},
};
use axum::{extract::Query, extract::State, http::StatusCode, Json};
use deadpool_postgres::Pool;
use eyre::{Context, Result};
use ruint::aliases::{U256, U64};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{
    types::{ToSql, Type},
    Row,
};

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

#[derive(Deserialize, Debug)]
pub struct LogsRequest {
    input: String,
}

#[derive(Serialize, Debug)]
pub struct LogsResponseArg {
    #[serde(rename = "tableId")]
    table_id: FixedBytes<32>,
    #[serde(rename = "keyTuple")]
    key_tuple: Vec<FixedBytes<32>>,
    #[serde(rename = "staticData")]
    static_data: Bytes,
    #[serde(rename = "encodedLengths")]
    encoded_lengths: FixedBytes<32>,
    #[serde(rename = "dynamicData")]
    dynamic_data: Bytes,
}

#[derive(Serialize, Debug)]
pub struct LogsResponse {
    address: FixedBytes<20>,
    #[serde(rename = "eventName")]
    event_name: String,
    args: LogsResponseArg,
}

impl LogsResponse {
    fn from_row(row: &Row) -> Result<Self, tokio_postgres::Error> {
        Ok(LogsResponse {
            address: row.try_get("address")?,
            event_name: String::from("Store_SetRecord"),
            args: LogsResponseArg {
                table_id: row.try_get("table_id")?,
                key_tuple: row.try_get("key")?,
                static_data: Bytes::copy_from_slice(row.try_get("static_data")?),
                encoded_lengths: row.try_get("encoded_lengths")?,
                dynamic_data: Bytes::copy_from_slice(row.try_get("dynamic_data")?),
            },
        })
    }
}

pub async fn logs(
    State(state): State<Config>,
    Query(query): Query<LogsRequest>,
) -> Result<Json<Vec<LogsResponse>>, ApiError> {
    let req_input: LogsRequestInput = serde_json::from_str(&query.input)?;
    let pg = state.pool.get().await.wrap_err("unable to get pg conn")?;
    let q = format!(
        r#"
        select
            address,
            table_id,
            key,
            static_data,
            encoded_lengths,
            dynamic_data
        from records
        where expired_block_num is null
        and ({})
    "#,
        req_input.to_sql().unwrap(),
    );
    let res: Vec<LogsResponse> = pg
        .query(&q, &[])
        .await?
        .iter()
        .map(|r| LogsResponse::from_row(r))
        .collect::<Result<_, _>>()?;
    Ok(Json(res))
}

#[derive(Debug, Deserialize)]
struct LogsRequestFilter {
    address: Option<FixedBytes<20>>,
    #[serde(rename = "tableId")]
    table_id: Option<FixedBytes<32>>,
    key0: Option<FixedBytes<32>>,
    key1: Option<FixedBytes<32>>,
}

impl LogsRequestFilter {
    pub fn to_sql(&self) -> Option<String> {
        let mut stmts = Vec::new();
        if let Some(a) = self.address {
            stmts.push(format!(r#"address = '\x{}'"#, hex::encode(a)))
        }
        if let Some(t) = self.table_id {
            stmts.push(format!(r#"table_id = '\x{}'"#, hex::encode(t)))
        }
        if let Some(k) = &self.key0 {
            stmts.push(format!(r#"key[1] = '\x{}'"#, hex::encode(k)))
        }
        if let Some(k) = &self.key1 {
            stmts.push(format!(r#"key[2] = '\x{}'"#, hex::encode(k)))
        }
        if stmts.len() > 0 {
            Some(format!("({})", stmts.join(" and ")))
        } else {
            None
        }
    }
}

#[derive(Debug, Deserialize)]
struct LogsRequestInput {
    #[serde(rename = "chainId")]
    pub chain_id: Option<u64>,
    pub address: Option<FixedBytes<20>>,
    pub filters: Option<Vec<LogsRequestFilter>>,
}

impl LogsRequestInput {
    pub fn to_sql(&self) -> Option<String> {
        if let Some(filters) = &self.filters {
            Some(
                filters
                    .iter()
                    .filter_map(|f| f.to_sql())
                    .collect::<Vec<String>>()
                    .join(" or "),
            )
        } else if let Some(address) = &self.address {
            Some(format!(r#"address='\x{}'"#, hex::encode(address)))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_filter_sql() {
        let mut filter = LogsRequestFilter {
            table_id: None,
            address: None,
            key0: None,
            key1: None,
        };
        assert_eq!(None, filter.to_sql());
        filter.table_id = Some(FixedBytes::<32>::with_last_byte(1));
        assert_eq!(
            r#"(table_id = '\x0000000000000000000000000000000000000000000000000000000000000001')"#,
            filter.to_sql().unwrap()
        );
        filter.address = Some(FixedBytes::<20>::with_last_byte(1));
        assert_eq!(
            r#"(address = '\x0000000000000000000000000000000000000001' and table_id = '\x0000000000000000000000000000000000000000000000000000000000000001')"#,
            filter.to_sql().unwrap()
        );
        filter.key0 = Some(FixedBytes::<32>::with_last_byte(1));
        assert_eq!(
            r#"(address = '\x0000000000000000000000000000000000000001' and table_id = '\x0000000000000000000000000000000000000000000000000000000000000001' and key[1] = '\x0000000000000000000000000000000000000000000000000000000000000001')"#,
            filter.to_sql().unwrap()
        );
        filter.key1 = Some(FixedBytes::<32>::with_last_byte(2));
        assert_eq!(
            r#"(address = '\x0000000000000000000000000000000000000001' and table_id = '\x0000000000000000000000000000000000000000000000000000000000000001' and key[1] = '\x0000000000000000000000000000000000000000000000000000000000000001' and key[2] = '\x0000000000000000000000000000000000000000000000000000000000000002')"#,
            filter.to_sql().unwrap()
        );
    }
}
