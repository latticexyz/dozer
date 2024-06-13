use crate::{api_error::ApiError, mud_schema};

use alloy::{
    hex,
    primitives::{fixed_bytes, Bytes, FixedBytes},
};
use axum::{extract::Query, extract::State, Json};
use deadpool_postgres::Pool;
use eyre::{Context, Result};
use itertools::Itertools;
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

#[derive(Deserialize, Serialize)]
pub struct GetRecsReq {
    pub address: FixedBytes<20>,
    pub query: String,
    pub values: Vec<Value>,
}

#[tracing::instrument(skip_all)]
pub async fn query(
    State(state): State<Config>,
    Json(req): Json<GetRecsReq>,
) -> Result<Json<Vec<Value>>, ApiError> {
    let mut vals = Vec::<Box<dyn ToSql + Sync + Send>>::new();
    for val in req.values {
        match val {
            Value::Number(i) => vals.push(Box::new(U64::from(i.as_u64().unwrap()))),
            Value::String(s) => vals.push(Box::new(hex::decode(s).unwrap())),
            _ => {
                return Err(ApiError::User(
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

#[derive(Deserialize, Debug)]
pub struct LogsRequest {
    input: String,
}

#[derive(Serialize, Debug)]
pub struct LogArg {
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
pub struct Log {
    address: FixedBytes<20>,
    #[serde(rename = "eventName")]
    event_name: String,
    args: LogArg,

    #[serde(skip_serializing)]
    block_num: U64,
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
        Ok(Log {
            address: row.try_get("address")?,
            event_name: String::from("Store_SetRecord"),
            block_num: row.try_get("block_num")?,
            log_idx: row.try_get("log_idx")?,
            args: LogArg {
                table_id: row.try_get("table_id")?,
                key_tuple: key,
                static_data: Bytes::from(row.try_get::<&str, Vec<u8>>("static_data")?),
                encoded_lengths: row.try_get("encoded_lengths")?,
                dynamic_data: Bytes::from(row.try_get::<&str, Vec<u8>>("dynamic_data")?),
            },
        })
    }
}

#[derive(Serialize, Debug)]
pub struct LogsResponse {
    #[serde(rename = "blockNumber")]
    block_num: String,
    logs: Vec<Log>,
}

#[tracing::instrument(skip_all)]
pub async fn logs(
    State(state): State<Config>,
    Query(query): Query<LogsRequest>,
) -> Result<Json<LogsResponse>, ApiError> {
    let req_input: LogsRequestInput = serde_json::from_str(&query.input)?;
    let query = LogsQuery::new(req_input);

    let params: &[&(dyn ToSql + Sync)] = &query
        .params
        .iter()
        .map(|b| b.as_ref() as &(dyn ToSql + Sync))
        .collect::<Vec<_>>()[..];

    let pg = state.pool.get().await.wrap_err("unable to get pg conn")?;
    let res: Vec<Log> = pg
        .query(&query.to_sql(), params)
        .await?
        .iter()
        .map(|r| Log::from_row(r))
        .collect::<Result<Vec<Log>, _>>()?
        .into_iter()
        .sorted_by_key(|l| (l.block_num, l.log_idx))
        .collect_vec();

    let bres = pg
        .query_one("select max(num)::text from blocks", &[])
        .await?;
    Ok(Json(LogsResponse {
        block_num: bres.get(0),
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
    or_predicates: Vec<String>,
    and_predicates: Vec<String>,
    num_params: i32,
    params: Vec<Box<Param>>,
}

impl LogsQuery {
    fn new(input: LogsRequestInput) -> Self {
        let mut query = LogsQuery {
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
            filters.push(LogsRequestFilter {
                table_id: Some(fixed_bytes!(
                    "746273746f72650000000000000000005461626c657300000000000000000000"
                )),
                key0: None,
                key1: None,
            });
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
        self.and_predicates.push(String::from(&format!(
            "{} = ${}",
            field,
            self.num_params.to_string()
        )));
    }

    fn add_filter(&mut self) {
        self.or_predicates
            .push(format!("({})", self.and_predicates.join(" and ")));
        self.and_predicates.clear();
    }

    fn to_sql(&self) -> String {
        format!(
            "
            select
                block_num,
                log_idx,
                address,
                table_id,
                key,
                static_data,
                encoded_lengths,
                dynamic_data
            from records
            where not expired
            and not deleted
            and address = $1
            and ({})
            ",
            self.or_predicates.join(" or ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logs_query() {
        let query = LogsQuery::new(LogsRequestInput {
            _chain_id: Some(690),
            address: Some(FixedBytes::<20>::with_last_byte(1)),
            filters: Some(vec![LogsRequestFilter {
                table_id: Some(FixedBytes::<32>::with_last_byte(1)),
                key0: Some(FixedBytes::<32>::with_last_byte(1)),
                key1: Some(FixedBytes::<32>::with_last_byte(1)),
            }]),
        });
        assert_eq!(query.params.len(), 5);
        assert_eq!(
            query.or_predicates,
            vec![
                "(table_id = $2 and sdec(key, 0, 32) = $3 and sdec(key, 32, 32) = $4)",
                "(table_id = $5)"
            ]
        );
    }
}
