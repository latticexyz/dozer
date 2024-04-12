use alloy::{
    primitives::{BlockHash, Bytes, FixedBytes},
    providers::{Provider, ProviderBuilder, ReqwestProvider},
    rpc::{
        self,
        types::eth::{BlockNumberOrTag, Filter},
    },
    sol,
    sol_types::SolEvent,
};
use axum::{
    body::Body,
    extract::{MatchedPath, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{eyre, ContextCompat, WrapErr};
use ruint::aliases::U64;
use serde::{Deserialize, Serialize};
use std::{cmp, str::FromStr, time::Duration};
use tokio;
use tokio_postgres::{NoTls, Row, Transaction};
use tower_http::trace::TraceLayer;
use tracing;
use tracing_subscriber::FmtSubscriber;

sol! {
 type EncodedLengths is bytes32;
 type ResourceId is bytes32;
 event HelloStore(bytes32 indexed storeVersion);
 #[derive(Debug)]
 event Store_SetRecord(
     ResourceId indexed table_id,
     bytes32[] key_tuple,
     bytes static_data,
     EncodedLengths encoded_lengths,
     bytes dynamic_data
 );
 #[derive(Debug)]
 event Store_SpliceStaticData(
     ResourceId indexed table_id,
     bytes32[] key_tuple,
     uint48 start,
     bytes data
 );
 #[derive(Debug)]
 event Store_SpliceDynamicData(
     ResourceId indexed table_id,
     bytes32[] key_tuple,
     uint8 dynamic_field_index,
     uint48 start,
     uint40 delete_count,
     EncodedLengths encoded_lengths,
     bytes data
 );
 #[derive(Debug)]
 event Store_DeleteRecord(
     ResourceId indexed table_id,
     bytes32[] key_tuple
 );
}

static SCHEMA: &'static str = include_str!("./schema.sql");

#[derive(Clone, Debug)]
struct Config {
    pool: Pool,
    eth: ReqwestProvider,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_level(false)
        .with_target(false)
        .without_time()
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let pg_config = tokio_postgres::Config::from_str("postgres://localhost/imud")?;
    let pg_mgr = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        },
    );
    let pg_pool = Pool::builder(pg_mgr).max_size(16).build()?;
    let eth_client = ProviderBuilder::new()
        .on_http(
            "https://rpc.holesky.redstone.xyz"
                .parse()
                .expect("unable to parse rpc url"),
        )
        .expect("unable to build eth client");

    let config = Config {
        pool: pg_pool,
        eth: eth_client,
    };

    {
        let conn = config.pool.get().await.wrap_err("getting pg connection")?;
        conn.batch_execute(SCHEMA).await.wrap_err("exec schema")?;
        init_blocks(&config).await?
    }
    let (app, listener) = (
        Router::new()
            .route("/", get(|| async { "hello\n" }))
            .route("/records", post(get_records))
            .with_state(config.clone())
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(|request: &axum::http::Request<_>| {
                        let matched_path = request
                            .extensions()
                            .get::<MatchedPath>()
                            .map(MatchedPath::as_str);
                        tracing::info_span!("http", matched_path)
                    })
                    .on_failure(
                        |_error: tower_http::classify::ServerErrorsFailureClass,
                         _latency: Duration,
                         _span: &tracing::Span| {},
                    )
                    .on_response(
                        |_: &axum::http::Response<Body>, latency: Duration, _: &tracing::Span| {
                            tracing::info!(latency = latency.as_millis())
                        },
                    ),
            ),
        tokio::net::TcpListener::bind("localhost:3000")
            .await
            .expect("binding to tcp for http server"),
    );
    tokio::spawn(async move {
        loop {
            if let Err(e) = index(&config).await {
                tracing::error!(%e, "An error occurred: {:?}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });
    axum::serve(listener, app).await.wrap_err("serving http")
}

enum ApiError {
    User(axum::http::StatusCode, String),
    Server(eyre::Report),
}

#[derive(Serialize)]
struct ApiErrorMessage {
    msg: String,
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            Self::User(status, msg) => {
                tracing::error!("user-error={}", msg);
                (status, msg)
            }
            Self::Server(e) => {
                tracing::error!(%e, "server-error={:?}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    String::from("server error"),
                )
            }
        };
        let m = ApiErrorMessage {
            msg: String::from(message),
        };
        (status, Json(m)).into_response()
    }
}

impl From<eyre::Report> for ApiError {
    fn from(value: eyre::Report) -> Self {
        ApiError::Server(value)
    }
}

#[derive(Deserialize)]
struct GetRecsReq {
    table_id: FixedBytes<32>,
    key: Vec<FixedBytes<32>>,
}

#[derive(Serialize, Deserialize)]
struct GetRecsResp {
    block_num: U64,
    log_idx: U64,
    static_data: Bytes,
    encoded_lengths: FixedBytes<32>,
    dynamic_data: Bytes,
}

impl GetRecsResp {
    fn from_row(row: &Row) -> Result<Self, tokio_postgres::Error> {
        Ok(GetRecsResp {
            block_num: row.try_get("block_num")?,
            log_idx: row.try_get("log_idx")?,
            static_data: Bytes::copy_from_slice(row.try_get("static_data")?),
            encoded_lengths: row.try_get("encoded_lengths")?,
            dynamic_data: Bytes::copy_from_slice(row.try_get("dynamic_data")?),
        })
    }
}

async fn get_records(
    State(state): State<Config>,
    Json(req): Json<GetRecsReq>,
) -> Result<Json<Vec<GetRecsResp>>, ApiError> {
    let conn = state.pool.get().await.wrap_err("getting conn from pool")?;
    let rows = conn
        .query(
            "
            select block_num, log_idx, static_data, encoded_lengths, dynamic_data
            from records
            where table_id = $1
            and key = $2
            and expired_block_num is null
            ",
            &[&req.table_id, &req.key],
        )
        .await
        .wrap_err("querying records table")?;
    let resp: Result<Vec<GetRecsResp>, _> = rows.iter().map(GetRecsResp::from_row).collect();
    Ok(Json(resp.unwrap()))
}

async fn init_blocks(config: &Config) -> eyre::Result<()> {
    let block = config
        .eth
        .get_block_by_number(BlockNumberOrTag::Number(0), false)
        .await?
        .expect("unable to find latest block");

    let conn = config.pool.get().await.unwrap();
    conn.execute(
        "
        insert into blocks(num, hash)
        values ($1, $2) on conflict(num) do nothing
        ",
        &[
            &U64::from(block.header.number.unwrap()),
            &block.header.hash.unwrap(),
        ],
    )
    .await
    .map(|_| ())
    .wrap_err("uanble to load inital block")
}

async fn local_latest(config: &Config) -> eyre::Result<(u64, BlockHash)> {
    let conn = config.pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT num, hash from blocks order by num desc limit 1",
            &[],
        )
        .await?;
    let n: Option<U64> = row.get(0);
    let h: BlockHash = row.get(1);
    Ok((n.unwrap().to(), h))
}

async fn index(config: &Config) -> eyre::Result<()> {
    loop {
        let remote_num = config.eth.get_block_number().await.unwrap();
        let (local_num, _) = local_latest(config).await?;
        if local_num >= remote_num {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        let delta = cmp::min(remote_num - local_num, 10000);
        let (from, to) = (local_num + 1, local_num + delta);
        tracing::info!(
            remote = remote_num,
            local = local_num,
            from = from,
            to = to,
            "get_logs"
        );

        let filter = Filter::new()
            .events(&[
                &Store_SetRecord::SIGNATURE,
                &Store_SpliceDynamicData::SIGNATURE,
                &Store_SpliceStaticData::SIGNATURE,
                &Store_DeleteRecord::SIGNATURE,
            ])
            .from_block(from)
            .to_block(to);
        let mut logs = config
            .eth
            .get_logs(&filter)
            .await
            .wrap_err("downloading logs")?;
        logs.sort_by_key(|l| (l.block_number, l.log_index));

        let to_hash = config
            .eth
            .get_block_by_number(BlockNumberOrTag::Number(to), false)
            .await
            .wrap_err("unable to get 'to' block")?
            .ok_or_else(|| eyre!("no block was returned for {}", to))?
            .header
            .hash;
        let mut conn = config.pool.get().await.wrap_err("getting db from pool")?;
        let tx = conn.transaction().await.wrap_err("opening index tx")?;
        process_logs(&tx, logs).await.wrap_err("processing logs")?;
        tx.execute(
            "insert into blocks(num, hash) values ($1, $2)",
            &[&U64::from(to), &to_hash],
        )
        .await
        .wrap_err(format!("updating blocks table to latest {}", to))?;
        tx.commit().await.wrap_err("unable to commit tx")?;
    }
}

#[tracing::instrument(skip_all)]
async fn process_logs(tx: &Transaction<'_>, logs: Vec<rpc::types::eth::Log>) -> eyre::Result<()> {
    tracing::info!(n = logs.len(), "process_logs");
    for log in logs {
        let (block_num, log_idx) = (
            log.block_number.wrap_err("missing block num from log")?,
            log.log_index.wrap_err("missing log idx from log")?,
        );
        match log.topics().first().unwrap_or_default() {
            &Store_SetRecord::SIGNATURE_HASH => {
                let rec = Store_SetRecord::decode_log_data(log.data(), true)
                    .wrap_err("decoding set record")?;
                expire_record(&tx, block_num, log_idx, rec.table_id, &rec.key_tuple, false).await?;
                set_record(&tx, log.block_number.unwrap(), log.log_index.unwrap(), &rec).await?;
            }
            &Store_SpliceDynamicData::SIGNATURE_HASH => {
                let rec = Store_SpliceDynamicData::decode_log_data(log.data(), true)
                    .wrap_err("decoding splice dynamic")?;
                let new_rec = splice_dynamic(&tx, &rec).await?;
                expire_record(&tx, block_num, log_idx, rec.table_id, &rec.key_tuple, false).await?;
                set_record(&tx, block_num, log_idx, &new_rec).await?;
            }
            &Store_SpliceStaticData::SIGNATURE_HASH => {
                let rec = Store_SpliceStaticData::decode_log_data(log.data(), true)
                    .wrap_err("decoding splice static")?;
                let new_rec = splice_static(&tx, &rec).await?;
                expire_record(&tx, block_num, log_idx, rec.table_id, &rec.key_tuple, false).await?;
                set_record(&tx, block_num, log_idx, &new_rec).await?
            }
            &Store_DeleteRecord::SIGNATURE_HASH => {
                let rec = Store_DeleteRecord::decode_log_data(log.data(), true)
                    .wrap_err("decoding delete record")?;
                expire_record(&tx, block_num, log_idx, rec.table_id, &rec.key_tuple, true).await?
            }
            _ => {}
        }
    }
    Ok(())
}

async fn set_record(
    tx: &Transaction<'_>,
    block_num: u64,
    log_idx: u64,
    record: &Store_SetRecord,
) -> eyre::Result<()> {
    tx.execute(
        "
        insert into records(table_id, key, static_data, encoded_lengths, dynamic_data, block_num, log_idx)
        values($1, $2, $3, $4, $5, $6, $7)
        ",
        &[
            &record.table_id,
            &record.key_tuple,
            &record.static_data.to_vec(),
            &record.encoded_lengths,
            &record.dynamic_data.to_vec(),
            &U64::from(block_num),
            &U64::from(log_idx),
        ],
    )
    .await
    .map(|_| ())
    .wrap_err("inserting record")
}

async fn expire_record(
    tx: &Transaction<'_>,
    block_num: u64,
    log_idx: u64,
    table_id: FixedBytes<32>,
    key: &Vec<FixedBytes<32>>,
    deleted: bool,
) -> eyre::Result<()> {
    tx.execute(
        "
            update records
            set expired_block_num =  $1, expired_log_idx=$2
            where expired_block_num is null and expired_log_idx is null
            and table_id = $3
            and key = $4
            and deleted = $5
        ",
        &[
            &U64::from(block_num),
            &U64::from(log_idx),
            &table_id,
            &key,
            &deleted,
        ],
    )
    .await
    .map(|_| ())
    .wrap_err("expiring record")
}

async fn splice_static(
    tx: &Transaction<'_>,
    record: &Store_SpliceStaticData,
) -> eyre::Result<Store_SetRecord> {
    let prev = tx
        .query(
            "
            select static_data, encoded_lengths, dynamic_data
            from records
            where table_id = $1
            and key = $2
            and expired_block_num is null
            and not deleted
            ",
            &[&record.table_id, &record.key_tuple],
        )
        .await?;
    let (mut sdata, dlen, ddata) = match prev.len() {
        0 => (vec![], FixedBytes::new([0u8; 32]), vec![]),
        1 => (
            prev.first().unwrap().get("static_data"),
            prev.first().unwrap().get("encoded_lengths"),
            prev.first().unwrap().get("dynamic_data"),
        ),
        _ => {
            return Err(eyre!("multiple previous records found"));
        }
    };
    splice(
        &mut sdata,
        record.start as usize,
        record.data.len(),
        &record.data,
    );
    Ok(Store_SetRecord {
        table_id: record.table_id,
        key_tuple: record.key_tuple.clone(),
        static_data: sdata.into(),
        encoded_lengths: dlen,
        dynamic_data: ddata.into(),
    })
}

#[tracing::instrument(skip_all)]
async fn splice_dynamic(
    tx: &Transaction<'_>,
    record: &Store_SpliceDynamicData,
) -> eyre::Result<Store_SetRecord> {
    let prev = tx
        .query(
            "
            select static_data, dynamic_data
            from records
            where table_id = $1
            and key = $2
            and expired_block_num is null
            and not deleted
            ",
            &[&record.table_id, &record.key_tuple],
        )
        .await
        .wrap_err("unable to find prev record to update")?;
    let (sdata, mut ddata) = match prev.len() {
        0 => (vec![], vec![]),
        1 => (
            prev.first().unwrap().get("static_data"),
            prev.first().unwrap().get("dynamic_data"),
        ),
        _ => {
            return Err(eyre!("multiple previous records found"));
        }
    };
    splice(
        &mut ddata,
        record.start as usize,
        record.delete_count as usize,
        &record.data,
    );
    Ok(Store_SetRecord {
        table_id: record.table_id,
        key_tuple: record.key_tuple.clone(),
        static_data: sdata.into(),
        encoded_lengths: record.encoded_lengths.clone(),
        dynamic_data: ddata.into(),
    })
}

// removes n bytes from data starting at i (zero-based indexing)
// inserts new into data at i
fn splice(data: &mut Vec<u8>, i: usize, n: usize, new: &Bytes) {
    if i > data.len() {
        data.resize(i, 0);
    }
    let end = std::cmp::min(i + n, data.len());
    data.splice(i..end, new.as_ref().iter().copied());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_splice_add_empty() {
        let mut data = vec![];
        splice(&mut data, 0, 0, &Bytes::from([1]));
        assert_eq!(data, vec![1]);
    }
    #[test]
    fn test_splice_add_end() {
        let mut data = vec![];
        splice(&mut data, 2, 0, &Bytes::from([4, 5, 6]));
        assert_eq!(data, vec![0, 0, 4, 5, 6]);
    }
    #[test]
    fn test_splice_beginning() {
        let mut data = vec![9, 2, 3];
        splice(&mut data, 0, 1, &Bytes::from([1]));
        assert_eq!(data, vec![1, 2, 3]);
    }
    #[test]
    fn test_splice_middle() {
        let mut data = vec![1, 9, 3];
        splice(&mut data, 1, 1, &Bytes::from([2]));
        assert_eq!(data, vec![1, 2, 3]);
    }
    #[test]
    fn test_splice_end() {
        let mut data = vec![1, 2, 9];
        splice(&mut data, 2, 1, &Bytes::from([3]));
        assert_eq!(data, vec![1, 2, 3]);
    }
    #[test]
    fn test_splice_increase_end() {
        let mut data = vec![1, 2, 3];
        splice(&mut data, 3, 0, &Bytes::from([4]));
        assert_eq!(data, vec![1, 2, 3, 4]);
    }
    #[test]
    fn test_splice_decrease_end() {
        let mut data = vec![1, 2, 3];
        splice(&mut data, 2, 1, &Bytes::new());
        assert_eq!(data, vec![1, 2]);
    }
    #[test]
    fn test_splice_increase_middle() {
        let mut data = vec![1, 8, 9];
        splice(&mut data, 1, 0, &Bytes::from([2, 3, 4, 5, 6, 7]));
        assert_eq!(data, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }
    #[test]
    fn test_splice_start_out_of_bounds() {
        let mut data = vec![1, 2, 3];
        splice(&mut data, 4, 1, &Bytes::from([4]));
        assert_eq!(data, vec![1, 2, 3, 0, 4]);
    }
}
