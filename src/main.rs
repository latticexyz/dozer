mod checksql;
use checksql::{unknown_function, unknown_table};

use alloy::{
    hex::{self, ToHexExt},
    primitives::{fixed_bytes, BlockHash, Bytes, FixedBytes},
    providers::{Provider, ProviderBuilder, ReqwestProvider},
    rpc::types::eth::{Block, BlockNumberOrTag, Filter, Log},
    sol,
    sol_types::{SolEvent, SolType},
};
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{MatchedPath, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::{eyre, ContextCompat, WrapErr};
use ruint::aliases::{U256, U64};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{borrow::Borrow, cmp, str::FromStr, time::Duration};
use tokio;
use tokio_postgres::{
    types::{ToSql, Type},
    Client, NoTls, Transaction,
};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer, trace::TraceLayer};
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

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    pg_url: Option<String>,

    #[arg(short, long)]
    eth_url: Option<String>,
}

impl Args {
    fn pg_url(&self) -> String {
        match &self.pg_url {
            Some(u) => u.clone(),
            None => {
                if let Ok(u) = std::env::var("PG_URL") {
                    u
                } else {
                    String::from("postgres://localhost/imud")
                }
            }
        }
    }
    fn eth_url(&self) -> url::Url {
        match &self.eth_url {
            Some(u) => u.parse().expect("unable to parse eth url"),
            None => {
                if let Ok(u) = std::env::var("ETH_URL") {
                    u.parse().expect("unable to parse eth url")
                } else {
                    "http://localhost:8545".parse().unwrap()
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    pool: Pool,
}

fn api_ro_pg(cstr: &str) -> Pool {
    let mut pg_config = tokio_postgres::Config::from_str(cstr).expect("unable to connect to ro pg");
    pg_config.user("uapi");
    let pg_mgr = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        },
    );
    Pool::builder(pg_mgr)
        .max_size(16)
        .build()
        .expect("unable to build new ro pool")
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_level(false)
        .with_target(false)
        .with_max_level(tracing::Level::INFO)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();
    let (mut w_pg, w_conn) = tokio_postgres::connect(&args.pg_url(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = w_conn.await {
            panic!("database writer error: {}", e)
        }
    });
    let eth_client = ProviderBuilder::new()
        .on_http(args.eth_url())
        .expect("unable to build eth client");
    {
        w_pg.batch_execute(SCHEMA).await.wrap_err("exec schema")?;
        init_blocks(&mut w_pg, &eth_client).await?;
    }

    let config = Config {
        pool: api_ro_pg(&args.pg_url()),
    };

    let (app, listener) = (
        Router::new()
            .route("/", get(|| async { "hello\n" }))
            .route("/q", post(get_records))
            .with_state(config.clone())
            .layer(CompressionLayer::new())
            .layer(TimeoutLayer::new(Duration::from_secs(10)))
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
            match index(&eth_client, &mut w_pg).await {
                Ok(_) => {}
                Err(IndexError::Fatal(e)) => {
                    tracing::error!(%e, "An error occurred: {:?}", e);
                    std::process::exit(1);
                }
                Err(IndexError::Retry(e)) => {
                    tracing::debug!("indexer retry: {:?}", e.to_string());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
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

impl From<tokio_postgres::Error> for ApiError {
    fn from(err: tokio_postgres::Error) -> Self {
        ApiError::Server(eyre!("database-error={}", err.to_string()))
    }
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
    query: String,
    values: Vec<Value>,
}

async fn get_records(
    State(state): State<Config>,
    Json(req): Json<GetRecsReq>,
) -> Result<Json<Vec<Value>>, ApiError> {
    if unknown_table(&req.query, &vec!["records"])? {
        return Err(ApiError::User(
            StatusCode::BAD_REQUEST,
            String::from("unknown table"),
        ));
    }
    if unknown_function(&req.query, &vec!["count", "b2i8"])? {
        return Err(ApiError::User(
            StatusCode::BAD_REQUEST,
            String::from("unknown function"),
        ));
    }
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
        .query(dbg!(&req.query), &vals[..])
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
                    Value::String(b.encode_hex())
                }
                _ => Value::Null,
            };
            row_json.insert(key, value);
        }
        result.push(Value::Object(row_json))
    }
    Ok(Json(result))
}

#[derive(Debug)]
enum IndexError {
    Retry(eyre::Report),
    Fatal(eyre::Report),
}

impl From<eyre::Report> for IndexError {
    fn from(err: eyre::Report) -> Self {
        IndexError::Fatal(err)
    }
}

impl From<tokio_postgres::Error> for IndexError {
    fn from(err: tokio_postgres::Error) -> Self {
        IndexError::Fatal(eyre!("database-error={}", err.to_string()))
    }
}

struct NumHash {
    num: u64,
    hash: FixedBytes<32>,
}

struct NextRange {
    from: NumHash,
    to: NumHash,
}

async fn init_blocks<F: EthApi>(pg: &mut Client, remote: &F) -> eyre::Result<()> {
    let block = remote
        .block(BlockNumberOrTag::Number(0))
        .await
        .map_err(|e| eyre!("getting block: {:?}", e))?;
    pg.execute(
        "
        insert into blocks(num, hash)
        values ($1, $2) on conflict(num) do nothing
        ",
        &[
            &U64::from(block.header.number.expect("missing header number")),
            &block.header.hash.unwrap_or_default(),
        ],
    )
    .await
    .map(|_| ())
    .wrap_err("unable to init blocks table")
}

async fn get_local_latest(tx: &Transaction<'_>) -> eyre::Result<(U64, BlockHash)> {
    let q = "SELECT num, hash from blocks order by num desc limit 1";
    let row = tx.query_one(q, &[]).await?;
    Ok((row.try_get("num")?, row.try_get("hash")?))
}

#[async_trait]
/// A subset of the ETH RPC API used by this indexer
/// We use alloy's ReqwestProvider for normal operations
/// and a Test struct for unit testing. This allows us to easily
/// simulate good/bad responses.
trait EthApi {
    async fn block(&self, n: BlockNumberOrTag) -> eyre::Result<Block, IndexError>;
    async fn logs(&self, filter: Filter) -> eyre::Result<Vec<Log>, IndexError>;
}

#[async_trait]
/// Wraps the alloy Result type with our internal error types
impl EthApi for ReqwestProvider {
    async fn block(&self, n: BlockNumberOrTag) -> eyre::Result<Block, IndexError> {
        self.get_block_by_number(n, false)
            .await
            .map_err(|err| IndexError::Retry(eyre::Report::from(err)))?
            .ok_or(IndexError::Retry(eyre!("no block found")))
    }

    /// In addition to getting the logs from the RPC API
    /// this function also does a basic validation step to ensure
    /// that the logs returned from the API are within the requested
    /// block range.
    async fn logs(&self, f: Filter) -> eyre::Result<Vec<Log>, IndexError> {
        let logs = self
            .get_logs(&f)
            .await
            .map_err(|err| IndexError::Retry(eyre::Report::from(err)))?;
        // It's not uncommon for RPC API providers to respond to
        // log requests with data that is unrelated to the requested block range
        for log in &logs {
            if let Some(n) = log.block_number {
                if let Some(BlockNumberOrTag::Number(m)) = f.block_option.get_from_block() {
                    if n < *m {
                        return Err(IndexError::Fatal(eyre!(
                            "log contains data for block={} but filter.from={}",
                            n,
                            m
                        )));
                    }
                }
                if let Some(BlockNumberOrTag::Number(m)) = f.block_option.get_to_block() {
                    if n > *m {
                        return Err(IndexError::Fatal(eyre!(
                            "log contains data for block={} but filter.to={}",
                            n,
                            m
                        )));
                    }
                }
            }
        }
        Ok(logs)
    }
}

#[tracing::instrument(fields(local, remote, removed) skip_all)]
async fn next_to_index<F: EthApi>(
    pgtx: &Transaction<'_>,
    remote: &F,
    max_reorg: u64,
) -> eyre::Result<NextRange, IndexError> {
    let mut removed = 0;
    for _ in 0..max_reorg {
        let latest_remote = remote.block(BlockNumberOrTag::Latest).await?;
        let remote_num = latest_remote.header.number.unwrap();
        let (local_num, local_hash) = get_local_latest(&pgtx)
            .await
            .map_err(|err| IndexError::Retry(eyre::Report::from(err)))?;
        let local_num: u64 = local_num.to();

        tracing::Span::current()
            .record("remote", remote_num)
            .record("local", local_num);

        if local_num >= remote_num {
            return Err(IndexError::Retry(eyre!(
                "nothing new remote={} local={}",
                remote_num,
                local_num,
            )));
        }
        let delta = cmp::min(remote_num - local_num, 10000);
        let (from, to) = (
            remote
                .block(BlockNumberOrTag::Number(local_num + 1))
                .await?,
            remote
                .block(BlockNumberOrTag::Number(cmp::min(
                    local_num + delta,
                    remote_num,
                )))
                .await?,
        );
        if from.header.parent_hash != local_hash {
            tracing::error!(
                "reorg remote={}/{} local={}/{}",
                from.header.hash.unwrap(),
                from.header.number.unwrap(),
                local_num,
                local_hash
            );
            pgtx.execute(
                "delete from blocks where num >= $1",
                &[&U64::from(local_num)],
            )
            .await?;
            pgtx.execute(
                "delete from tables where block_num >= $1",
                &[&U64::from(local_num)],
            )
            .await?;
            pgtx.execute(
                "delete from records where block_num >= $1",
                &[&U64::from(local_num)],
            )
            .await?;
            pgtx.execute(
                "
                update records set expired_block_num = NULL, expired_log_idx = NULL
                where expired_block_num >= $1
                ",
                &[&U64::from(local_num)],
            )
            .await?;
            removed += 1;
            continue;
        }
        tracing::Span::current().record("removed", removed);
        return Ok(NextRange {
            from: NumHash {
                num: from.header.number.unwrap(),
                hash: from.header.hash.unwrap(),
            },
            to: NumHash {
                num: to.header.number.unwrap(),
                hash: to.header.hash.unwrap(),
            },
        });
    }
    return Err(IndexError::Fatal(eyre!("reorg too big")));
}

#[tracing::instrument(fields(from, to, n) skip_all)]
async fn index<T: EthApi>(remote: &T, pg: &mut Client) -> eyre::Result<(), IndexError> {
    let pgtx = pg.transaction().await.wrap_err("opening index tx")?;
    let next = next_to_index(&pgtx, remote, 100).await?;
    pgtx.commit().await.wrap_err("unable to commit tx")?;

    let filter = Filter::new()
        .events(&[
            &Store_SetRecord::SIGNATURE,
            &Store_SpliceDynamicData::SIGNATURE,
            &Store_SpliceStaticData::SIGNATURE,
            &Store_DeleteRecord::SIGNATURE,
        ])
        .select(next.from.num..next.to.num);
    let mut logs = remote.logs(filter).await?;
    logs.sort_by_key(|l| (l.block_number, l.log_index));

    tracing::Span::current()
        .record("from", next.from.num)
        .record("to", next.to.num)
        .record("n", logs.len());

    let tx = pg.transaction().await.wrap_err("opening index tx")?;
    process_logs(&tx, logs).await.wrap_err("processing logs")?;
    tx.execute(
        "insert into blocks(num, hash) values ($1, $2)",
        &[&U64::from(next.to.num), &next.to.hash],
    )
    .await
    .wrap_err(format!("updating blocks table to latest {}", next.to.num))?;
    tx.commit().await.wrap_err("unable to commit tx")?;
    Ok(())
}

#[tracing::instrument(fields(n, skipped) skip_all)]
async fn process_logs(tx: &Transaction<'_>, logs: Vec<Log>) -> eyre::Result<()> {
    let (mut skipped, n) = (0, logs.len());
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

                const TABLES_TABLE_ID: FixedBytes<32> = fixed_bytes!(
                    "746273746f72650000000000000000005461626c657300000000000000000000"
                );
                if rec.table_id == TABLES_TABLE_ID {
                    save_schema(tx, block_num, log_idx, &rec).await?
                }
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
            _ => skipped += 1,
        }
    }
    tracing::Span::current()
        .record("n", n)
        .record("skipped", skipped);
    Ok(())
}

#[derive(Debug)]
#[allow(dead_code)]
struct DynamicData<'a> {
    f0: Option<&'a [u8]>,
    f1: Option<&'a [u8]>,
    f2: Option<&'a [u8]>,
    f3: Option<&'a [u8]>,
    f4: Option<&'a [u8]>,
}

impl<'a> DynamicData<'a> {
    fn new(data: &'a [u8], el: FixedBytes<32>) -> eyre::Result<Self> {
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

#[tracing::instrument(level="debug" skip_all)]
async fn save_schema(
    tx: &Transaction<'_>,
    block_num: u64,
    log_idx: u64,
    rec: &Store_SetRecord,
) -> eyre::Result<()> {
    let table_id = rec.key_tuple.first().wrap_err("missing table_id")?;
    let table_name: Vec<u8> = table_id[15..32]
        .iter()
        .map(|c| *c)
        .filter(|c| *c > 0 && *c < 255) //ascii table names
        .collect();
    let table_name = String::from_utf8(table_name).unwrap();
    let key_schema = FixedBytes::<32>::from_slice(
        rec.static_data
            .get(32..64)
            .wrap_err("unable to get key_schema")?,
    );
    let val_schema = FixedBytes::<32>::from_slice(
        rec.static_data
            .get(64..96)
            .wrap_err("unable to get val_schema")?,
    );
    let ddat = DynamicData::new(rec.dynamic_data.borrow(), rec.encoded_lengths)?;
    type SolArrayOf<T> = sol! { T[] };
    let key_names = SolArrayOf::<sol!(string)>::abi_decode(
        ddat.f0.expect("missing dynamic field for key names"),
        false,
    )?;
    let val_names = SolArrayOf::<sol!(string)>::abi_decode(
        ddat.f1.expect("missing dynamic field for val names"),
        false,
    )?;
    tracing::info!("new-schema table={:x}/{}", table_id, table_name,);
    tx.execute(
        "
        insert into tables(block_num, log_idx, table_id, table_name, key_schema, val_schema, key_names, val_names)
        values ($1, $2, $3, $4, $5, $6, $7, $8)
        ",
        &[
            &U64::from(block_num),
            &U64::from(log_idx),
            table_id,
            &table_name,
            &key_schema,
            &val_schema,
            &key_names,
            &val_names,
        ],
    )
    .await
    .map(|_| ())
    .wrap_err("inserting new table")
}

#[tracing::instrument(level="debug" skip_all)]
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

#[tracing::instrument(level="debug" skip_all)]
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

#[tracing::instrument(level="debug" skip_all)]
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

#[tracing::instrument(level="debug" skip_all)]
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
    use pgtemp::PgTempDB;

    use super::*;
    use std::sync::Once;

    static LOGGING_INIT: Once = Once::new();

    fn logging() {
        LOGGING_INIT.call_once(|| {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(tracing::Level::DEBUG)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        });
    }

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

    fn test_block(num: u64, hash: u8, parent: u8) -> Block {
        let mut block = Block::default();
        block.header.number = Some(num);
        block.header.hash = Some(FixedBytes::with_last_byte(hash));
        block.header.parent_hash = FixedBytes::with_last_byte(parent);
        block
    }

    struct TestGetRemote(Block);

    #[async_trait]
    impl EthApi for TestGetRemote {
        async fn logs(&self, _: Filter) -> eyre::Result<Vec<Log>, IndexError> {
            todo!()
        }
        async fn block(&self, n: BlockNumberOrTag) -> eyre::Result<Block, IndexError> {
            match n {
                BlockNumberOrTag::Number(n) => Ok(test_block(n, n as u8, (n - 1) as u8)),
                BlockNumberOrTag::Latest => Ok(self.0.clone()),
                _ => panic!("ah"),
            }
        }
    }

    #[tokio::test]
    async fn test_next_to_index() {
        logging();
        let db = &PgTempDB::async_new().await;
        let mut pg = test_pg(&db.connection_string()).await;
        let pgtx = pg.transaction().await.expect("opening index tx");
        pgtx.execute(
            "insert into blocks(num, hash) values ($1, $2)",
            &[&U64::from(0), &FixedBytes::<32>::ZERO],
        )
        .await
        .expect("setting up blocks table");

        let trg = TestGetRemote {
            0: test_block(10, 10, 9),
        };
        let next_range = next_to_index(&pgtx, &trg, 1).await.unwrap();
        assert_eq!(next_range.from.num, 1);
        assert_eq!(next_range.to.num, 10);
    }

    #[tokio::test]
    async fn test_next_to_index_reorg() {
        logging();
        let db = &PgTempDB::async_new().await;
        let mut pg = test_pg(&db.connection_string()).await;
        let pgtx = pg.transaction().await.expect("opening index tx");

        pgtx.execute(
            "
            insert into blocks(num, hash) values ($1, $2), ($3, $4)
            ",
            &[
                &U64::from(0),
                &FixedBytes::<32>::ZERO,
                &U64::from(1),
                &FixedBytes::<32>::with_last_byte(99),
            ],
        )
        .await
        .expect("setting up blocks table");

        pgtx.execute(
            r#"
            insert into records(table_id, key, block_num, log_idx, expired_block_num, expired_log_idx)
            values ('\x01', '{"\\x01"}', 0, 0, 1, 0), ('\x01', '{"\\x01"}', 1, 0, NULL, NULL)
            "#,
            &[],
        )
        .await
        .expect("setting up blocks table");

        let trg = TestGetRemote {
            0: test_block(2, 2, 1),
        };
        next_to_index(&pgtx, &trg, 2).await.unwrap();

        let rows = pgtx
            .query("select num, hash from blocks order by num desc", &[])
            .await
            .expect("test query");
        let mut got: Vec<(U64, FixedBytes<32>)> = vec![];
        for row in rows {
            got.push((row.get("num"), row.get("hash")))
        }
        assert_eq!(got, vec![(U64::from(0), FixedBytes::<32>::ZERO)]);

        let rows = pgtx
            .query("select block_num, expired_block_num from records", &[])
            .await
            .expect("querying records table");
        let mut got: Vec<(U64, Option<U64>)> = vec![];
        for row in rows {
            got.push((row.get("block_num"), row.get("expired_block_num")))
        }
        assert_eq!(got, vec![(U64::from(0), None)]);
    }

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
