mod api_error;
mod indexer;
mod schema;
mod sql;

use api_error::ApiError;
use schema::Schema;
use sql::ParsedQuery;

use alloy::{
    hex::{self, ToHexExt},
    providers::ProviderBuilder,
};
use axum::{
    body::Body,
    extract::{MatchedPath, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::WrapErr;
use ruint::aliases::{U256, U64};
use serde::Deserialize;
use serde_json::Value;
use std::{str::FromStr, time::Duration};
use tokio;
use tokio_postgres::{
    types::{ToSql, Type},
    NoTls,
};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer, trace::TraceLayer};
use tracing;
use tracing_subscriber::FmtSubscriber;

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
        indexer::init_blocks(&mut w_pg, &eth_client).await?;
    }

    let config = Config {
        pool: api_ro_pg(&args.pg_url()),
    };

    let (app, listener) = (
        Router::new()
            .route("/", get(|| async { "hello\n" }))
            .route("/q", post(query))
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
        tokio::net::TcpListener::bind("localhost:8000")
            .await
            .expect("binding to tcp for http server"),
    );

    tokio::spawn(async move {
        loop {
            match indexer::index(&eth_client, &mut w_pg).await {
                Ok(_) => {}
                Err(indexer::IndexError::Fatal(e)) => {
                    tracing::error!(%e, "An error occurred: {:?}", e);
                    std::process::exit(1);
                }
                Err(indexer::IndexError::Retry(e)) => {
                    tracing::debug!("indexer retry: {:?}", e.to_string());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
    axum::serve(listener, app).await.wrap_err("serving http")
}

#[derive(Deserialize)]
struct GetRecsReq {
    query: String,
    values: Vec<Value>,
}

async fn query(
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
