mod api;
mod api_error;
mod indexer;
mod schema;
mod sql;

use alloy::providers::ProviderBuilder;
use axum::{
    body::Body,
    extract::MatchedPath,
    routing::{get, post},
    Router,
};
use clap::Parser;
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::WrapErr;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::{cmp::max, str::FromStr, time::Duration};
use tokio;
use tokio_postgres::{Client, NoTls};
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

    #[arg(short, long)]
    ro_password: Option<String>,
}

impl Args {
    fn ro_password(&self) -> String {
        match &self.ro_password {
            Some(s) => s.clone(),
            None => {
                if let Ok(s) = std::env::var("RO_PASSWORD") {
                    s
                } else {
                    String::new()
                }
            }
        }
    }
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

fn api_ro_pg(cstr: &str, ro_password: &str) -> Pool {
    let mut pg_config = tokio_postgres::Config::from_str(cstr).expect("unable to connect to ro pg");
    pg_config.user("uapi");
    pg_config.password(ro_password);
    let pg_mgr = if cstr.contains("sslmode") {
        let mut builder = SslConnector::builder(SslMethod::tls()).expect("Error creating builder.");
        builder.set_verify(SslVerifyMode::NONE);
        let connector = MakeTlsConnector::new(builder.build());
        Manager::from_config(
            pg_config,
            connector,
            ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        )
    } else {
        Manager::from_config(
            pg_config,
            NoTls,
            ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        )
    };
    Pool::builder(pg_mgr)
        .max_size(16)
        .build()
        .expect("unable to build new ro pool")
}

async fn pg_tls_connect(url: &str) -> Result<Client, tokio_postgres::Error> {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("Error creating builder.");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let (client, conn) = tokio_postgres::connect(url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            panic!("database writer error: {}", e)
        }
    });
    Ok(client)
}

async fn pg_connect(url: &str) -> Result<Client, tokio_postgres::Error> {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            panic!("database writer error: {}", e)
        }
    });
    Ok(client)
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

    let mut w_pg = if args.pg_url().contains("sslmode") {
        pg_tls_connect(&args.pg_url()).await?
    } else {
        pg_connect(&args.pg_url()).await?
    };

    let eth_client = ProviderBuilder::new()
        .on_http(args.eth_url())
        .expect("unable to build eth client");
    {
        w_pg.batch_execute(SCHEMA).await.wrap_err("exec schema")?;
        indexer::init_blocks(&mut w_pg, &eth_client).await?;
    }

    let config = api::Config {
        pool: api_ro_pg(&args.pg_url(), &args.ro_password()),
    };

    let (app, listener) = (
        Router::new()
            .route("/", get(|| async { "hello\n" }))
            .route("/q", post(api::query))
            .route("/api/logs", get(api::logs))
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
        tokio::net::TcpListener::bind("0.0.0.0:8000")
            .await
            .expect("binding to tcp for http server"),
    );

    tokio::spawn(async move {
        //TODO: this is a workaround for the redstone RPC API not having a reliable
        // block range limit for the eth_getLogs request.
        const MAX_BATCH_SIZE: u64 = 1000;
        let mut batch_size = MAX_BATCH_SIZE;
        loop {
            match indexer::index(&eth_client, &mut w_pg, batch_size).await {
                Ok(_) => batch_size = MAX_BATCH_SIZE,
                Err(indexer::IndexError::Fatal(e)) => {
                    tracing::error!(%e, "An error occurred: {:?}", e);
                    std::process::exit(1);
                }
                Err(indexer::IndexError::Retry(e)) => {
                    batch_size = max(1, batch_size / 2);
                    tracing::error!("indexer retry: {:?}", e.to_string());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
    axum::serve(listener, app).await.wrap_err("serving http")
}
