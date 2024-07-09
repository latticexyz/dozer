mod api;
mod api_logs;
mod api_sql;
mod api_tables;
mod indexer;
mod mud_encoding;
mod mud_schema;
mod reindex;
mod validate_sql;

use alloy::providers::ProviderBuilder;
use axum::{
    body::Body,
    extract::MatchedPath,
    routing::{get, post},
    Router,
};
use clap::{Parser, Subcommand};
use deadpool_postgres::{Manager, ManagerConfig, Pool};
use eyre::WrapErr;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::{future::ready, process::exit, str::FromStr, time::Duration};
use tokio::{self};
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer, trace::TraceLayer};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use url::Url;

static SCHEMA: &'static str = include_str!("./schema.sql");

#[derive(Parser)]
#[command(name = "dozer", about = "An indexer for MUD", version = "0.1")]
struct Dozer {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Parser)]
struct ServerArgs {
    #[arg(long, env = "PG_URL", default_value = "postgres://localhost/dozer")]
    pg_url: String,

    #[arg(long, env = "ETH_URL", default_value = "https://rpc.redstonechain.com")]
    eth_url: Url,

    #[arg(long, env = "RO_PASSWORD")]
    ro_password: Option<String>,

    #[clap(long, action = clap::ArgAction::SetTrue)]
    no_index: bool,

    #[clap(short, long)]
    index_start: Option<u64>,

    #[clap(short, long, default_value = "1000")]
    batch_size: u64,

    #[clap(short, long, default_value = "0.0.0.0:8000")]
    listen: String,
}

#[derive(Subcommand)]
enum Commands {
    Query(api_sql::cli::Request),
    Reindex(ServerArgs),
    Server(ServerArgs),
    Table(api_tables::cli::Request),
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let fmt_layer = fmt::layer()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .compact();
    let filter_layer = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::registry()
        .with(MetricsLayer::new())
        .with(fmt_layer)
        .with(filter_layer)
        .init();

    let args = Dozer::parse();
    let http_client = reqwest::Client::new();

    match args.command {
        Some(Commands::Table(args)) => api_tables::cli::request(&http_client, args).await,
        Some(Commands::Query(args)) => api_sql::cli::request(&http_client, args).await,
        Some(Commands::Server(args)) => server(args).await,
        Some(Commands::Reindex(args)) => reindex(args).await,
        None => server(ServerArgs::parse()).await,
    }
}

fn api_ro_pg(cstr: &str, ro_password: &str) -> Pool {
    let mut pg_config = tokio_postgres::Config::from_str(cstr).expect("unable to connect to ro pg");
    pg_config.user("uapi");
    pg_config.password(ro_password);
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("tls builder");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let pg_mgr = Manager::from_config(
        pg_config,
        connector,
        ManagerConfig {
            recycling_method: deadpool_postgres::RecyclingMethod::Fast,
        },
    );
    Pool::builder(pg_mgr)
        .max_size(16)
        .build()
        .expect("unable to build new ro pool")
}

async fn reindex(args: ServerArgs) -> eyre::Result<()> {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("tls builder");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let (mut w_pg, w_pg_conn) = tokio_postgres::connect(&args.pg_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = w_pg_conn.await {
            panic!("database writer error: {}", e)
        }
    });

    w_pg.batch_execute(SCHEMA).await.wrap_err("exec schema")?;
    loop {
        match reindex::tables(&mut w_pg).await {
            Err(e) => {
                panic!("reindexing: {:?}", e);
            }
            Ok(n) if n == 0 => {
                println!("done");
                exit(0)
            }
            Ok(n) => {
                tracing::info!("re-indexed {} tables", n)
            }
        }
    }
}

async fn server(args: ServerArgs) -> eyre::Result<()> {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("tls builder");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let (mut w_pg, w_pg_conn) = tokio_postgres::connect(&args.pg_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = w_pg_conn.await {
            panic!("database writer error: {}", e)
        }
    });

    let eth_client = ProviderBuilder::new().on_http(args.eth_url);
    {
        w_pg.batch_execute(SCHEMA).await.wrap_err("exec schema")?;
        indexer::init_blocks(&mut w_pg, &eth_client, args.index_start.unwrap_or(0)).await?;
    }

    let config = api::Config {
        pool: api_ro_pg(&args.pg_url, &args.ro_password.unwrap_or_default()),
    };

    let prom_record = PrometheusBuilder::new()
        .add_global_label("name", "dozer")
        .build_recorder();
    let prom_handler = prom_record.handle();
    metrics::set_global_recorder(TracingContextLayer::all().layer(prom_record))
        .expect("unable to set global metrics recorder");

    let tracing = TraceLayer::new_for_http()
        .make_span_with(|req: &axum::http::Request<Body>| {
            let path = req
                .extensions()
                .get::<MatchedPath>()
                .map(MatchedPath::as_str);
            tracing::info_span!("http", path, status = tracing::field::Empty)
        })
        .on_response(
            |resp: &axum::http::Response<_>, d: Duration, span: &tracing::Span| {
                span.record("status", resp.status().as_str());
                let _guard = span.enter();
                metrics::counter!("api.requests").increment(1);
                metrics::histogram!("api.latency").record(d.as_millis() as f64);
                if !resp.status().is_success() {
                    metrics::counter!("api.errors").increment(1);
                    tracing::error!("uri in error log")
                }
            },
        );

    let service = tower::ServiceBuilder::new()
        .layer(tracing)
        .layer(TimeoutLayer::new(Duration::from_secs(10)))
        .layer(CompressionLayer::new());

    let (app, listener) = (
        Router::new()
            .route("/", get(|| async { "hello\n" }))
            .route("/metrics", get(move || ready(prom_handler.render())))
            .route("/q", post(api_sql::handle))
            .route("/tables", post(api_tables::handle))
            .route("/api/logs", get(api_logs::handle))
            .layer(service)
            .with_state(config.clone()),
        tokio::net::TcpListener::bind(args.listen)
            .await
            .expect("binding to tcp for http server"),
    );

    tokio::spawn(async move {
        if args.no_index {
            println!("indexing disabled");
            return;
        }
        if let Err(err) = w_pg.query("select pg_advisory_lock(1)", &[]).await {
            println!("unable lock for indexing: {}", err);
            return;
        }
        //TODO: this is a workaround for the redstone RPC API not having a reliable
        //block range limit for the eth_getLogs request.
        let mut batch_size = args.batch_size;
        loop {
            match indexer::index(&eth_client, &mut w_pg, batch_size).await {
                Ok(_) => batch_size = args.batch_size,
                Err(indexer::IndexError::Fatal(e)) => {
                    tracing::error!(%e, "An error occurred: {:?}", e);
                    std::process::exit(1);
                }
                Err(indexer::IndexError::Retry(e)) => {
                    batch_size = std::cmp::max(1, batch_size / 10);
                    tracing::error!("indexer retry: {:?}", e.to_string());
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
    axum::serve(listener, app).await.wrap_err("serving http")
}
