mod api;
mod api_error;
mod indexer;
mod mud_encoding;
mod mud_schema;

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
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_tracing_context::{MetricsLayer, TracingContextLayer};
use metrics_util::layers::Layer as MetricsUtilLayer;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use std::{future::ready, str::FromStr, time::Duration};
use tokio;
use tower_http::{compression::CompressionLayer, timeout::TimeoutLayer, trace::TraceLayer};
use tracing_subscriber::{
    layer::SubscriberExt, Layer as TracingSubscriberLayer, Registry as TracingSubscriberRegistry,
};
use url::Url;

static SCHEMA: &'static str = include_str!("./schema.sql");

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
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

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let no_uri = tracing_subscriber::fmt::format::debug_fn(|writer, field, value| {
        if field.name() == "uri" {
            write!(writer, "uri: [see-error-log]")
        } else {
            write!(writer, "{}: {:?}", field, value)
        }
    });
    let file_appender = tracing_appender::rolling::hourly("/tmp", "dozer-error.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let error_data_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking)
        .with_filter(tracing::level_filters::LevelFilter::ERROR);
    let info_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .fmt_fields(no_uri)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .with_filter(tracing::level_filters::LevelFilter::INFO);
    let subscriber = TracingSubscriberRegistry::default()
        .with(error_data_layer)
        .with(info_layer)
        .with(MetricsLayer::new());
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Args::parse();

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
        pool: api_ro_pg(
            &args.pg_url,
            &args.ro_password.expect("missing read only pg password"),
        ),
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
            .route("/q", post(api::query))
            .route("/api/logs", get(api::logs))
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
