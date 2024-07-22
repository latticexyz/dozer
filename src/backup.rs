use std::{
    path::Path,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    error::SdkError,
    operation::put_object::{PutObjectError, PutObjectOutput},
    primitives::ByteStream,
    Client,
};
use clap::Parser;
use eyre::Result;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use tokio::fs;

#[derive(Parser)]
pub struct Args {
    #[clap(long = "backup-bucket", env = "DOZER_BACKUP_BUCKET")]
    bucket: String,

    #[clap(long = "backup-dir", env = "DOZER_BACKUP_DIR")]
    dir: String,

    #[clap(long = "backup-window", default_value = "1 day")]
    window: humantime::Duration,
}

pub async fn run(pg_url: &str, args: &Args) -> eyre::Result<()> {
    let s3 = aws_sdk_s3::Client::new(&aws_config::load_defaults(BehaviorVersion::latest()).await);

    let mut builder = SslConnector::builder(SslMethod::tls()).expect("tls builder");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let (pg, pg_conn) = tokio_postgres::connect(pg_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = pg_conn.await {
            panic!("database writer error: {}", e)
        }
    });
    pg.query("select pg_advisory_lock(2)", &[]).await?;

    match local_backups(&args.dir).await?.into_iter().last() {
        Some(last) if now() - last > args.window.as_secs() => {
            tracing::info!("local backup needed. last: {}", since(last));
            pgdump(pg_url)?;
        }
        Some(last) => {
            tracing::info!("local backup up to date. last: {}", since(last))
        }
        None => {
            tracing::info!("no local backups");
            pgdump(pg_url)?;
        }
    };
    let last_local = local_backups(&args.dir)
        .await?
        .into_iter()
        .last()
        .expect("missing local backup");
    match remote_backups(&s3, &args.bucket).await?.into_iter().last() {
        Some(last_remote) if last_local > last_remote => {
            tracing::info!("remote is behind local");
            upload_backup(
                &s3,
                &args.bucket,
                &Path::new(&args.dir).join(to_filename(last_local)),
            )
            .await?;
        }
        Some(last) => {
            tracing::info!("remote backup is up to date. last: {}", since(last))
        }
        None => {
            tracing::info!("no remote backups");
            upload_backup(
                &s3,
                &args.bucket,
                &Path::new(&args.dir).join(to_filename(last_local)),
            )
            .await?;
        }
    }
    local_cleanup(&args.dir).await
}

#[tracing::instrument(skip_all, fields(id))]
fn pgdump(database_url: &str) -> Result<u64> {
    let id = now();
    tracing::Span::current().record("id", id);
    tracing::info!("starting pg_dump");
    std::process::Command::new("pg_dump")
        .arg(database_url)
        .arg("-F")
        .arg("c")
        .arg("-f")
        .arg(to_filename(id))
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?;
    Ok(id)
}

fn since(id: u64) -> String {
    humantime::format_duration(Duration::from_secs(now() - id)).to_string()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn to_filename(id: u64) -> String {
    format!("dozer-backup-{}", id)
}

fn from_filename(name: &str) -> Option<u64> {
    name.strip_prefix("dozer-backup-")?.parse::<u64>().ok()
}

#[tracing::instrument(skip_all)]
async fn local_backups(dir: &str) -> Result<Vec<u64>> {
    let mut res = Vec::new();
    let mut paths = fs::read_dir(dir).await?;
    while let Some(entry) = paths.next_entry().await? {
        if let Some(file) = entry.path().file_stem().unwrap().to_str() {
            if let Some(id) = from_filename(file) {
                res.push(id)
            }
        }
    }
    res.sort();
    Ok(res)
}

#[tracing::instrument(skip_all, fields(deleted))]
async fn local_cleanup(dir: &str) -> Result<()> {
    let backups = local_backups(dir).await?;
    let num_backups = backups.len();
    if num_backups > 1 {
        assert!(backups.windows(2).all(|b| b[0] <= b[1]));
        for id in backups.into_iter().take(num_backups - 1) {
            let path = Path::new(dir).join(to_filename(id));
            tracing::info!(id, "deleting");
            fs::remove_file(path).await?;
        }
    }
    tracing::Span::current().record("deleted", num_backups - 1);
    Ok(())
}

#[tracing::instrument(skip_all, fields(bucket))]
async fn remote_backups(client: &Client, bucket: &str) -> Result<Vec<u64>> {
    let mut res = Vec::new();
    let mut response = client
        .list_objects_v2()
        .bucket(bucket)
        .into_paginator()
        .send();
    while let Some(result) = response.next().await {
        for object in result?.contents() {
            let key = object.key().expect("missing key");
            if let Some(id) = from_filename(key) {
                res.push(id)
            }
        }
    }
    res.sort();
    Ok(res)
}

#[tracing::instrument(skip_all, fields(id))]
pub async fn upload_backup(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    file: &Path,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let id = file
        .file_name()
        .expect("unable to get file name from path")
        .to_str()
        .expect("unable to convert file name to str");
    tracing::Span::current().record("id", from_filename(id));
    client
        .put_object()
        .bucket(bucket_name)
        .key(id)
        .body(
            ByteStream::from_path(file)
                .await
                .expect("unable to open file for streaming"),
        )
        .send()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_filename() {
        assert!(from_filename("").is_none());
        assert!(from_filename("dozer-backup").is_none());
        assert!(from_filename("dozer-backup-foo").is_none());
        assert_eq!(
            1721432878,
            from_filename("dozer-backup-1721432878").expect("unable to parse time")
        );
    }
}
