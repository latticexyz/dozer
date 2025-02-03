use eyre::Result;
use postgresql_embedded::{PostgreSQL, Settings, Version};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use tokio_postgres::{Client, NoTls};

pub fn fmt_sql(sql: &str) -> Result<String> {
    const PG: &PostgreSqlDialect = &PostgreSqlDialect {};
    let ast = Parser::parse_sql(PG, sql)?;
    Ok(ast[0].to_string())
}

pub async fn test_pg() -> (PostgreSQL, Client) {
    let pg_settings = Settings {
        version: Version::new(16, Some(2), Some(3)),
        ..Default::default()
    };
    let mut db = PostgreSQL::new(pg_settings);
    db.setup().await.expect("setting up pg");
    db.start().await.expect("starting pg");
    db.create_database("dozer-test")
        .await
        .expect("creating test db");
    let (client, connection) = tokio_postgres::connect(&db.settings().url("dozer-test"), NoTls)
        .await
        .expect("unable to start test database");
    tokio::spawn(connection);
    client
        .batch_execute(include_str!("./schema.sql"))
        .await
        .expect("resetting schema");
    (db, client)
}
