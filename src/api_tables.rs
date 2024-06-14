use alloy::primitives::{Address, FixedBytes};
use axum::extract::State;
use eyre::Context;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::ToSql;

use crate::{
    api,
    mud_schema::{self, Schema},
};

type Param = (dyn ToSql + Sync + Send);

#[derive(Debug, Deserialize, Serialize)]
pub enum Query {
    #[serde(rename = "name")]
    Name(String),
    #[serde(rename = "id")]
    Id(FixedBytes<32>),
}
#[derive(Deserialize, Serialize)]
pub struct Request {
    pub address: Option<Address>,
    pub query: Query,
}

#[tracing::instrument(skip_all)]
pub async fn handle(
    State(state): State<api::Config>,
    api::Json(req): api::Json<Request>,
) -> Result<axum::Json<Vec<mud_schema::Schema>>, api::Error> {
    let pg = state.pool.get().await.wrap_err("getting conn from pool")?;
    match req.query {
        Query::Id(id) => {
            let mut q = String::from("select address, id, key_names, key_schema, val_names, val_schema from tables where id = $1");
            let mut params: Vec<Box<Param>> = vec![Box::new(id)];
            add_address(req.address, &mut q, &mut params);
            pg.query(
                &q,
                &params
                    .iter()
                    .map(|b| b.as_ref() as &(dyn ToSql + Sync))
                    .collect::<Vec<_>>()[..],
            )
            .await?
            .iter()
            .map(Schema::from_row)
            .collect::<Result<Vec<Schema>, _>>()
            .map_err(|e| api::Error::Server(e.into()))
            .map(axum::Json)
        }
        Query::Name(name) => {
            let mut q = String::from("select address, id, key_names, key_schema, val_names, val_schema from tables where name ilike $1");
            let mut params: Vec<Box<Param>> = vec![Box::new(name)];
            add_address(req.address, &mut q, &mut params);
            pg.query(
                &q,
                &params
                    .iter()
                    .map(|b| b.as_ref() as &(dyn ToSql + Sync))
                    .collect::<Vec<_>>()[..],
            )
            .await?
            .iter()
            .map(Schema::from_row)
            .collect::<Result<Vec<Schema>, _>>()
            .map_err(|e| api::Error::Server(e.into()))
            .map(axum::Json)
        }
    }
}

fn add_address(address: Option<Address>, sql: &mut String, params: &mut Vec<Box<Param>>) {
    if let Some(a) = address {
        sql.push_str(" and address = $2");
        params.push(Box::new(a.into_array()));
    }
}

pub mod cli {
    use alloy::{
        hex::FromHex,
        primitives::{Address, B256},
    };
    use clap::Args;
    use eyre::Result;
    use reqwest::Client;
    use std::io::Write;
    use url::Url;

    use crate::{api::client_post, mud_schema};

    #[derive(Args, Debug)]
    pub struct Request {
        #[clap(short, long, global = true, default_value = "http://0.0.0.0:8000")]
        dozer_url: Url,

        pub resource_id: String,

        #[arg(short, long, env = "DOZER_ADDRESS")]
        pub address: Option<Address>,
    }

    pub async fn request(http_client: &Client, targs: Request) -> Result<()> {
        let req_body = if let Ok(table_id) = B256::from_hex(&targs.resource_id) {
            super::Request {
                address: targs.address,
                query: super::Query::Id(table_id),
            }
        } else {
            super::Request {
                address: targs.address,
                query: super::Query::Name(targs.resource_id),
            }
        };

        let mut req_path = targs.dozer_url.clone();
        req_path.set_path("/tables");

        let res =
            client_post::<Vec<mud_schema::Schema>, _>(&http_client, req_path, &req_body).await?;
        let mut tw = tabwriter::TabWriter::new(std::io::stdout());
        res.iter()
            .for_each(|s| writeln!(tw, "{}", s.description()).expect("unable to write to stdout"));
        Ok(())
    }
}
