use alloy::primitives::FixedBytes;
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
    pub address: Option<FixedBytes<20>>,
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

fn add_address(address: Option<FixedBytes<20>>, sql: &mut String, params: &mut Vec<Box<Param>>) {
    if let Some(a) = address {
        sql.push_str(" and address = $2");
        params.push(Box::new(a));
    }
}
