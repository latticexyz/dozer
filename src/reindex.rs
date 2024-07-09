use alloy::primitives::B256;
use eyre::{Context, Result};
use ruint::aliases::U64;
use tokio_postgres::Client;

use crate::{
    indexer, mud_encoding,
    mud_schema::{self, Schema},
};

#[tracing::instrument(fields(tables, block_num, log_idx) skip_all)]
pub async fn tables(pg: &mut Client) -> Result<u64, indexer::IndexError> {
    let pgtx = pg.transaction().await.wrap_err("opening index tx")?;
    let rows = pgtx
        .query(
            "
            select block_num, log_idx
            from tables
            order by block_num desc, log_idx desc
            limit 1
            ",
            &[],
        )
        .await?;
    let (block_num, log_idx) = if rows.len() == 0 {
        (U64::from(0), U64::from(0))
    } else {
        (
            rows.first().unwrap().try_get::<&str, U64>("block_num")?,
            rows.first().unwrap().try_get::<&str, U64>("log_idx")?,
        )
    };
    let records: Vec<indexer::Record> = pgtx
        .query(
            "
            select
                block_num,
                log_idx,
                address,
                table_id,
                key,
                static_data,
                encoded_lengths,
                dynamic_data,
                deleted
            from records
            where (block_num, log_idx) > ($1, $2)
            and table_id = $3
            and not deleted
            and not expired
            order by block_num asc, log_idx asc
            limit 1000
        ",
            &[&block_num, &log_idx, &mud_schema::Schema::TABLES_TABLE_ID],
        )
        .await?
        .iter()
        .map(indexer::Record::from_row)
        .collect::<Result<Vec<_>, _>>()?;
    let n = records.len() as u64;
    tracing::Span::current()
        .record("tables", n)
        .record("block_num", block_num.to::<u64>())
        .record("log_idx", log_idx.to::<u64>());
    if n == 0 {
        return Ok(0);
    }
    for rec in records {
        let key: B256 = B256::from_slice(&rec.key);
        let data =
            mud_encoding::Data::new(&rec.static_data, rec.encoded_lengths, &rec.dynamic_data)?;
        let schema = &Schema::from_data(rec.address, key, &data)?;
        schema
            .insert(&pgtx, rec.block_num.to(), rec.log_idx.to(), rec.address)
            .await?;
    }
    pgtx.commit().await.wrap_err("unable to commit tx")?;
    Ok(n)
}
