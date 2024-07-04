use crate::{mud_encoding, mud_schema::Schema};

use alloy::{
    primitives::{BlockHash, Bytes, FixedBytes, B256},
    providers::{Provider, ReqwestProvider},
    rpc::{
        client::{BatchRequest, Waiter},
        types::eth::{Block, BlockNumberOrTag, Filter, Log},
    },
    sol,
    sol_types::SolEvent,
};
use async_trait::async_trait;
use eyre::{eyre, ContextCompat, WrapErr};
use futures::pin_mut;
use itertools::Itertools;
use ruint::aliases::U64;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    vec,
};
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Client, Row, Transaction};

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

#[derive(Debug)]
pub enum IndexError {
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

pub async fn init_blocks<F: EthApi>(pg: &mut Client, remote: &F, start: u64) -> eyre::Result<()> {
    let block = remote
        .block(BlockNumberOrTag::Number(start))
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
pub trait EthApi {
    async fn block(&self, n: BlockNumberOrTag) -> eyre::Result<Block, IndexError>;
    async fn logs(&self, filter: Filter) -> eyre::Result<Vec<Log>, IndexError>;
}

#[async_trait]
/// Wraps the alloy Result type with our internal error types
impl EthApi for ReqwestProvider {
    #[tracing::instrument(skip_all)]
    async fn block(&self, n: BlockNumberOrTag) -> eyre::Result<Block, IndexError> {
        self.get_block_by_number(n, false)
            .await
            .map_err(|err| IndexError::Retry(eyre::Report::from(err)))?
            .ok_or(IndexError::Retry(eyre!("no block found")))
    }

    /// In addition to getting the logs from the RPC API this function also does
    /// a basic validation step to ensure that the logs returned from the API
    /// are within the requested block range.
    ///
    /// This function uses a batch request to get the logs. The batch request
    /// contains a request for: block by height (using the log filter's "to"
    /// field) and the logs request. We do this to ensure that the backend
    /// serving the request has the latest block.
    #[tracing::instrument(skip_all fields(logs))]
    async fn logs(&self, f: Filter) -> eyre::Result<Vec<Log>, IndexError> {
        let mut batch = BatchRequest::new(self.client());
        let block: Waiter<Block> = batch
            .add_call(
                "eth_getBlockByNumber",
                &(U64::from(f.get_to_block().unwrap()), false),
            )
            .wrap_err("building eth_getBlockByNumber")?;
        let logs: Waiter<Vec<Log>> = batch
            .add_call("eth_getLogs", &(&f,))
            .wrap_err("building eth_getLogs")?;
        batch.send().await.wrap_err("making batch call")?;
        let (_block, logs) = (
            block
                .await
                .map_err(|e| IndexError::Retry(eyre!("block {}", e)))?,
            logs.await
                .map_err(|e| IndexError::Retry(eyre!("logs {}", e)))?,
        );
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
        tracing::Span::current().record("logs", logs.len());
        Ok(logs)
    }
}

#[tracing::instrument(fields(local, remote, removed) skip_all)]
async fn next_to_index<F: EthApi>(
    pgtx: &Transaction<'_>,
    remote: &F,
    batch_size: u64,
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
        let delta = cmp::min(remote_num - local_num, batch_size);
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
                with latest as (
                    select max(block_num) as block_num, address, table_id, key
                    from records
                    where expired
                    and block_num >= $1
                    group by address, table_id, key
                )
                update records r set expired = false
                from latest
                where (r.address, r.table_id, r.key) = (latest.address, latest.table_id, latest.key)
                and r.block_num = latest.block_num
                ",
                &[&U64::from(cmp::max(local_num as i64 - max_reorg as i64, 0))],
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

#[tracing::instrument(fields(from, to, updates, records, updated) skip_all)]
pub async fn index<T: EthApi>(
    remote: &T,
    pg: &mut Client,
    batch_size: u64,
) -> eyre::Result<(), IndexError> {
    let pgtx = pg.transaction().await.wrap_err("opening index tx")?;
    let next = next_to_index(&pgtx, remote, batch_size, 100).await?;
    pgtx.commit().await.wrap_err("unable to commit tx")?;

    let filter = Filter::new()
        .events(&[
            &Store_SetRecord::SIGNATURE,
            &Store_SpliceDynamicData::SIGNATURE,
            &Store_SpliceStaticData::SIGNATURE,
            &Store_DeleteRecord::SIGNATURE,
        ])
        .select(next.from.num..next.to.num);
    let updates: Vec<Update> = remote
        .logs(filter)
        .await?
        .into_iter()
        .map(Update::from_log)
        .collect::<Result<Vec<Option<Update>>, IndexError>>()?
        .into_iter()
        .flatten()
        .sorted_by_key(|u| (u.block_num, u.log_idx))
        .collect::<Vec<_>>();

    let record_ids: HashSet<RecordId> = updates.iter().map(|u| u.id()).collect();
    let (updates_count, records_count) = (updates.len(), record_ids.len());

    let tx = pg.transaction().await.wrap_err("opening index tx")?;
    for u in &updates {
        if u.table_id == Schema::TABLES_TABLE_ID {
            save_table(&tx, &u).await?;
        }
    }
    let mut records = Record::load(&tx, record_ids).await?;
    let mut updated = HashSet::new();
    updates.into_iter().for_each(|u| {
        let id = u.id();
        match records.get_mut(&id) {
            Some(r) => {
                updated.insert(id);
                r.update(u);
            }
            None => {
                let mut r = Record::default(id.clone());
                r.update(u);
                records.insert(id, r);
            }
        }
    });
    Record::expire(&tx, updated).await?;
    Record::copy(&tx, records).await?;
    tx.execute(
        "insert into blocks(num, hash) values ($1, $2)",
        &[&U64::from(next.to.num), &next.to.hash],
    )
    .await
    .wrap_err(format!("updating blocks table to latest {}", next.to.num))?;
    tx.commit().await.wrap_err("unable to commit tx")?;

    tracing::Span::current()
        .record("from", next.from.num)
        .record("to", next.to.num)
        .record("updates", updates_count)
        .record("records", records_count);
    Ok(())
}

#[tracing::instrument(fields(block_num, log_idx), skip_all)]
async fn save_table(pgtx: &Transaction<'_>, update: &Update) -> Result<(), IndexError> {
    if let UpdateKind::Set {
        static_data,
        encoded_lengths,
        dynamic_data,
    } = &update.kind
    {
        tracing::Span::current()
            .record("block_num", update.block_num)
            .record("log_idx", update.log_idx);

        let key: B256 = B256::from_slice(&update.key);
        let schema = &Schema::from_data(
            update.address,
            key,
            &mud_encoding::Data::new(&static_data, *encoded_lengths, &dynamic_data)?,
        )?;
        schema
            .insert(pgtx, update.block_num, update.log_idx, update.address)
            .await?
    }
    Ok(())
}

type RecordId = (FixedBytes<20>, FixedBytes<32>, Vec<u8>);

#[derive(Debug)]
struct Record {
    block_num: U64,
    log_idx: U64,
    address: FixedBytes<20>,
    table_id: FixedBytes<32>,
    key: Vec<u8>,
    static_data: Vec<u8>,
    encoded_lengths: FixedBytes<32>,
    dynamic_data: Vec<u8>,
    deleted: bool,
}

impl Record {
    fn id(&self) -> RecordId {
        (self.address, self.table_id, self.key.clone())
    }

    fn default(id: RecordId) -> Self {
        Record {
            block_num: U64::from(0),
            log_idx: U64::from(0),
            address: id.0,
            table_id: id.1,
            key: id.2,
            static_data: vec![],
            encoded_lengths: FixedBytes::<32>::ZERO,
            dynamic_data: vec![],
            deleted: false,
        }
    }

    fn from_row(row: &Row) -> Result<Self, tokio_postgres::Error> {
        Ok(Record {
            block_num: row.try_get("block_num")?,
            log_idx: row.try_get("log_idx")?,
            address: row.try_get("address")?,
            table_id: row.try_get("table_id")?,
            key: row.try_get("key")?,
            static_data: row.try_get("static_data")?,
            encoded_lengths: row.try_get("encoded_lengths")?,
            dynamic_data: row.try_get("dynamic_data")?,
            deleted: row.try_get("deleted")?,
        })
    }

    #[tracing::instrument(skip_all fields(records))]
    async fn load(
        tx: &Transaction<'_>,
        recs: HashSet<RecordId>,
    ) -> Result<HashMap<RecordId, Record>, IndexError> {
        const Q: &str = "
            with q as (
                select
                    unnest($1::bytea[]) address,
                    unnest($2::bytea[]) table_id,
                    unnest($3::bytea[]) key
            )
            select block_num, log_idx, r.address, r.table_id, r.key, static_data, encoded_lengths, dynamic_data, deleted
            from records r
            join q
            on (r.address, r.table_id, r.key) =  (q.address, q.table_id, q.key)
            and not r.expired
        ";
        let loaded_recs: Vec<Record> = tx
            .query(
                Q,
                &[
                    &recs.iter().map(|r| r.0).collect::<Vec<FixedBytes<20>>>(),
                    &recs.iter().map(|r| r.1).collect::<Vec<FixedBytes<32>>>(),
                    &recs.iter().map(|r| &r.2).collect::<Vec<&Vec<u8>>>(),
                ],
            )
            .await?
            .iter()
            .map(Self::from_row)
            .collect::<Result<Vec<Record>, _>>()?;
        tracing::Span::current().record("records", loaded_recs.len());
        Ok(loaded_recs.into_iter().map(|r| (r.id(), r)).collect())
    }

    #[tracing::instrument(skip_all fields(records))]
    async fn expire(tx: &Transaction<'_>, recs: HashSet<RecordId>) -> Result<u64, IndexError> {
        const Q: &str = r#"
            with q as (
                select
                    unnest($1::bytea[]) as address,
                    unnest($2::bytea[]) as table_id,
                    unnest($3::bytea[]) as key
            )
            update records set expired = true
            from q
            where (records.address, records.table_id, records.key) = (q.address, q.table_id, q.key)
            and not records.expired;
        "#;
        tx.execute(
            Q,
            &[
                &recs.iter().map(|r| r.0).collect::<Vec<FixedBytes<20>>>(),
                &recs.iter().map(|r| r.1).collect::<Vec<FixedBytes<32>>>(),
                &recs.iter().map(|r| &r.2).collect::<Vec<&Vec<u8>>>(),
            ],
        )
        .await
        .map_err(|err| IndexError::Fatal(eyre!("error: {}", err)))
        .inspect(|res| {
            tracing::Span::current().record("records", res);
        })
    }

    #[tracing::instrument(skip_all fields(records))]
    async fn copy(tx: &Transaction<'_>, rec: HashMap<RecordId, Record>) -> Result<u64, IndexError> {
        const Q: &str = r#"
            copy records (
                address,
                table_id,
                key,
                static_data,
                encoded_lengths,
                dynamic_data,
                block_num,
                log_idx,
                deleted
            )
            from stdin binary
        "#;
        let sink = tx.copy_in(Q).await.wrap_err("unable to start copy in")?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::BYTEA,
                tokio_postgres::types::Type::NUMERIC,
                tokio_postgres::types::Type::INT4,
                tokio_postgres::types::Type::BOOL,
            ],
        );
        pin_mut!(writer);
        for r in rec.values() {
            writer
                .as_mut()
                .write(&[
                    &r.address,
                    &r.table_id,
                    &r.key,
                    &r.static_data,
                    &r.encoded_lengths,
                    &r.dynamic_data,
                    &r.block_num,
                    &r.log_idx,
                    &r.deleted,
                ])
                .await?;
        }
        writer
            .finish()
            .await
            .map_err(|err| IndexError::Fatal(eyre!("error: {}", err)))
            .inspect(|res| {
                tracing::Span::current().record("records", res);
            })
    }

    fn update(&mut self, u: Update) {
        self.block_num = U64::from(u.block_num);
        self.log_idx = U64::from(u.log_idx);
        match u.kind {
            UpdateKind::Del => {
                self.deleted = true;
                self.static_data.clear();
                self.encoded_lengths = FixedBytes::<32>::ZERO;
                self.dynamic_data.clear();
            }
            UpdateKind::Set {
                static_data,
                encoded_lengths,
                dynamic_data,
            } => {
                self.deleted = false;
                self.encoded_lengths = encoded_lengths;
                self.static_data = static_data.to_vec();
                self.dynamic_data = dynamic_data.to_vec();
            }
            UpdateKind::DSplice {
                encoded_lengths,
                start,
                count,
                data,
            } => {
                self.deleted = false;
                self.encoded_lengths = encoded_lengths;
                splice(
                    &mut self.dynamic_data,
                    start as usize,
                    count as usize,
                    &data,
                );
            }
            UpdateKind::SSplice { start, data } => {
                self.deleted = false;
                splice(&mut self.static_data, start as usize, data.len(), &data);
            }
        }
    }
}

#[derive(Debug)]
enum UpdateKind {
    Del,
    Set {
        static_data: Bytes,
        encoded_lengths: FixedBytes<32>,
        dynamic_data: Bytes,
    },
    DSplice {
        encoded_lengths: FixedBytes<32>,
        start: u64,
        count: u64,
        data: Bytes,
    },
    SSplice {
        start: u64,
        data: Bytes,
    },
}

#[derive(Debug)]
struct Update {
    block_num: u64,
    log_idx: u64,
    address: FixedBytes<20>,
    table_id: FixedBytes<32>,
    key: Vec<u8>,
    kind: UpdateKind,
}

impl Update {
    fn id(&self) -> RecordId {
        (self.address, self.table_id, self.key.clone())
    }

    fn from_log(log: Log) -> Result<Option<Self>, IndexError> {
        let (block_num, log_addr, log_idx) = (
            log.block_number.wrap_err("missing block num from log")?,
            *log.address(),
            log.log_index.wrap_err("missing log idx from log")?,
        );
        match log.topics().first().unwrap_or_default() {
            &Store_SetRecord::SIGNATURE_HASH => {
                let rec = Store_SetRecord::decode_log_data(log.data(), true)
                    .wrap_err("decoding set record")?;
                Ok(Some(Update {
                    block_num: block_num,
                    log_idx: log_idx,
                    address: log_addr,
                    table_id: rec.table_id,
                    key: flatten_key(rec.key_tuple),
                    kind: UpdateKind::Set {
                        static_data: rec.static_data,
                        encoded_lengths: rec.encoded_lengths,
                        dynamic_data: rec.dynamic_data,
                    },
                }))
            }
            &Store_SpliceDynamicData::SIGNATURE_HASH => {
                let rec = Store_SpliceDynamicData::decode_log_data(log.data(), true)
                    .wrap_err("decoding splice dynamic")?;
                Ok(Some(Update {
                    block_num: block_num,
                    log_idx: log_idx,
                    address: log_addr,
                    table_id: rec.table_id,
                    key: flatten_key(rec.key_tuple),
                    kind: UpdateKind::DSplice {
                        encoded_lengths: rec.encoded_lengths,
                        start: rec.start,
                        count: rec.delete_count,
                        data: rec.data,
                    },
                }))
            }
            &Store_SpliceStaticData::SIGNATURE_HASH => {
                let rec = Store_SpliceStaticData::decode_log_data(log.data(), true)
                    .wrap_err("decoding splice static")?;
                Ok(Some(Update {
                    block_num: block_num,
                    log_idx: log_idx,
                    address: log_addr,
                    table_id: rec.table_id,
                    key: flatten_key(rec.key_tuple),
                    kind: UpdateKind::SSplice {
                        start: rec.start,
                        data: rec.data,
                    },
                }))
            }
            &Store_DeleteRecord::SIGNATURE_HASH => {
                let rec = Store_DeleteRecord::decode_log_data(log.data(), true)
                    .wrap_err("decoding delete record")?;
                Ok(Some(Update {
                    block_num: block_num,
                    log_idx: log_idx,
                    address: log_addr,
                    table_id: rec.table_id,
                    key: flatten_key(rec.key_tuple),
                    kind: UpdateKind::Del,
                }))
            }
            _ => Ok(None),
        }
    }
}

fn flatten_key(key: Vec<FixedBytes<32>>) -> Vec<u8> {
    key.into_iter().flat_map(|b| b.0.into_iter()).collect()
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
    static SCHEMA: &'static str = include_str!("./schema.sql");

    use alloy::primitives::{Address, LogData, B256};
    use postgresql_embedded::{PostgreSQL, Settings, Version};
    use tokio_postgres::NoTls;
    use tracing_subscriber::FmtSubscriber;

    use super::*;
    use std::sync::Once;

    static LOGGING_INIT: Once = Once::new();

    fn logging() {
        LOGGING_INIT.call_once(|| {
            let subscriber = FmtSubscriber::builder()
                .with_max_level(tracing::Level::INFO)
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("setting default subscriber failed");
        });
    }

    async fn test_pg() -> (PostgreSQL, Client) {
        let mut pg_settings = Settings::default();
        pg_settings.version = Version::new(16, Some(2), Some(3));
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
            .batch_execute(SCHEMA)
            .await
            .expect("resetting schema");
        (db, client)
    }

    fn test_block(num: u64, hash: u8, parent: u8) -> Block {
        let mut block = Block::default();
        block.header.number = Some(num);
        block.header.hash = Some(FixedBytes::with_last_byte(hash));
        block.header.parent_hash = FixedBytes::with_last_byte(parent);
        block
    }

    fn wrap_log(
        b: Block,
        log_index: u64,
        l: alloy::primitives::Log<LogData>,
    ) -> alloy::rpc::types::eth::Log {
        Log {
            inner: l,
            block_hash: b.header.hash,
            block_number: b.header.number,
            block_timestamp: None,
            transaction_hash: Some(B256::with_last_byte(0x01)),
            transaction_index: Some(0x01),
            log_index: Some(log_index),
            removed: false,
        }
    }

    fn un_flatten_key(key: Vec<u8>) -> Vec<FixedBytes<32>> {
        key.chunks_exact(32)
            .map(|chunk| FixedBytes::<32>::from_slice(chunk))
            .collect()
    }

    fn dr(b: Block, log_index: u64, id: RecordId) -> Log {
        wrap_log(
            b,
            log_index,
            alloy::primitives::Log::new_from_event(
                Address(id.0),
                Store_DeleteRecord {
                    table_id: id.1,
                    key_tuple: un_flatten_key(id.2),
                },
            )
            .unwrap()
            .reserialize(),
        )
    }

    fn sr(b: Block, log_index: u64, id: RecordId) -> Log {
        wrap_log(
            b,
            log_index,
            alloy::primitives::Log::new_from_event(
                Address(id.0),
                Store_SetRecord {
                    table_id: id.1,
                    key_tuple: un_flatten_key(id.2),
                    static_data: Bytes::from_static(&[0x01]),
                    encoded_lengths: B256::with_last_byte(0x00),
                    dynamic_data: Bytes::from_static(&[0x01]),
                },
            )
            .unwrap()
            .reserialize(),
        )
    }

    fn ss(b: Block, log_index: u64, id: RecordId, start: u64, data: Bytes) -> Log {
        wrap_log(
            b,
            log_index,
            alloy::primitives::Log::new_from_event(
                Address(id.0),
                Store_SpliceStaticData {
                    table_id: id.1,
                    key_tuple: un_flatten_key(id.2),
                    start,
                    data,
                },
            )
            .unwrap()
            .reserialize(),
        )
    }

    struct TestGetRemote(Block, Vec<Log>);

    #[async_trait]
    impl EthApi for TestGetRemote {
        async fn logs(&self, _: Filter) -> eyre::Result<Vec<Log>, IndexError> {
            Ok(self.1.clone())
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
    async fn test_index() {
        logging();
        let (_pg_server, mut pg) = test_pg().await;
        pg.execute(
            "insert into blocks(num, hash) values ($1, $2)",
            &[&U64::from(0), &FixedBytes::<32>::ZERO],
        )
        .await
        .expect("setting up blocks table");

        let rid1 = (
            FixedBytes::<20>::with_last_byte(0x01),
            B256::with_last_byte(0x01),
            B256::with_last_byte(0x01).to_vec(),
        );
        let mut ids = HashSet::new();
        ids.insert(rid1.clone());

        {
            super::index(
                &TestGetRemote {
                    0: test_block(1, 1, 0),
                    1: vec![
                        sr(test_block(1, 1, 0), 1, rid1.clone()),
                        ss(
                            test_block(1, 1, 0),
                            2,
                            rid1.clone(),
                            0,
                            Bytes::copy_from_slice(&B256::repeat_byte(0x1)[..]),
                        ),
                        dr(test_block(1, 1, 0), 3, rid1.clone()),
                    ],
                },
                &mut pg,
                1,
            )
            .await
            .expect("unable to index");
            let pgtx = pg.transaction().await.expect("opening index tx");
            let recs = Record::load(&pgtx, ids.clone())
                .await
                .expect("loading records");
            let rec = recs.get(&rid1).expect("finding record");
            assert_eq!(rec.static_data, &[0u8; 0]);
        }
        {
            super::index(
                &TestGetRemote {
                    0: test_block(2, 2, 1),
                    1: vec![ss(
                        test_block(2, 2, 1),
                        2,
                        rid1.clone(),
                        0,
                        Bytes::copy_from_slice(&B256::repeat_byte(0x1)[..]),
                    )],
                },
                &mut pg,
                1,
            )
            .await
            .expect("unable to index");
            let pgtx = pg.transaction().await.expect("opening index tx");
            let recs = Record::load(&pgtx, ids.clone())
                .await
                .expect("loading records");
            let rec = recs.get(&rid1).expect("finding record");
            assert_eq!(rec.static_data, &[1u8; 32]);
        }
    }

    #[tokio::test]
    async fn test_next_to_index() {
        logging();
        let (_pg_server, mut pg) = test_pg().await;
        let pgtx = pg.transaction().await.expect("opening index tx");
        pgtx.execute(
            "insert into blocks(num, hash) values ($1, $2)",
            &[&U64::from(0), &FixedBytes::<32>::ZERO],
        )
        .await
        .expect("setting up blocks table");

        let trg = TestGetRemote {
            0: test_block(10, 10, 9),
            1: vec![],
        };
        let next_range = next_to_index(&pgtx, &trg, 10, 1).await.unwrap();
        assert_eq!(next_range.from.num, 1);
        assert_eq!(next_range.to.num, 10);
    }

    #[tokio::test]
    async fn test_next_to_index_reorg() {
        logging();
        let (_pg_server, mut pg) = test_pg().await;
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
            insert into records(address, table_id, key, block_num, log_idx, expired)
            values ('\x01', '\x01', '\x01', 0, 0, true), ('\x01', '\x01', '\x01', 1, 0, false)
            "#,
            &[],
        )
        .await
        .expect("setting up blocks table");

        let trg = TestGetRemote {
            0: test_block(2, 2, 1),
            1: vec![],
        };
        next_to_index(&pgtx, &trg, 2, 2).await.unwrap();

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
            .query("select block_num, expired from records", &[])
            .await
            .expect("querying records table");
        let mut got: Vec<(U64, bool)> = vec![];
        for row in rows {
            got.push((row.get("block_num"), row.get("expired")))
        }
        assert_eq!(got, vec![(U64::from(0), false)]);
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
