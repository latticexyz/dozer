# Dozer: an indexer for MUD

<img src="https://github.com/latticexyz/dozer/blob/master/dozer.png?raw=true" width=200px />

- [Logs API](#logs-api)
- [Config](#config)
- [Data Model](#data-model)

Dozer downloads MUD Store logs from an eth rpc get_logs api and saves
the data into a Postgres table named records. Dozer provides a JSON
HTTP API that allows users to request complete records by table_id,
key_tuple, and block number. The block number in the request enables
point-in-time queries.

## Logs API

Returns complete, encoded records. This API is useful because it
reduces splice{dynamic,static} event logs into a complete Record.

### `GET /api/logs`

#### Request

Query Parameters: `?input=`

Where `input` is a percent encoded JSON object with the following fields:

```
{
    "chainId": 0,
    "address": "",
    "filter": [
        {
            "tableId": "",
            "key0": "",
            "key1": "",
        }
    ]
}
```

- An `input.filter` must have `tableId`
- An `input.filter` may have `key0` or `key1`

#### Response

- If the request has at least 1 filter then the response contains all records
  for the Tables table
- If the request contains an address then all records are associated with the
  address
- If the request contains filters, then each returned record matches at least
  one of the filters for all specified fileds within the filter.
- Returns (404, "no logs found") when no logs are found in db
- Returns (200, json_resp) when logs are found

Where `json_resp` is:

```
{
    "blockNumber": 0,
    "logs": [
        {
            "address": "0x...",
            "eventName": "Store_SetRecord",
            "args": {
                "tableId": "0x...",
                "keyTuple": ["0x..."],
                "staticData": "0x...",
                "encodedLengths": "0x...",
                "dynamicData": "0x..."
            }
        }
    ]
}
```

Where `blockNumber` is the latest block processed by the indexer and `eventName` is always "Store_SetRecord".

The remaining items are the latest version of the record. (aggregated from all prior set/splice logs)

## Config

Configuration is first read by the command flag. If it is missing, it
is read from the os environment. If it is still missing a compiled
deafult is used.

### Indexing

flag: `--no-index`

default: `false`

Set this if you would like to run API only and disable indexing.

### HTTP API

flag: `-l, --listen <address:port>`

default: `0.0.0.0:8000`

### Ethereum

flag: `-e, --eth-url <ETH_URL>`

env var: `$ETH_URL`

default: `http://localhost:8545`

### Postgres

A [postgres connection uri](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS)

flag: `-p, --pg-url <PG_URL>`

env var: `$PG_URL`

default: `postgres://localhost/dozer`

## Data Model

Dozer defines the records table as:

```

table_id bytea
key_tuple bytea[]
static_data bytea
dynamic_lengths bytea
dynamic_data bytea
block_num numeric
log_idx int
expired_block_num numeric
expired_log_idx int
deleted bool

```

Each row in this table represents a complete record. The latest
version of the record is a row in which expired_block_num and
expired_log_idx is null. The following considerations are made when
processing logs:

A SetRecord log contains a complete copy of static bytes, dynamic
lengths, and dynamic bytes and so we can expire all previous records
for the table_id/key and insert a new, complete record into the table.

A splice request is meant to replace a set of bytes within the static
or dynamic bytes arrays. Each request contains:

    i   = position to delete from current byte array
    n   = number of bytes to delete from current byte array
    new = bytes to insert at position i

A SpliceStaticData log contains a splice request for the static bytes
and so we must find the previous record in the table using the
table_id/key in order to consturct a new, complete record. A
SpliceStaticData log does not change the length of the static byte
array.

A SpliceDynamicData log contians a splice request for the dynamic
bytes along with a complete dynamic lengths value and so we must find
a previous record using the table_id/key in order to construct a new,
complete record.

A SpliceDynamicData log may change the length of the dynamic byte
array. If the splice changes the size of the previous dynamic byte
array then the added or removed bytes must occur at the end of the
previous dynamic byte array. The start value (i) for the splice
request indicates the precise position in the dynamic data byte array
and is not relative to the dynamicFieldIndex in the log. The log will
contain a new dynamic lengths value that can replace the previous
value.

A Splice{Dynamic,Static}Data log may proceed a Set log. In this case,
if i > 0 then i bytes are inserted before the new bytes.

A DeleteRecord log contains the table_id/key of the record to be
delted. In this case we find the previous, un-expired record and mark
it as expired and additionally set deleted=true. A
Splice{Dynamic,Static}Data or SetRecord log may follow a DeleteRecord
log and in this case we do not carry forward the previous static_data,
dynamic_lengths, or dynamic_data.

Many logs for a particular table_id/key may exist within a block.

## Installation

Install system dependencies for Linux
```
sudo apt install -y build-essential pkg-config libssl-dev
```

Install Rust toolchain for Mac/Linux.
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Download and build dozer
```
git clone git@github.com:latticexyz/dozer.git
cd dozer
cargo build
```

## CLI

For an up-to-date menu of CLI options please install and use the `-h` flag.

## Backup

Dozer will use pg_dump to backup the database and then it will
upload the compressed backup to an S3 bucket. The S3 bucket is
configured with a lifecycle policy such that we delete old backups.

Here is the rough outline of _how_ dozer will preform the backup:

1. Check the local directory for files named `dozer-backup-$unix_timestamp`
2. Run pg_dump with the current time if the last backup was older than `--backup-window` or if there are no local backups
3. Check the s3 bucket for object with keys named: `dozer-backup-$unix_timestamp`
4. Upload the latest local backup if it is newer than the latest backup in S3 or if there are no remote backups
5. Remove all but the latest local backup

The backup routine will run whenever dozer is running unless the `no-backup` flag is set.

The backup routine will be triggered from within dozer every 60s.

There is also a `backup` sub-command if you want to trigger a backup without running a dozer server. See `dozer backup --help` for more details.

## Restore

Restore will download a backup from S3, create a dozer database on localhost, and restore the backup.

If a database (specified by `--pg-url`) already exists then `restore` will exit with error after downloading the backup from S3.

**Operator must ensure that `dozer server` is not running concurrently with `dozer server`.**

Restore from a sepcific S3 key
```
dozer restore --pg-url postgres://localhost/dozer dozer-backup-$unix_timestamp
```

Restore from the latest backup in S3
```
dozer restore --pg-url postgres://localhost/dozer
```
