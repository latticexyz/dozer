# indexer for mud

- [HTTP API](#api)
- [Config](#config)
- [Data Model](#data-model)

imud downloads MUD Store logs from an eth rpc get_logs api and saves
the data into a Postgres table named records. imud provides a JSON
HTTP API that allows users to request complete records by table_id,
key_tuple, and block number. The block number in the request enables
point-in-time queries.

## API

XXX

## Config

Configuration is first read by the command flag. If it is missing, it
is read from the os environment. If it is still missing a compiled
deafult is used.

### Ethereum

flag: `-e, --eth-url <ETH_URL>`

env var: `$ETH_URL`

default: `http://localhost:8545`

### Postgres

A [postgres connection uri](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING-URIS)

flag: `-p, --pg-url <PG_URL>`

env var: `$PG_URL`

default: `postgres://localhost:imud`

## Data Model

imud defines the records table as:

```
table_id            bytea
key_tuple           bytea[]
static_data         bytea
dynamic_lengths     bytea
dynamic_data        bytea
block_num           numeric
log_idx             int
expired_block_num   numeric
expired_log_idx     int
deleted             bool
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
