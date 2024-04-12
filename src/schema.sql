create extension if not exists btree_gin;

create table if not exists blocks (num numeric primary key, hash bytea, parent bytea);
create table if not exists records(
    table_id bytea,
    key bytea[],
    static_data bytea,
    encoded_lengths bytea,
    dynamic_data bytea,
    block_num numeric,
    log_idx numeric,
    expired_block_num numeric,
    expired_log_idx int,
    deleted bool default false,
    primary key (table_id, key, block_num, log_idx)
);
create index if not exists "records_table_key" on records using gin(table_id, key);