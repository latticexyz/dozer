drop schema public cascade;
create schema public;
create table if not exists blocks (num numeric primary key, hash bytea, parent bytea);
create table if not exists records(
    table_id bytea,
    key bytea[],
    static_data bytea,
    dynamic_lengths bytea,
    dynamic_data bytea,
    block_num numeric,
    log_idx numeric,
    expired_block_num numeric,
    expired_log_idx int,
    deleted bool,
    primary key (table_id, key, block_num, log_idx)
);