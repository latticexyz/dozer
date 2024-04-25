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

create table if not exists tables(
    block_num numeric,
    log_idx numeric,
    id bytea,
    name text,
    key_schema bytea,
    val_schema bytea,
    key_names text[],
    val_names text[],
    primary key (id)
);

create or replace function b2n(b bytea)
returns numeric as $$
declare
    n numeric := 0;
begin
    if length(b) > 32 then
        raise exception 'input exceeds maximum length of 32 bytes';
    end if;
    for i in 1..length(b) loop
        n:= n * 256 + get_byte(b, i - 1); -- Shift left by 8 bits and add current byte directly
    end loop;
    return n;
end;
$$ language plpgsql strict immutable;

create or replace function sdec(data bytea, i int, n int)
returns bytea as $$
begin
    if i + n - 1 > length(data) then
        raise exception 'index out of bounds. position % plus length % exceeds total length %.', i, n, length(data);
    end if;
    return substring(data from i for n);  -- substring is 1-index
end;
$$ language plpgsql strict immutable;