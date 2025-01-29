create extension if not exists pg_stat_statements;

create or replace function sdec(data bytea, i int, n int)
returns bytea as $$
begin
    if data is null then
        return null;
    elseif i + n - 1 > length(data) then
        return null;
    end if;
    return substring(data from i+1 for n);  -- substring is 1-index
end;
$$ language plpgsql strict immutable parallel safe cost 1;

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
$$ language plpgsql strict immutable parallel safe cost 1;

create or replace function b2sn(b bytea)
returns numeric as $$
declare
    n numeric := 0;
    len int;
    is_neg bool;
begin
    len := length(b);
    if len > 32 then
        raise exception 'input exceeds maximum length of 32 bytes';
    end if;

    is_neg := (get_byte(b, 0) & 128) > 0;
    if is_neg then
        for i in 1..len loop
            n := n * 256 + (~get_byte(b, i - 1) & 255);
        end loop;
        n := (n + 1) * -1;
    else
        for i in 1..length(b) loop
            n := n * 256 + get_byte(b, i - 1);
        end loop;
    end if;

    return n;
end;
$$ language plpgsql strict immutable parallel safe cost 1;

create table if not exists blocks (num numeric primary key, hash bytea, parent bytea);

create table if not exists records(
    address bytea,
    table_id bytea,
    key bytea,
    static_data bytea,
    encoded_lengths bytea,
    dynamic_data bytea,
    block_num numeric,
    tx_hash bytea,
    log_idx int,
    expired bool default false not null,
    deleted bool default false not null,
    primary key (address, table_id, key, block_num, log_idx)
);
create unique index if not exists "records_not_expired" on records (address, table_id, key) where not expired;

create index if not exists "records_all"
on records(address, table_id, sdec(key, 0, 32), sdec(key, 32, 32))
where not expired and not deleted;

create index if not exists "records_all_static_num"
on records(address, table_id, sdec(key, 0, 32), sdec(key, 32, 32), b2n(sdec(static_data, 0, 32)))
where not expired and not deleted;

create index if not exists "records_key_0" on records(sdec(key, 0, 32)) where not expired and not deleted;
create index if not exists "records_key_1" on records(sdec(key, 32, 32)) where not expired and not deleted;

create index if not exists "records_block_num" on records(block_num desc) where not expired and not deleted;

create table if not exists tables(
    block_num numeric,
    log_idx numeric,
    address bytea,
    id bytea,
    name text,
    key_schema bytea,
    val_schema bytea,
    key_names text[],
    val_names text[],
    primary key (address, id)
);

create or replace function b2ab(data bytea, n int)
returns bytea[] as $$
declare
    nparts int;
    parts bytea[];
begin
    nparts := ceil(length(data) / n::float);
    parts := array[]::bytea[];
    for i in 0..(nparts- 1) loop
        parts := array_append(parts, substring(data, (i * n) + 1, n));
    end loop;
    return parts;
end;
$$ language plpgsql immutable parallel safe cost 1;

create or replace function b2an(data bytea, n int)
returns numeric[] as $$
declare
    nparts int;
    parts numeric[] = array[]::numeric[];
begin
    nparts := ceil(length(data) / n::float);
    for i in 0..(nparts- 1) loop
        parts := array_append(parts, b2n(substring(data, (i * n) + 1, n)));
    end loop;
    return parts;
end;
$$ language plpgsql immutable parallel safe cost 1;

create or replace function ddec(encoded_lengths bytea, dynamic_data bytea, field int)
returns bytea as $$
declare
    field_start int := 1; --substring is index-1
    field_length int;
    tmp int;
begin
    if length(encoded_lengths) != 32 then
        raise exception 'encoded_length must be 32 bytes got %', length(encoded_lengths);
    end if;

    tmp := 20 - (field * 5);
    field_length := get_byte(encoded_lengths, tmp)::int * 256^4 +
                    get_byte(encoded_lengths, tmp + 1)::int * 256^3 +
                    get_byte(encoded_lengths, tmp + 2)::int * 256^2 +
                    get_byte(encoded_lengths, tmp + 3)::int * 256 +
                    get_byte(encoded_lengths, tmp + 4)::int;

    -- for i in 0..-1 doesn't run
    -- for i in 0..3 runs 4 times
    FOR i IN 0..(field-1) LOOP
        tmp := 20 - (i * 5);
        field_start := field_start + get_byte(encoded_lengths, tmp)::int * 256^4 +
                                get_byte(encoded_lengths, tmp + 1)::int * 256^3 +
                                get_byte(encoded_lengths, tmp + 2)::int * 256^2 +
                                get_byte(encoded_lengths, tmp + 3)::int * 256 +
                                get_byte(encoded_lengths, tmp + 4)::int;
    END LOOP;
    RETURN substring(dynamic_data FROM field_start FOR field_length);
end;
$$ language plpgsql immutable parallel safe cost 1;
