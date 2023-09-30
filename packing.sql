-- attach_sql
attach database ?1 as source;

-- get_page_size_sql
pragma source.page_size;

-- set_page_size_fmt
pragma page_size=%u;

-- begin_sql
begin immediate transaction;

-- create_temps_sql
create table temp.split (
    split_id integer primary key
);

create table temp.frag (
    frag_id integer primary key,
    "offset" integer not null,
    size integer not null,
    cell_size integer not null,
    split_id integer not null,
    page_id integer,
    final_id integer unique
);

create index temp.frag_size
    on frag (cell_size);

create index temp.frag_split
    on frag (split_id);

create index temp.frag_page
    on frag (page_id);

-- list_blobs_sql
select id, length(val)
    from source.blobs
    order by id;

-- insert_temp_split_sql
insert into temp.split (split_id)
    values (?1);

-- insert_temp_frag_sql
insert into temp.frag (frag_id, "offset", size, cell_size, split_id)
    values (?1, ?2, ?3, ?4, ?5);

-- create_temp_page_sql
create table temp.page (
    page_id integer primary key,
    free_space integer
);

create index temp.page_space
    on page (free_space)
    where free_space is not null;

-- min_size_sql
select min(cell_size) from temp.frag;

-- list_frags_sql
select frag_id, cell_size from temp.frag
    order by cell_size desc;

-- find_page_sql
select page_id, free_space from temp.page
    where free_space is not null and free_space>=?1
    order by free_space
    limit 1;

-- insert_page_sql
insert into temp.page (page_id, free_space)
    values (?1, ?2);

-- update_page_sql
update temp.page
    set free_space=?2
    where page_id=?1;

-- assign_page_sql
update temp.frag
    set page_id=?2
    where frag_id=?1;

-- unsplit_sql
create table temp.unsplit (
    split_id integer primary key
);

insert into temp.unsplit
    select split_id from temp.frag
        group by split_id, page_id
        having count(*)>=2
    union all select split_id from
        (select min(split_id) as split_id from temp.frag
             group by page_id
             having count(*)=1)
        group by split_id
        having count(*)>=2;

update temp.frag as f1
    set size=(select sum(f2.size) from temp.frag f2
                  where f2.split_id=f1.split_id)
    where split_id in temp.unsplit
        and "offset"=0;

delete from temp.frag
    where split_id in temp.unsplit
        and "offset">0;

drop table temp.unsplit;

-- create_order_sql
create table temp.split_order (
    split_seq integer primary key,
    split_id integer not null unique
);

create table temp.page_order (
    page_seq integer primary key,
    page_id integer not null unique
);

-- one_split_sql
insert into temp.split_order (split_id)
    select split_id from temp.split
        where split_id>=?1
            and split_id not in (select split_id from temp.split_order)
        order by split_id
        limit 1
    returning split_id;

-- more_pages_sql
insert or ignore into temp.page_order (page_id)
    select page_id
        from temp.split_order
            natural cross join temp.frag
        where split_seq>?1;

-- more_splits_sql
insert or ignore into temp.split_order (split_id)
    select split_id
        from temp.page_order
            natural cross join temp.frag
        where page_seq>=?1;

-- order_frags_sql
drop table temp.split_order;

with
    frag_order (frag_id, final_id) as
        (select frag_id, row_number() over (order by page_seq, frag_id)
             from temp.frag
                  natural join temp.page
                  natural join temp.page_order)
update temp.frag as f
    set final_id=frag_order.final_id
    from frag_order
    where f.frag_id=frag_order.frag_id;

drop table temp.page_order;

drop table temp.page;

-- write_splits_sql
create table main.splits (
    id integer primary key,
    head integer,
    tail integer
);

insert into main.splits (id, head, tail)
    select split_id,
            (select f0.final_id from temp.frag f0
                 where f0.split_id=s.split_id
                 order by f0.frag_id
                 limit 1),
            (select f1.final_id from temp.frag f1
                 where f1.split_id=s.split_id
                 order by f1.frag_id
                 limit 1
                 offset 1)
        from temp.split s
        order by split_id;

drop table temp.split;

-- write_frags_sql
create table main.frags (
    id integer primary key,
    val blob not null
);

insert into main.frags (id, val)
    select f.final_id, substr(s.val, f."offset"+1, f.size)
        from temp.frag f,
            source.blobs s on s.id=f.split_id
        order by f.final_id;

drop table temp.frag;

-- commit_sql
commit transaction;

