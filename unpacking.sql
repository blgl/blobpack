-- attach_sql
attach database ?1 as source;

-- get_page_size_sql
pragma source.page_size;

-- set_page_size_fmt
pragma page_size=%u;

-- begin_sql
begin immediate transaction;

-- create_blob_sql
create table main.blobs (
    id integer primary key,
    val blob
);

-- extract_frags_sql
select s.id, h.val, t.val
    from source.splits s
        left join source.frags h on h.id=s.head
        left join source.frags t on t.id=s.tail
    order by s.id;

-- insert_blob_sql
insert into main.blobs (id, val)
    values (?1, ?2);

-- commit_sql
commit transaction;

