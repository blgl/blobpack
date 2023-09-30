Sample code showing how to minimise unused space in a blob-heavy SQLite3
database to be distributed.  If a blob could cause unused space, split it
in two before distribution and re-join it at the destination.

The `blobpack` program transforms a database with this schema

```
create table blobs (
    id integer primary key,
    val blob
);
```

into one with this schema

```
create table frags (
    id integer primary key,
    val blob not null
);

create table splits (
    id integer primary key,
    head integer references frags,
    tail integer references frags
);
```

If the source `val` is `NULL`, both `head` and `tail` are `NULL`.
If a blob doesn't need splitting, `tail` is `NULL`.

The `blobunpack` program performs the inverse transformation.
