drop table if exists pg_catalog.pg_qcache;
create table pg_catalog.pg_qcache(
    queryid bigint,
    dbid oid,
    procid int32,
    timestamp int64,
    primary key(queryid, dbid, procid)
);