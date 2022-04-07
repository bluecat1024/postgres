set allow_system_table_mods=true;
drop table pg_catalog.pg_qcache;
create table pg_catalog.pg_qcache(
    queryid int64,
    dbid oid,
    procid int32,
    timestamp int64,
    primary key(queryid, dbid, procid)
);