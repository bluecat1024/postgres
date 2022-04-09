#include "postgres.h"
#include "fmgr.h"
#include <inttypes.h>

#include "access/nbtree.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/explain.h"
#include "miscadmin.h"
#include "optimizer/planner.h"
#include "storage/backendid.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

#define TABLE_COLUMN 5
#define TABLE_NAME "pg_qcache"
#define INDEX_NAME "pg_qcache_pkey"

void		_PG_init(void);
void		_PG_fini(void);

static bool IndexLookup(Relation index_relation, IndexTuple ind_tup);
static void NormalQuit(QueryDesc *query_desc);
static void InitOids();
static void qcache_ExecutorEnd(QueryDesc *query_desc);

static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static Oid index_oid = -1;
static Oid table_oid = -1;
static IndexInfo *index_info = NULL;
static bool oid_initialized = false;

void _PG_init(void) {
    elog(LOG, "QCache extension initialization.");

    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = qcache_ExecutorEnd;
}

void _PG_fini(void) {
    ExecutorEnd_hook = prev_ExecutorEnd;
    index_oid = InvalidOid;
    table_oid = InvalidOid;
}

/* Search the btree index for a given key by the tuple. */
static bool IndexLookup(Relation index_relation, IndexTuple ind_tup) {
    BTScanInsert key;
	BTStack		stack;
	Buffer		lbuf;
	bool		exists = false;

    key = _bt_mkscankey(index_relation, ind_tup);
    key->pivotsearch = true;
    key->scantid = NULL;
    stack = _bt_search(index_relation, key, &lbuf, BT_READ, NULL);
    
    if (BufferIsValid(lbuf)) {
        BTInsertStateData insertstate;
		OffsetNumber offnum;
        Page page;

        insertstate.itup = ind_tup;
        insertstate.itemsz = MAXALIGN(IndexTupleSize(ind_tup));
		insertstate.itup_key = key;
		insertstate.postingoff = 0;
		insertstate.bounds_valid = false;
		insertstate.buf = lbuf;

        /* Get matching tuple on leaf page */
		offnum = _bt_binsrch_insert(index_relation, &insertstate);
        /* Compare first >= matching item on leaf page, if any */
		page = BufferGetPage(lbuf);
        /* Should match on first heap TID when tuple has a posting list */
		if (offnum <= PageGetMaxOffsetNumber(page) &&
			insertstate.postingoff <= 0 &&
			_bt_compare(index_relation, key, page, offnum) == 0) {
            exists = true;
        } else {
            elog(DEBUG1, "QCache page max offset %d",
            (int)PageGetMaxOffsetNumber(page));
            elog(DEBUG1, "QCache page posting offset %d",
            (int)insertstate.postingoff);
            elog(DEBUG1, "QCache lookup compare result %d",
            (int)_bt_compare(index_relation, key, page, offnum));
        }
		_bt_relbuf(index_relation, lbuf);
    }

    _bt_freestack(stack);
	pfree(key);

    return exists;
}

static void NormalQuit(QueryDesc *query_desc) {
    /* Account for previous hook. */
    if (prev_ExecutorEnd != NULL) {
        prev_ExecutorEnd(query_desc);
    } else {
        standard_ExecutorEnd(query_desc);
    }
}

static void InitOids() {
    Relation index_relation;

    if (oid_initialized) {
        return;
    }

    elog(LOG, "QCache: Initializing table oids.");

    index_oid = RelnameGetRelid(INDEX_NAME);
    table_oid = RelnameGetRelid(TABLE_NAME);

    elog(LOG, "QCache Index oid: %d", index_oid);
    elog(LOG, "QCache Table oid: %d", table_oid);

    /* Initialize the index info. */
    index_relation = index_open(index_oid, AccessShareLock);
    index_info = BuildIndexInfo(index_relation);
    relation_close(index_relation, AccessShareLock);

    /* Set the flag. */
    oid_initialized = true;
}

static void qcache_ExecutorEnd(QueryDesc *query_desc) {
    Relation index_relation = NULL;
    Relation table_relation = NULL;
    int idx = 0;
    Datum values[TABLE_COLUMN];
    bool is_nulls[TABLE_COLUMN];
    IndexTuple ind_tup = NULL;
    uint64 queryid = query_desc->plannedstmt->queryId;

    elog(DEBUG1, "QCache Invocation qid: %" PRIu64, queryid);

    /* No handling for query 0. */
    if (queryid == UINT64CONST(0)) {
        NormalQuit(query_desc);
        return;
    }

    InitOids();

    /* Open relation about index. */
    if (index_oid == InvalidOid || table_oid == InvalidOid
        || index_info == NULL) {
        elog(ERROR, "Cannot get the oid. Check the relname for lookup.");
        NormalQuit(query_desc);
        return;
    }

    index_relation = index_open(index_oid, RowExclusiveLock);

    /* Fill in the index tuple.*/
    memset(values, 0, sizeof(values));
    memset(is_nulls, 0, sizeof(is_nulls));
    values[idx++] = Int64GetDatumFast(queryid);
    values[idx++] = ObjectIdGetDatum(MyDatabaseId);
    values[idx++] = Int32GetDatum(MyProcPid);
    ind_tup = index_form_tuple(index_relation->rd_att, values, is_nulls);

    elog(DEBUG1, "QCache prepare lookup: %" PRIu64, queryid);
    /* Insert new tuples to table and index if not found. */
    if (!IndexLookup(index_relation, ind_tup)) {
        HeapTuple heap_tup = NULL;
        TimestampTz stmt_start_ts;
        ItemPointer tid = NULL;

        elog(DEBUG1, "QCache inserting key qid: %" PRIu64, queryid);
        table_relation = table_open(table_oid, RowExclusiveLock);

        stmt_start_ts = GetCurrentStatementStartTimestamp();
        values[idx++] = Int64GetDatumFast(stmt_start_ts);
        is_nulls[idx++] = true;

        elog(DEBUG1, "QCache prepare insert key qid: %" PRIu64, queryid);
        heap_tup = heap_form_tuple(table_relation->rd_att, values, is_nulls);
        simple_heap_insert(table_relation, heap_tup);
        elog(DEBUG1, "QCache inserted heap qid: %" PRIu64, queryid);
        /* Get new tid and add one entry to index. */
        tid = &(heap_tup->t_self);
        btinsert(index_relation, values, is_nulls, tid, table_relation,
            UNIQUE_CHECK_YES, false, index_info);
        elog(DEBUG1, "QCache inserted index qid: %" PRIu64, queryid);

        pfree(heap_tup);
    }

    /* Free all resources. */
    pfree(ind_tup);
    if (table_relation != NULL) {
        table_close(table_relation, RowExclusiveLock);
    }
    if (index_relation != NULL) {
        index_close(index_relation, RowExclusiveLock);
    }

    NormalQuit(query_desc);
}
