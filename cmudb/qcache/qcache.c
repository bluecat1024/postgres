#include "postgres.h"
#include "fmgr.h"
#include <inttypes.h>

#include "access/nbtree.h"
#include "access/heapam.h"
#include "access/relation.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/createas.h"
#include "commands/explain.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/planner.h"
#include "storage/backendid.h"
#include "tscout/executors.h"
#include "utils/builtins.h"
#include "utils/rel.h"

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

/* The definations of plan serialization for Explain/ExecutorEnd. */
static Oid GetScanTableOid(Index rti, EState *estate);
static void ExplainInsertUpdateIndexes(Index rti, ExplainState *es, EState *estate);
static void hutch_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
								   const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv);

SUBST_FN_DECLS

SUBST_EXPLAIN_NODE_ENTRY

SUBST_NODE_TO_NAME

static void AugmentPlan(struct Plan *plan, PlanState *ps, ExplainState *es, EState *estate);
static void WalkPlan(struct Plan *plan, PlanState *ps, ExplainState *es, EState *estate);
static StringInfo GetSerializedExplainOutput(QueryDesc* queryDesc);

static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery = NULL;
static Oid index_oid = -1;
static Oid table_oid = -1;
static IndexInfo *index_info = NULL;
static bool oid_initialized = false;

void _PG_init(void) {
    elog(LOG, "QCache extension initialization.");

    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = qcache_ExecutorEnd;
    prev_ExplainOneQuery = ExplainOneQuery_hook;
    ExplainOneQuery_hook = hutch_ExplainOneQuery;
}

void _PG_fini(void) {
    ExecutorEnd_hook = prev_ExecutorEnd;
    ExplainOneQuery_hook = prev_ExplainOneQuery;
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

/* Normally call executor end hook with previous or default.
   Used both in qcache and hutch, so as not to mess things up. */
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

static Oid GetScanTableOid(Index rti, EState *estate) {
	Relation rel;
	Assert(rti > 0 && rti <= estate->es_range_table_size);
	rel = estate->es_relations[rti - 1];
	if (rel == NULL) {
		RangeTblEntry *rte = exec_rt_fetch(rti, estate);
		Assert(rte->rtekind == RTE_RELATION);
		return rte->relid;
	}

	return rel->rd_id;
}

static void ExplainInsertUpdateIndexes(Index rti, ExplainState *es, EState *estate) {
	Relation rel;
	List *oids;
	Assert(rti > 0 && rti <= estate->es_range_table_size);

	rel = ExecGetRangeTableRelation(estate, rti);
	Assert(rel != NULL);
	oids = RelationGetIndexList(rel);

	ExplainPropertyInteger("ModifyTable_indexupdates_num", NULL, oids ? oids->length : 0, es);
	ExplainPropertyOidList("ModifyTable_indexupdates_oids", oids, es);
}


static void AugmentPlan(struct Plan *plan, PlanState *ps, ExplainState *es, EState *estate) {
	char nodeName[32];
	snprintf(nodeName, sizeof(nodeName), "node-%d", plan->plan_node_id);

	ExplainPropertyText("node", nodeName, es);
	ExplainPropertyInteger("node_id", NULL, plan->plan_node_id, es);
    ExplainPropertyInteger("left_child_node_id", NULL, ChildPlanNodeId(plan->lefttree), es);
    ExplainPropertyInteger("right_child_node_id", NULL, ChildPlanNodeId(plan->righttree), es);

	if (plan->type == T_ModifyTable) {
		ModifyTable *modifyTable = (ModifyTable*)plan;
		// If the current plan is a ModifyTable, then also note the number of input rows.
		Assert(outerPlan(plan) != NULL);
		ExplainPropertyFloat("ModifyTable_input_plan_rows", NULL, outerPlan(plan)->plan_rows, 9, es);
		ExplainPropertyInteger("ModifyTable_input_plan_width", NULL, outerPlan(plan)->plan_width, es);

		if (modifyTable->operation == CMD_INSERT || modifyTable->operation == CMD_UPDATE) {
			// For INSERT/UPDATE, this captures the number of indexes that need to be
			// inserted into in the worst case. For HOT Update, we might not update any
			// indexes at all.
			Index rti;
			Assert(modifyTable->resultRelations != NULL);
			rti = linitial_int(modifyTable->resultRelations);
			ExplainInsertUpdateIndexes(rti, es, estate);
		}

		if (modifyTable->operation == CMD_UPDATE || modifyTable->operation == CMD_DELETE) {
			// For UPDATE/DELETE, this captures the number of repeated scans that we
			// might have had to perform. This is also an upper bound. In reality, if
			// there's no concurrent transaction, this can be much lower.
			double rows = IsolationUsesXactSnapshot() ? 0 : outerPlan(plan)->plan_rows;
			ExplainPropertyFloat("ModifyTable_recheck_rows", NULL, rows, 9, es);
		}
	}

	if (plan->type == T_LockRows) {
		// In this case, we'll actually have to execute the entire subplan
		// multiple times. As such, we note that repeated times == # output rows.
		LockRows *lockRows = (LockRows*)plan;
		double rows = IsolationUsesXactSnapshot() ? 0 : outerPlan(lockRows)->plan_rows;
		ExplainPropertyFloat("LockRows_recheck_rows", NULL, rows, 9, es);
	}
}


static void WalkPlan(struct Plan *plan, PlanState *ps, ExplainState *es, EState *estate) {
	Assert(plan != NULL);
	ExplainEntry((struct Node*)plan, es, estate);
	ExplainEntry((struct Node*)ps, es, estate);
	AugmentPlan(plan, ps, es, estate);

	if (outerPlan(plan) != NULL || innerPlan(plan) != NULL) {
		ExplainOpenGroup("Plans", "Plans", false, es);
	}

	if (outerPlan(plan) != NULL) {
		Assert(outerPlanState(ps) != NULL);
		ExplainOpenGroup("left-child", NULL, true, es);
		WalkPlan(outerPlan(plan), outerPlanState(ps), es, estate);
		ExplainCloseGroup("left-child", NULL, true, es);
	}

	if (innerPlan(plan) != NULL) {
		Assert(innerPlanState(ps) != NULL);
		ExplainOpenGroup("right-child", NULL, true, es);
		WalkPlan(innerPlan(plan), innerPlanState(ps), es, estate);
		ExplainCloseGroup("right-child", NULL, true, es);
	}

	if (outerPlan(plan) != NULL || innerPlan(plan) != NULL) {
		ExplainCloseGroup("Plans", "Plans", false, es);
	}

	// TODO(Karthik): Handle sub-plans.
}

static StringInfo GetSerializedExplainOutput(QueryDesc* queryDesc) {
	// Create a fake ExplainState
	StringInfo ret;
	ExplainState* state = NewExplainState();
	state->analyze = true;
	state->format = EXPLAIN_FORMAT_TSCOUT;
	ExplainBeginOutput(state);

	ExplainOpenGroup("TscoutProps", NULL, true, state);
	ExplainOpenGroup("Tscout", "Tscout", true, state);
	WalkPlan(queryDesc->planstate->plan, queryDesc->planstate, state, queryDesc->estate);
	ExplainCloseGroup("Tscout", "Tscout", true, state);
	ExplainCloseGroup("TscoutProps", NULL, true, state);

	ExplainEndOutput(state);
	ret = state->str;
	pfree(state);
	return ret;
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
    values[idx++] = Int32GetDatum(LogicPid);
    ind_tup = index_form_tuple(index_relation->rd_att, values, is_nulls);

    elog(DEBUG1, "QCache prepare lookup: %" PRIu64, queryid);
    /* Insert new tuples to table and index if not found. */
    if (!IndexLookup(index_relation, ind_tup)) {
        HeapTuple heap_tup = NULL;
        TimestampTz stmt_start_ts;
        ItemPointer tid = NULL;
        elog(DEBUG1, "QCache prepare serialized: %" PRIu64, queryid);
        StringInfo serialized_plan = GetSerializedExplainOutput(query_desc);
        elog(DEBUG1, "QCache finished serialization: %" PRIu64, queryid);
        char *temp_buf = (char *)palloc(serialized_plan->len + 1);
        memcpy(temp_buf, serialized_plan->data, serialized_plan->len);
        temp_buf[serialized_plan->len] = '\0';

        elog(DEBUG1, "QCache inserting key qid: %" PRIu64, queryid);
        table_relation = table_open(table_oid, RowExclusiveLock);

        stmt_start_ts = GetCurrentStatementStartTimestamp();
        values[idx++] = Int64GetDatumFast(stmt_start_ts);
        values[idx++] = CStringGetTextDatum(temp_buf);

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
        pfree(temp_buf);
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

static void hutch_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
								   const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv) {
  PlannedStmt *plan;
  QueryDesc *queryDesc;
  instr_time plan_start, plan_duration;
  int eflags = 0;

  if (prev_ExplainOneQuery) {
	prev_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
  }

  // Postgres does not expose an interface to call into the standard ExplainOneQuery.
  // Hence, we duplicate the operations performed by the standard ExplainOneQuery i.e.,
  // calling into the standard planner.
  // A non-standard planner can be hooked in, in the the future.
  INSTR_TIME_SET_CURRENT(plan_start);
  plan = (planner_hook ? planner_hook(query, queryString, cursorOptions, params)
					   : standard_planner(query, queryString, cursorOptions, params));

  INSTR_TIME_SET_CURRENT(plan_duration);
  INSTR_TIME_SUBTRACT(plan_duration, plan_start);

  // We first run the standard explain code path. This is due to an adverse interaction
  // between hutch and HypoPG.
  //
  // HypoPG utilizes two hooks: (1) The ProcessUtility_hook is invoked at the beginning of a
  // utility command (e.g., EXPLAIN). The hook determines whether HypoPG is compatible
  // with the current utility command and sets a flag. (2) ExecutorEnd_hook is used
  // to clear the per-query state (it resets the flag set by the ProcessUtility_hook)
  //
  // However, in order for Hutch to generate the X's, we execute an ExecutorStart(),
  // extract all the X's from the resulting query plan & state, and invoke ExecutorEnd().
  //
  // Assuming we are unable/unwilling to clone HypoPG and patch this behavior, then
  // generating the X's will shutdown HypoPG for this query. This means that HypoPG will
  // no longer intercept any catalog inquiries about its hypothetical indexes.
  //
  // This "fix" assumes that we don't depend on HypoPG interception (e.g., catalog)
  // to generate the X's. This is because ExplainOnePlan() will also end up
  // invoking the ExecutorEnd_hook which shuts down HypoPG. As such, we first invoke
  // ExplainOnePlan() and then we generate the relevant X's.
  ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &plan_duration, NULL);

  if (es->format == EXPLAIN_FORMAT_TSCOUT) {
	queryDesc =
		CreateQueryDesc(plan, queryString, InvalidSnapshot, InvalidSnapshot, None_Receiver, params, queryEnv, 0);

	// If we don't do this, we can't get any useful information about the index keys
	// that are actually used to perform the index lookup.
	//
	// TODO(wz2): Note that this will actually break with hypothetical indexes. I think we will probably
	// have to bite the bullet at some point and fork hypopg if we continue to use that. Furthermore,
	// the current hypopg implementation cannot return modifications to insert/update indexes.
	eflags = 0;
	if (into) eflags |= GetIntoRelEFlags(into);

	// Run the executor.
	ExecutorStart(queryDesc, eflags);
	Assert(queryDesc->estate != NULL);
	// This calls into initPlan() which populates the plan tree.
	// TODO (Karthik): Create a hook to executor start.

	// Finally, walks through the plan, dumping the output of the plan in a separate top-level group.
	ExplainOpenGroup("TscoutProps", NULL, true, es);
	ExplainOpenGroup("Tscout", "Tscout", true, es);
	WalkPlan(queryDesc->planstate->plan, queryDesc->planstate, es, queryDesc->estate);
	ExplainCloseGroup("Tscout", "Tscout", true, es);
	ExplainCloseGroup("TscoutProps", NULL, true, es);

	// Free the created query description resources.
	NormalQuit(queryDesc);
	FreeQueryDesc(queryDesc);
  }
}
