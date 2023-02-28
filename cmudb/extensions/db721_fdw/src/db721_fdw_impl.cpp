// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "state.h"
#include "parser.h"
#include <set>

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/optimizer/optimizer.h"
#include "../../../../src/include/optimizer/pathnode.h"
#include "../../../../src/include/optimizer/paths.h"
#include "../../../../src/include/optimizer/planmain.h"
#include "../../../../src/include/optimizer/plancat.h"
#include "../../../../src/include/optimizer/restrictinfo.h"
#include "../../../../src/include/optimizer/tlist.h"
}
// clang-format on

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {
  // TODO(721): Write me!
  // Dog terrier("Terrier");
  // elog(LOG, "db721_GetForeignRelSize: %s", terrier.Bark().c_str());
  setlocale(LC_COLLATE, "en_US.UTF-8");
  baserel->fdw_private = CreatePlanState(root, baserel, foreigntableid);
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  // TODO(721): Write me!
  // Dog scout("Scout");
  // elog(LOG, "db721_GetForeignPaths: %s", scout.Bark().c_str());

  Cost startup = baserel->baserestrictcost.startup;
  Cost totalcost = startup + baserel->rows * cpu_tuple_cost;
  add_path(baserel, (Path *)create_foreignscan_path(
    root,
    baserel,
    NULL,
    baserel->rows,
    startup,
    totalcost,
    NULL,
    NULL,
    NULL,
    (List *)baserel->fdw_private
  ));
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {
  // TODO(721): Write me!
  // Dog scout("Scout");
  // elog(LOG, "db721_GetForeignPlan: %s", scout.Bark().c_str());

  // Attno must be preserved from tlist to enable projection.
  Db721ExecState *exec_state = (Db721ExecState *)palloc0(sizeof(Db721ExecState));
  Db721PlanState *fdw_state = (Db721PlanState *)baserel->fdw_private;
  exec_state->filename = fdw_state->filename;
  exec_state->rows = fdw_state->rows;
  exec_state->block_size = fdw_state->block_size;
  exec_state->early_out = fdw_state->early_out;
  // Move forward the target list.
  exec_state->target_attr_sorted = fdw_state->target_attr_sorted;
  std::set<int> used_attr;
  int attr = -1;
  while ((attr = bms_next_member(fdw_state->target_attr, attr)) >= 0) {
    // elog(LOG, "Exec pred attr %d", attr);
    used_attr.insert(attr - 8);
  }
  attr = -1;
  while ((attr = bms_next_member(fdw_state->pred_attr, attr)) >= 0) {
    // elog(LOG, "Exec pred attr %d", attr);
    used_attr.insert(attr - 8);
    exec_state->pred_attr_sorted =
      lappend_int(exec_state->pred_attr_sorted, attr - 8);
  }
  for (int attr : used_attr) {
    // elog(LOG, "Exec used attr %d", attr);
    exec_state->used_attr_sorted =
      lappend_int(exec_state->used_attr_sorted, attr);
  }

  ListCell *lc;
  int i = 0;
  exec_state->column_execdata = (ColumnExecData *)palloc0(fdw_state->natts
    * sizeof(ColumnExecData));
  foreach(lc, fdw_state->column_metadata) {
    if (used_attr.find(i) == used_attr.end()) {
      ++i;
      continue;
    }
    ColumnMetaData *colmeta = (ColumnMetaData *)lfirst(lc);
    exec_state->column_execdata[i].block_mask = colmeta->block_mask;
    exec_state->column_execdata[i].type = colmeta->type;
    exec_state->column_execdata[i].width = colmeta->width;
    exec_state->column_execdata[i].pushed_quals = colmeta->pushed_quals;
    exec_state->column_execdata[i].begin_offset = colmeta->begin_offset;
    ++i;
  }
  
  return make_foreignscan(
    fdw_state->new_tlist,
    fdw_state->remain_quals,
    baserel->relid,
    NULL,
    (List *)exec_state,
    fdw_state->new_tlist,
    NULL,
    outer_plan
  );
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  // TODO(721): Write me!
  // elog(LOG, "db721_BeginForeignScan");
  Db721ExecState *exec_state = ((Db721ExecState *)((ForeignScan *)node
    ->ss.ps.plan)->fdw_private);
  node->fdw_state = exec_state;
  if (exec_state->early_out) {
    return;
  }
  // Corner case: set slot to empty if tuple width 0.
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  if (exec_state->target_attr_sorted == NIL) {
    slot->tts_tupleDescriptor->natts = 0;
  }
  exec_state->InitScan();
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  // elog(LOG, "db721_IterateForeignScan");

  Db721ExecState *exec_state = (Db721ExecState *)node->fdw_state;
  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  if (exec_state->early_out) {
    return NULL;
  }

  ExecClearTuple(slot);
  TupleTableSlot *ret_slot = exec_state->IterateScan(slot);
  // elog(LOG, "slot ptr %ld", (long long)ret_slot);
  return ret_slot;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  Db721ExecState *exec_state = (Db721ExecState *)node->fdw_state;
  if (exec_state->early_out) {
    return;
  }
  exec_state->ResetScan();
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
  // Release all file descriptors.
  Db721ExecState *exec_state = (Db721ExecState *)node->fdw_state;
  if (exec_state->early_out) {
    return;
  }
  exec_state->EndScan();
}