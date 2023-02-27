#include "rapidjson/document.h"
#include "state.h"
#include <fstream>
#include <memory>
#include <algorithm>
#include <unordered_set>

extern "C" {
    #include "../../../../src/include/postgres.h"
    #include "../../../../src/include/fmgr.h"
    #include "../../../../src/include/foreign/fdwapi.h"
    #include "../../../../src/include/access/table.h"
    #include "../../../../src/include/nodes/makefuncs.h"
    #include "../../../../src/include/optimizer/optimizer.h"
    #include "../../../../src/include/optimizer/pathnode.h"
    #include "../../../../src/include/optimizer/paths.h"
    #include "../../../../src/include/optimizer/planmain.h"
    #include "../../../../src/include/optimizer/plancat.h"
    #include "../../../../src/include/optimizer/restrictinfo.h"
    #include "../../../../src/include/optimizer/tlist.h"
    #include "../../../../src/include/foreign/foreign.h"
    #include "../../../../src/include/commands/defrem.h"
    #include "../../../../src/include/catalog/pg_am_d.h"
    #include "../../../../src/include/utils/builtins.h"
    #include "../../../../src/include/utils/lsyscache.h"
    #include "../../../../src/include/utils/rel.h"
}

static constexpr int kSizeMetaLength = 4;

rapidjson::Document GetMetadata(const char *filename) {
    int meta_length = 0;
    std::ifstream db721_file(filename, std::ios::in | std::ios::binary);
    // elog(LOG, "Is open: %d", db721_file.is_open());
    db721_file.seekg(0, db721_file.end);
    int flen = db721_file.tellg();
    db721_file.seekg(flen - kSizeMetaLength);
    db721_file.read((char *)(&meta_length), kSizeMetaLength);
    db721_file.seekg(flen - kSizeMetaLength - meta_length);
    // elog(LOG, "json len %d", meta_length);
    std::string json_meta;
    json_meta.resize(meta_length);
    db721_file.read(&(json_meta[0]), meta_length);
    // elog(LOG, "read json string %s", json_meta.c_str());
    auto doc = rapidjson::Document();
    doc.Parse(json_meta.c_str(), meta_length);
    db721_file.close();
    return doc;
}

void GetTabOpt(Oid relid, Db721PlanState *fdw_state) {
    ForeignTable *table;
    ListCell     *lc;
    table = GetForeignTable(relid);

    foreach(lc, table->options) {
        DefElem    *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, "filename") == 0) {
            fdw_state->filename = defGetString(def);
            elog(LOG, "filename, %s", defGetString(def));
        }
    }
}

List *MakeStatQual(
    Var *v,
    rapidjson::Value &stat,
    const std::string &type) {
    OpExpr *maxe;
    OpExpr *mine;
    if (type == "float") {
        double maxv = stat["max"].GetDouble();
        double minv = stat["min"].GetDouble();
        
        Const *max_c = makeConst(v->vartype + 1, v->vartypmod,
            v->varcollid, sizeof(double), Float8GetDatum(maxv),
            0, 1);
        Const *min_c = makeConst(v->vartype + 1, v->vartypmod,
            v->varcollid, sizeof(double), Float8GetDatum(minv),
            0, 1);

        maxe = (OpExpr *) make_opclause(kPgRealLessThan,
			InvalidOid, false, (Expr *) v, (Expr *) max_c,
			InvalidOid, v->varcollid);
        mine = (OpExpr *) make_opclause(kPgRealGreaterThan,
			InvalidOid, false, (Expr *) v, (Expr *) min_c,
			InvalidOid, v->varcollid);
    } else if (type == "int") {
        double maxv = stat["max"].GetInt();
        double minv = stat["min"].GetInt();
        
        Const *max_c = makeConst(v->vartype, v->vartypmod,
            v->varcollid, sizeof(int), Int32GetDatum(maxv),
            0, 1);
        Const *min_c = makeConst(v->vartype, v->vartypmod,
            v->varcollid, sizeof(int), Int32GetDatum(minv),
            0, 1);

        maxe = (OpExpr *) make_opclause(kPgIntLessThan,
			InvalidOid, false, (Expr *) v, (Expr *) max_c,
			InvalidOid, v->varcollid);
        mine = (OpExpr *) make_opclause(kPgIntGreaterThan,
			InvalidOid, false, (Expr *) v, (Expr *) min_c,
			InvalidOid, v->varcollid);
    } else {
        elog(LOG, "attno: %d", v->varattno);
        const char *maxv = stat["max"].GetString();
        const char *minv = stat["min"].GetString();
        elog(LOG, "fetching max min %s %s", maxv, minv);
        Const *max_c = makeConst(25, -1,
            100, -1, CStringGetTextDatum(maxv),
            0, 0);
        Const *min_c = makeConst(25, -1,
            100, -1, CStringGetTextDatum(minv),
            0, 0);
        // elog(LOG, "Const built");

        // For string, there is one layer of type cast.
        elog(LOG, "make: left var: %d %d %d %d %d %d %d %d %d", v->varattno,
            v->varcollid, v->vartype, v->vartypmod,
            v->location, v->varattnosyn, v->varlevelsup,
            v->varno, v->varnosyn);
        elog(LOG, "make: const: %d %d %d %d %d %d %d",
            max_c->constbyval, max_c->constcollid,
            max_c->constisnull, max_c->constlen,
            max_c->consttype, max_c->consttypmod,
            max_c->location);
        RelabelType *cast_v = makeRelabelType((Expr *)v, 25,
            -1, 100, COERCE_IMPLICIT_CAST);
        elog(LOG, "make : Relabel: %d %d %d %d", cast_v->relabelformat,
            cast_v->resultcollid, cast_v->resulttype,
            cast_v->resulttypmod);

        // elog(LOG, "Var built");

        maxe = (OpExpr *) make_opclause(kPgTextLessThan,
			InvalidOid, false, (Expr *) cast_v, (Expr *) max_c,
			InvalidOid, cast_v->resultcollid);
        mine = (OpExpr *) make_opclause(kPgTextGreaterThan,
			InvalidOid, false, (Expr *) cast_v, (Expr *) min_c,
			InvalidOid, cast_v->resultcollid);
    }

    Node *and_qual = make_and_qual((Node *)maxe, (Node *)mine);

    return list_make1(and_qual);
}

bool PushDownQual(Expr *clause,
    Db721PlanState *fdw_state) {
    if (!IsA(clause, OpExpr)) {
        return false;
    }

    OpExpr *expr = (OpExpr *) clause;
    if (list_length(expr->args) != 2) {
        return false;
    }
    Expr *lf = (Expr *) linitial(expr->args);
    Expr *rf = (Expr *) lsecond(expr->args);
    if (expr->opno == kPgTextEqual) {
        elog(LOG, "left nodetype: %d, right nodetype: %d",
            nodeTag((Node *)lf), nodeTag((Node *)rf));
        elog(LOG, "left child nodetype: %d",
            nodeTag((Node *)(((RelabelType *)lf)->arg)));
        elog(LOG, "Relabel %d %d %d %d %d", ((RelabelType *)lf)->resulttype,
            ((RelabelType *)lf)->resulttypmod, ((RelabelType *)lf)->resultcollid,
            ((RelabelType *)lf)->relabelformat);
        Var *v = (Var *)((RelabelType *)lf)->arg;
        elog(LOG, "left var: %d %d %d %d %d %d %d %d %d", v->varattno,
            v->varcollid, v->vartype, v->vartypmod,
            v->location, v->varattnosyn, v->varlevelsup,
            v->varno, v->varnosyn);
        Const *max_c = (Const *)rf;
        elog(LOG, "const: %d %d %d %d %d %d %d",
            max_c->constbyval, max_c->constcollid,
            max_c->constisnull, max_c->constlen,
            max_c->consttype, max_c->consttypmod,
            max_c->location);
        RelabelType *relab = (RelabelType *)lf;
        elog(LOG, "Relabel: %d %d %d %d", relab->relabelformat,
            relab->resultcollid, relab->resulttype,
            relab->resulttypmod);
        // elog(LOG, "left is V: %d, left is O: %d, right is C: %d, right is E-V: %d",
        // IsA(lf, Var), IsA(lf, OpExpr),
        // IsA(rf, Const), IsA(rf, OpExpr) && IsA(linitial(((OpExpr *)rf)->args), Const));
    }
    bool lisvar = (IsA(lf, Var) || (IsA(lf, RelabelType)
        && IsA(((RelabelType *)lf)->arg, Var)));
    bool risvar = (IsA(rf, Var) || (IsA(rf, RelabelType)
        && IsA(((RelabelType *)rf)->arg, Var)));
    if (!((IsA(lf, Const) && risvar)
        || (IsA(rf, Const) && lisvar))) {
        return false;
    }

    Var *v = (Var *)lf;
    Const *c = (Const *)rf;
    int opno = expr->opno;

    if (IsA(lf, Const)) {
        v = (Var *)rf;
        c = (Const *)lf;
        opno = get_commutator(opno);
    }

    if (IsA(v, RelabelType)) {
        v = (Var *)((RelabelType *)v)->arg;
    }
    elog(LOG, "Mapping opcode %d", opno);

    PushedQual *pq = (PushedQual *)palloc0(sizeof(PushedQual));
    pq->opcode = map_op_code(opno);
    if (pq->opcode < 0) {
        return false;
    }

    elog(LOG, "Fetch col %d", v->varattno - 1);
    elog(LOG, "list addr %ld", (long long)fdw_state->column_metadata);
    ColumnMetaData *colmeta = (ColumnMetaData *)list_nth(
        fdw_state->column_metadata, v->varattno - 1);
    elog(LOG, "Fetched col %d", v->varattno - 1);
    int type = colmeta->type;
    if (type == kTypeReal) {
        // elog(LOG, "Get double const");
        pq->constreal = DatumGetFloat8(c->constvalue);
        // elog(LOG, "Get double const %lf", pq->constreal);
    } else if (type == kTypeInt) {
        pq->constint = DatumGetInt32(c->constvalue);
    } else {
        strcpy(pq->conststr, TextDatumGetCString(c->constvalue));
        elog(LOG, "Str const: %s", pq->conststr);
    }
    elog(LOG, "Push complete.", v->varattno - 1);
    colmeta->pushed_quals = lappend(colmeta->pushed_quals, pq);

    // bms_add_member(fdw_state->pred_attr, v->varattno);

    return true;
}

void FetchColumnMeta(TupleDesc tupledesc,
    rapidjson::Document& doc,
    List *quals,
    PlannerInfo *root,
    RelOptInfo *baserel,
    Db721PlanState *fdw_state) {
    elog(LOG, "Parse step 1");
    fdw_state->block_size =
        doc["Max Values Per Block"].GetInt();
    // Create columns based on order.
    auto &column_meta = doc["Columns"];
    elog(LOG, "Parse step 2");

    // Pull pred attrs into pred_attr.
    ListCell *lc;
    foreach(lc, quals) {
        pull_varattnos((Node *)(lfirst(lc)), baserel->relid,
            &fdw_state->pred_attr);
    }
    // Only visit column meta for used columns.
    std::unordered_set<int> used_attrs_set;
    std::unordered_set<int> pred_attrs_set;
    int attr = -1;
    while ((attr = bms_next_member(fdw_state->pred_attr, attr)) >= 0) {
        pred_attrs_set.insert(attr - 8);
        used_attrs_set.insert(attr - 8);
    }
    attr = -1;
    while ((attr = bms_next_member(fdw_state->target_attr, attr)) >= 0) {
        used_attrs_set.insert(attr - 8);
    }

    List *tlist = build_physical_tlist(root, baserel);

    fdw_state->natts = tupledesc->natts;
    for (int i = 0; i < tupledesc->natts; ++i) {
        // Skip when column not used.
        if (used_attrs_set.find(i) == used_attrs_set.end()) {
            fdw_state->column_metadata = lappend(
                fdw_state->column_metadata, NULL
            );
            continue;
        }

        char *colname = NameStr(TupleDescAttr(tupledesc, i)->attname);
        int idx = 0;
        while (colname[idx] != '\0') {
            colname[idx] = tolower(colname[idx]);
            ++idx;
        }

        auto &cb_data = column_meta[colname];
        int nblock = cb_data["num_blocks"].GetInt();
        elog(LOG, "Parse step 3");
        ColumnMetaData *fdw_column =
            (ColumnMetaData *)palloc0(sizeof(ColumnMetaData));
        fdw_column->block_mask = (bool *)palloc0(sizeof(bool) * nblock);
        fdw_column->begin_offset = cb_data["start_offset"].GetInt();
        std::string dtype = cb_data["type"].GetString();
        elog(LOG, "Parse step 4");
        // Only int, float and str.
        if (dtype == "float") {
            fdw_column->type = kTypeReal;
            fdw_column->width = sizeof(float);
        } else if (dtype == "int") {
            fdw_column->type = kTypeInt;
            fdw_column->width = sizeof(int);
        } else {
            fdw_column->type = kTypeStr;
            fdw_column->width = kTextLength;
        }

        // Step 2: build >= min and <= max quals,
        // Refute the ones and rule out by the mask.
        auto &block_stats = cb_data["block_stats"];
        int total_rows = 0;
        Var *v = (Var *)((TargetEntry *)list_nth(tlist, i))->expr;
        elog(LOG, "attno in parser: %d %d %d %d", v->varattno,
            v->vartype, v->vartypmod, v->varcollid);
        elog(LOG, "Parse step 5");
        for (int block_i = 0; block_i < nblock; ++block_i) {
            auto &stat = block_stats[std::to_string(block_i).c_str()];
            total_rows += stat["num"].GetInt();
            // Skip block pruning if not in predicates.
            if (pred_attrs_set.find(i) == pred_attrs_set.end()) {
                continue;
            }
            elog(LOG, "column %s making maxmin qual", colname); 
            List *stat_qual = MakeStatQual(v, stat, dtype);
            elog(LOG, "column %s refuting qual", colname); 
            if (!predicate_refuted_by(stat_qual, quals, false)) {
                elog(LOG, "Column %s block %d passes", colname, block_i);
                fdw_column->block_mask[block_i] = true;
            }
            elog(LOG, "column %s refuted qual done, moving to next", colname);
        }

        fdw_state->rows = total_rows;
        fdw_state->column_metadata =
            lappend(fdw_state->column_metadata, fdw_column);
    }

    // Now do the push down per qual.
    // Not pushable ones go to remain_quals.
    elog(LOG, "Parse step 6");
    foreach(lc, quals) {
        Expr *clause = (Expr *)lfirst(lc);
        if (!PushDownQual(clause, fdw_state)) {
            fdw_state->remain_quals =
                lappend(fdw_state->remain_quals, clause);
        }
        elog(LOG, "Parse step 7");
    }
}

Db721PlanState *CreatePlanState(PlannerInfo *root, RelOptInfo *baserel, Oid relid) {
    Db721PlanState *fdw_state = (Db721PlanState *)palloc0(sizeof(Db721PlanState));
    // Filename.
    GetTabOpt(relid, fdw_state);
    // Target attrs.
    pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid,
        &fdw_state->target_attr);

    RangeTblEntry *rte = root->simple_rte_array[baserel->relid];
    Relation rel = table_open(rte->relid, AccessShareLock);
    TupleDesc tupledesc = RelationGetDescr(rel);

    // Get column data, handle predicate pushdown.
    List *quals = extract_actual_clauses(baserel->baserestrictinfo, false);
    auto doc = GetMetadata(fdw_state->filename);
    FetchColumnMeta(tupledesc, doc,
        quals, root, baserel, fdw_state);
    baserel->rows = fdw_state->rows;
    baserel->tuples = fdw_state->rows;

    table_close(rel, AccessShareLock);

    return fdw_state;
}