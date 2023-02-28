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
#include "../../../../src/include/utils/builtins.h"
#include <unistd.h>
}

#ifndef STATE_H
#define STATE_H

static constexpr int kTextLength = 32;
static constexpr int kTypeInt = 1;
static constexpr int kTypeReal = 2;
static constexpr int kTypeStr = 3;
// Sorry Only those column OP constant can be handled.
static constexpr int kPgIntGreater = 521;
static constexpr int kPgIntLess = 97;
static constexpr int kPgIntGreaterThan = 525;
static constexpr int kPgIntLessThan = 523;
static constexpr int kPgIntEqual = 96;
static constexpr int kPgIntNotEqual = 518;
static constexpr int kPgRealGreater = 1123;
static constexpr int kPgRealLess = 1122;
static constexpr int kPgRealGreaterThan = 1125;
static constexpr int kPgRealLessThan = 1124;
static constexpr int kPgRealEqual = 1120;
static constexpr int kPgRealNotEqual = 1121;
static constexpr int kPgTextGreater = 666;
static constexpr int kPgTextLess = 664;
static constexpr int kPgTextGreaterThan = 667;
static constexpr int kPgTextLessThan = 665;
static constexpr int kPgTextEqual = 98;
static constexpr int kPgTextNotEqual = 531;

static constexpr int k721Greater = 0;
static constexpr int k721Less = 1;
static constexpr int k721GreaterThan = 2;
static constexpr int k721LessThan = 3;
static constexpr int k721Equal = 4;
static constexpr int k721NotEqual = 5;

int map_op_code(int pg_opno) {
    switch (pg_opno) {
        case kPgIntGreater:
        case kPgRealGreater:
        case kPgTextGreater:
            return k721Greater;
        case kPgIntLess:
        case kPgRealLess:
        case kPgTextLess:
            return k721Less;
        case kPgIntGreaterThan:
        case kPgRealGreaterThan:
        case kPgTextGreaterThan:
            return k721GreaterThan;
        case kPgIntLessThan:
        case kPgRealLessThan:
        case kPgTextLessThan:
            return k721LessThan;
        case kPgIntEqual:
        case kPgRealEqual:
        case kPgTextEqual:
            return k721Equal;
        case kPgIntNotEqual:
        case kPgRealNotEqual:
        case kPgTextNotEqual:
            return k721NotEqual;
        default:
            return -1;
    }
}

typedef struct PushedQual {
    int opcode;
    int constint;
    double constreal;
    char conststr[kTextLength];

    bool eval_pred_int(int val) {
        switch (this->opcode) {
            case k721Greater:
                return val > constint;
            case k721Less:
                return val < constint;
            case k721GreaterThan:
                return val >= constint;
            case k721LessThan:
                return val <= constint;
            case k721Equal:
                return val == constint;
            case k721NotEqual:
                return val != constint;
            default:
                return false;
        }
    }

    bool eval_pred_real(double val) {
        switch (this->opcode) {
            case k721Greater:
                return val > constreal;
            case k721Less:
                return val < constreal;
            case k721GreaterThan:
                return val >= constreal;
            case k721LessThan:
                return val <= constreal;
            case k721Equal:
                return val == constreal;
            case k721NotEqual:
                return val != constreal;
            default:
                return false;
        }
    }

    bool eval_pred_str(const char *val) {
        int ret_val = strcoll(val, conststr);
        switch (this->opcode) {
            case k721Greater:
                return ret_val > 0;
            case k721Less:
                return ret_val < 0;
            case k721GreaterThan:
                return ret_val >= 0;
            case k721LessThan:
                return ret_val <= 0;
            case k721Equal:
                return ret_val == 0;
            case k721NotEqual:
                return ret_val != 0;
            default:
                return false;
        }
    }
} PushedQual;

typedef struct ColumnMetaData {
    int begin_offset;
    int type;
    int width;
    bool *block_mask; // Calculated in first hook.
    List *pushed_quals;
} ColumnMetaData;

// Mapping of colname -> attrnum done in parsing.
typedef struct Db721PlanState {
    char *filename;
    Bitmapset *target_attr;
    Bitmapset *pred_attr;
    Bitmapset *nonpush_attr;
    List *target_attr_sorted;
    List *new_tlist;
    List *remain_quals;
    int rows;
    int natts;
    int block_size;
    List *column_metadata;
} Db721PlanState;

typedef struct ColumnExecData {
    int begin_offset;
    int type;
    int width;
    int fd;
    void *mat_buffer;
    bool *block_mask; // Calculated in first hook.
    List *pushed_quals; // Calculated in first hook.

    void InitScan(const char *filename, int block_size) {
        this->fd = open(filename, O_RDONLY);
        this->mat_buffer = palloc0(block_size * this->width);
        lseek(this->fd, this->begin_offset, SEEK_SET);
    }

    void ResetScan() {
        lseek(this->fd, this->begin_offset, SEEK_SET);
    }

    bool BlockValid(int block_idx) {
        return this->block_mask[block_idx];
    }

    void AdvanceBlock(int num_vals) {
        lseek(this->fd, width * num_vals, SEEK_CUR);
    }

    void EndScan() {
        close(this->fd);
    }
    // Only fetch when all blocks are not pruned.
    void FetchBlock(int num_vals) {
        read(this->fd, this->mat_buffer, width * num_vals);
    }

    bool ValValid(int curridx) {
        ListCell *lc;
        bool is_valid = true;
        switch (this->type) {
            case kTypeInt: {
                int val = *(int *)((char *)mat_buffer + curridx * width);
                foreach(lc, pushed_quals) {
                    PushedQual *qual = (PushedQual *)lfirst(lc);
                    if (!qual->eval_pred_int(val)) {
                        is_valid = false;
                        break;
                    }
                }
                break;
            }
            case kTypeReal: {
                float val = *(float *)((char *)mat_buffer + curridx * width);
                foreach(lc, pushed_quals) {
                    PushedQual *qual = (PushedQual *)lfirst(lc);
                    if (!qual->eval_pred_real(val)) {
                        is_valid = false;
                        break;
                    }
                }
                break;
            }
            case kTypeStr: {
                char *val = (char *)((char *)mat_buffer + curridx * width);
                foreach(lc, pushed_quals) {
                    PushedQual *qual = (PushedQual *)lfirst(lc);
                    if (!qual->eval_pred_str(val)) {
                        is_valid = false;
                        break;
                    }
                }
                break;
            }
        }

        return is_valid;
    }

    Datum GetCurrData(int curridx) {
        switch (this->type) {
            case kTypeInt:
                return Int32GetDatum(
                    *(int *)((char *)mat_buffer + curridx * width)
                );
            case kTypeReal:
                return Float4GetDatum(
                    *(float *)((char *)mat_buffer + curridx * width)
                );
            case kTypeStr:
                return CStringGetTextDatum(
                    (char *)((char *)mat_buffer + curridx * width)
                );
        }
    }
} ColumnExecData;

// Hacky: keep a full list of fd.
// But only move the column fds used.
typedef struct Db721ExecState {
    char *filename;
    List *target_attr_sorted;
    List *pred_attr_sorted;
    List *used_attr_sorted;
    int rows;
    int block_size;
    // Used to grab from current block.
    int curr_idx;
    // To check whether reaching the end.
    int global_curr_idx;
    ColumnExecData *column_execdata;

    void InitScan() {
        ListCell *lc;
        curr_idx = 0;
        global_curr_idx = 0;
        foreach(lc, used_attr_sorted) {
            int attr = lfirst_oid(lc);
            // elog(LOG, "Init scan for column %d", attr);
            this->column_execdata[attr].InitScan(filename, block_size); 
        }
        // Place to the first valid block.
        this->AdvanceBlock();
    }

    void AdvanceBlock() {
        this->curr_idx = 0;
        ListCell *lc;
        while (global_curr_idx < rows) {
            bool block_valid = true;
            foreach(lc, this->pred_attr_sorted) {
                int attr = lfirst_oid(lc);
                if (!column_execdata[attr].BlockValid(global_curr_idx
                    / block_size)) {
                    block_valid = false;
                    break;
                }
            }

            if (block_valid) {
                foreach(lc, used_attr_sorted) {
                    int attr = lfirst_oid(lc);
                    int num_next_blk = (block_size <= rows - this->global_curr_idx) ?
                        block_size : rows - this->global_curr_idx;
                    // elog(LOG, "Fetch block %d num %d", attr, num_next_blk);
                    this->column_execdata[attr].FetchBlock(
                        num_next_blk
                    );
                }
                break;
            } else {
                this->global_curr_idx += block_size;
                foreach(lc, used_attr_sorted) {
                    int attr = lfirst_oid(lc);
                    this->column_execdata[attr].AdvanceBlock(
                        block_size
                    ); 
                }       
            }
        }
    }

    void ResetScan() {
        ListCell *lc;
        this->curr_idx = 0;
        this->global_curr_idx = 0;
        foreach(lc, used_attr_sorted) {
            int attr = lfirst_oid(lc);
            this->column_execdata[attr].ResetScan(); 
        }
        // Place to the first valid block.
        this->AdvanceBlock();
    }

    void EndScan() {
        ListCell *lc;
        foreach(lc, used_attr_sorted) {
            int attr = lfirst_oid(lc);
            this->column_execdata[attr].EndScan(); 
        }
    }

    TupleTableSlot *IterateScan(TupleTableSlot *slot) {
        ListCell *lc;
        while (true) {
            bool tuple_valid = true;
            if (this->global_curr_idx >= this->rows) {
                return NULL;
            }

            foreach(lc, this->pred_attr_sorted) {
                int attr = lfirst_int(lc);
                if (!column_execdata[attr].ValValid(curr_idx)) {
                    tuple_valid = false;
                    break;
                }
            }

            if (tuple_valid) {
                // Fill in tuple slot.
                // elog(LOG, "Filling in slot");
                TupleDesc tupledesc = slot->tts_tupleDescriptor;
                for (int attidx = 0; attidx < tupledesc->natts; ++attidx) {
                    int curr_attr = list_nth_int(target_attr_sorted, attidx);
                    slot->tts_values[attidx] = column_execdata[curr_attr].GetCurrData(
                        this->curr_idx
                    );
                }
                ExecStoreVirtualTuple(slot);

                ++this->global_curr_idx;
                if (++this->curr_idx >= block_size) {
                    this->AdvanceBlock();
                }
                break;
            }
            ++this->global_curr_idx;
            if (++this->curr_idx >= block_size) {
                this->AdvanceBlock();
            }
        }
        return slot;
    }
} Db721ExecState;

#endif