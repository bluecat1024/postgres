#include "postgres.h"
#include "fmgr.h"
#include <inttypes.h>

#include "qss.h"
#include "cmudb/qss/qss.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

ExplainOneQuery_hook_type qss_prev_ExplainOneQuery = NULL;
ExplainOneUtility_hook_type qss_prev_ExplainOneUtility = NULL;
ExecutorEnd_hook_type qss_prev_ExecutorEnd = NULL;
ExecutorStart_hook_type qss_prev_ExecutorStart = NULL;
ProcessUtility_hook_type qss_prev_ProcessUtility = NULL;
get_relation_info_hook_type qss_prev_get_relation_info = NULL;

MemoryContext qss_MemoryContext = NULL;

void _PG_init(void) {
    elog(LOG, "QCache extension initialization.");

	qss_prev_ExecutorEnd = ExecutorEnd_hook;
	qss_prev_ExecutorStart = ExecutorStart_hook;
	qss_prev_ExplainOneQuery = ExplainOneQuery_hook;
	qss_prev_ExplainOneUtility = ExplainOneUtility_hook;
	qss_prev_ProcessUtility = ProcessUtility_hook;
	qss_prev_get_relation_info = get_relation_info_hook;

	qss_QSSAbort_hook = qss_Abort;
	qss_AllocInstrumentation_hook = qss_AllocInstrumentation;
	ExecutorEnd_hook = qss_ExecutorEnd;
	ExecutorStart_hook = qss_ExecutorStart;
	ExplainOneQuery_hook = qss_ExplainOneQuery;
	ExplainOneUtility_hook = qss_ExplainOneUtility;
	ProcessUtility_hook = qss_ProcessUtility;
	get_relation_info_hook = qss_GetRelationInfo;

	qss_MemoryContext = AllocSetContextCreate(TopMemoryContext,
					"QSS context",
					ALLOCSET_DEFAULT_MINSIZE,
					ALLOCSET_DEFAULT_INITSIZE,
					ALLOCSET_DEFAULT_MAXSIZE);

	RegisterXactCallback(qss_xact_callback, NULL);
}

void _PG_fini(void) {
	ExecutorEnd_hook = qss_prev_ExecutorEnd;
	ExecutorStart_hook = qss_prev_ExecutorStart;
	ExplainOneQuery_hook = qss_prev_ExplainOneQuery;
	ExplainOneUtility_hook = qss_prev_ExplainOneUtility;
	ProcessUtility_hook = qss_prev_ProcessUtility;
	get_relation_info_hook = qss_prev_get_relation_info;
	qss_AllocInstrumentation_hook = NULL;
	qss_QSSAbort_hook = NULL;

	UnregisterXactCallback(qss_xact_callback, NULL);
}
