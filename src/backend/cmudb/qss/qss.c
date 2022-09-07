#include "cmudb/qss/qss.h"

bool qss_capture_enabled = false;
bool qss_capture_exec_stats = false;
bool qss_capture_nested = false;
bool qss_output_noisepage = false;
bool qss_capture_abort = false;

qss_AllocInstrumentation_type qss_AllocInstrumentation_hook = NULL;
qss_QSSAbort_type qss_QSSAbort_hook = NULL;
Instrumentation* ActiveQSSInstrumentation = NULL;

Instrumentation* AllocQSSInstrumentation(EState* estate, const char *ou) {
	if (qss_capture_enabled && qss_capture_exec_stats && qss_output_noisepage && qss_AllocInstrumentation_hook) {
		return qss_AllocInstrumentation_hook(estate, ou);
	}

	return NULL;
}

void QSSAbort() {
	if (qss_QSSAbort_hook) {
		qss_QSSAbort_hook();
	}

	ActiveQSSInstrumentation = NULL;
}
