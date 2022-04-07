EXTENSION = qcache
MODULE_big = qcache
DATA = qcache--0.0.1.sql
OBJS = qcache.o
# If PG_CONFIG is not set, try the default NoisePage build folder.
PG_CONFIG ?= ../../../build/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)