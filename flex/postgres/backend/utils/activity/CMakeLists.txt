# OBJS = \
# 	backend_progress.o \
# 	backend_status.o \
# 	pgstat.o \
# 	pgstat_archiver.o \
# 	pgstat_bgwriter.o \
# 	pgstat_checkpointer.o \
# 	pgstat_database.o \
# 	pgstat_function.o \
# 	pgstat_io.o \
# 	pgstat_relation.o \
# 	pgstat_replslot.o \
# 	pgstat_shmem.o \
# 	pgstat_slru.o \
# 	pgstat_subscription.o \
# 	pgstat_wal.o \
# 	pgstat_xact.o \
# 	wait_event.o \
# 	wait_event_funcs.o

add_library(flex_postgres_activity STATIC "backend_progress.c" "backend_status.c" "pgstat.c" "pgstat_archiver.c" "pgstat_bgwriter.c" "pgstat_checkpointer.c" "pgstat_database.c" "pgstat_function.c" "pgstat_io.c" "pgstat_relation.c" "pgstat_replslot.c" "pgstat_shmem.c" "pgstat_slru.c" "pgstat_subscription.c" "pgstat_wal.c" "pgstat_xact.c" "wait_event.c" "wait_event_funcs.c")