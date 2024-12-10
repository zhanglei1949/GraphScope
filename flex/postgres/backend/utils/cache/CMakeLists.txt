# OBJS = \
# 	attoptcache.o \
# 	catcache.o \
# 	evtcache.o \
# 	inval.o \
# 	lsyscache.o \
# 	partcache.o \
# 	plancache.o \
# 	relcache.o \
# 	relfilenumbermap.o \
# 	relmapper.o \
# 	spccache.o \
# 	syscache.o \
# 	ts_cache.o \
# 	typcache.o

add_library(flex_postgres_cache STATIC "attoptcache.c" "catcache.c" "evtcache.c" "inval.c" "lsyscache.c" "partcache.c" "plancache.c" "relcache.c" "relfilenumbermap.c" "relmapper.c" "spccache.c" "syscache.c" "ts_cache.c" "typcache.c")