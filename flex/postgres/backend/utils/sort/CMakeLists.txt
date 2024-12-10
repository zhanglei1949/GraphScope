# OBJS = \
# 	logtape.o \
# 	qsort_interruptible.o \
# 	sharedtuplestore.o \
# 	sortsupport.o \
# 	tuplesort.o \
# 	tuplesortvariants.o \
# 	tuplestore.o

add_library(flex_postgres_utils_sort STATIC "logtape.c" "qsort_interruptible.c" "sharedtuplestore.c" "sortsupport.c" "tuplesort.c" "tuplesortvariants.c" "tuplestore.c")