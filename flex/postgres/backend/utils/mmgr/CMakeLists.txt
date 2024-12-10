# OBJS = \
# 	alignedalloc.o \
# 	aset.o \
# 	bump.o \
# 	dsa.o \
# 	freepage.o \
# 	generation.o \
# 	mcxt.o \
# 	memdebug.o \
# 	portalmem.o \
# 	slab.o

add_library(flex_postgres_utils_mmgr STATIC "alignedalloc.c" "aset.c" "bump.c" "dsa.c" "freepage.c" "generation.c" "mcxt.c" "memdebug.c" "portalmem.c" "slab.c")