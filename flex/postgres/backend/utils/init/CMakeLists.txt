# OBJS = \
# 	globals.o \
# 	miscinit.o \
# 	postinit.o \
# 	usercontext.o

add_library(flex_postgres_init STATIC "globals.c" "miscinit.c" "postinit.c" "usercontext.c")