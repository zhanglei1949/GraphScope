file(GLOB NODE_SRC_FILES "*.c")
# compile all .c into a .o


add_library(flex_postgres_nodes STATIC "copyfuncs.c" "equalfuncs.c" "outfuncs.c" "queryjumblefuncs.c" "readfuncs.c" "bitmapset.c" "value.c" "makefuncs.c" "list.c" "read.c" "nodeFuncs.c" "extensible.c" "tidbitmap.c" "multibitmapset.c" "params.c" "print.c")
