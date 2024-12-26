file(GLOB_RECURSE BACKEND_SRC_FILES "*.cc")
add_library(flex_postgres_wal STATIC ${BACKEND_SRC_FILES})
target_link_libraries(flex_postgres_wal PUBLIC flex_postgres_backend)