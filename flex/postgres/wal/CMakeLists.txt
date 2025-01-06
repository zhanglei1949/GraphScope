file(GLOB_RECURSE WAL_SRC_FILES "*.cc")
# exclude pg_wal_utils.cc
list(FILTER WAL_SRC_FILES EXCLUDE REGEX "pg_wal_utils.cc")
add_library(flex_postgres_wal STATIC ${WAL_SRC_FILES})
target_link_libraries(flex_postgres_wal PUBLIC flex_postgres_backend)