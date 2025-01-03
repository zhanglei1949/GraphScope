file(GLOB_RECURSE TIMEZONE_SRC_FILES "**/*.c" "*.c")
add_library(flex_postgres_timezone STATIC ${TIMEZONE_SRC_FILES})