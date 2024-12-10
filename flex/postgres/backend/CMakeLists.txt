add_subdirectory(nodes)
add_subdirectory(regex)
add_subdirectory(port)
add_subdirectory(libpq)
add_subdirectory(rewrite)
add_subdirectory(utils)
#Add all files in the directory to compile a dynamic library
file(GLOB_RECURSE BACKEND_SRC_FILES "**/*.c")
# Remove files under nodes directory
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "nodes/*")
# Remove files whose name contains win32, like port/win32_shmem.c
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX ".*win32.*")
# Filter out regex files
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "regex/*")
# FIlter out port files
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "port/*")
# Filter out jit files
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "jit/*")
# exclude like_match.c
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "like_match.c")
# exclude libpq
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "libpq/*")

# append utils/mmgr/portalmem.c
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/mmgr/portalmem.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/adt/regexp.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/adt/pg_upgrade_support.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/jit/jit.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/sort/sortsupport.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/adt/like_support.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/commands/portalcmds.c)


include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/postgres/src/interfaces/libpq)

add_library(flex_postgres_backend STATIC ${BACKEND_SRC_FILES})
target_link_libraries(flex_postgres_backend flex_postgres_nodes
    flex_postgres_rewrite flex_postgres_libpq flex_postgres_port  flex_postgres_regex ${POSTGRESS_PORT_LIB_PATH} ${POSTGRESS_COMMON_LIB_PATH})

#flex_postgres_utils_sort flex_postgres_utils_mmgr flex_postgres_init flex_postgres_hash flex_postgres_fmgr flex_postgres_error flex_postgres_cache flex_postgres_activity flex_postgres_adt