add_subdirectory(nodes)
add_subdirectory(regex)
add_subdirectory(port)
add_subdirectory(libpq)
add_subdirectory(rewrite)
#Add all files in the directory to compile a dynamic library
file(GLOB_RECURSE BACKEND_SRC_FILES "**/*.c" "**/*.cc")
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

# Remove lauch_backend.c
# list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "postmaster/launch_backend.c")
# remove flex_process.cc
# list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "postmaster/flex_process.cc")
# remove flex_process_init.c
# list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "postmaster/flex_process_init.c")
# remove post_master_main.c
list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "postmaster/post_master_main.c")
# filter postgres.c
#list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "postmaster/postgres.c")
# filter postmaster.c
#list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "postmaster/postmaster.c")

list(FILTER BACKEND_SRC_FILES EXCLUDE REGEX "wait_event_funcs_data.c")

# append utils/mmgr/portalmem.c
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/mmgr/portalmem.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/adt/regexp.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/adt/pg_upgrade_support.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/jit/jit.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/sort/sortsupport.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/utils/adt/like_support.c)
list(APPEND BACKEND_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/commands/portalcmds.c)


include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/postgres/src/interfaces/libpq)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../engines/http_server/)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../utils/)

add_library(flex_postgres_backend STATIC ${BACKEND_SRC_FILES})
target_link_libraries(flex_postgres_backend PUBLIC flex_postgres_timezone flex_postgres_nodes
    flex_postgres_rewrite flex_postgres_libpq flex_postgres_port  flex_postgres_regex ${POSTGRESS_PORT_LIB_PATH} ${POSTGRESS_COMMON_LIB_PATH} flex_server)


add_executable(flex_postmaster ${CMAKE_CURRENT_SOURCE_DIR}/postmaster/post_master_main.c ${CMAKE_CURRENT_SOURCE_DIR}/postmaster/flex_process_init.c
    ${CMAKE_CURRENT_SOURCE_DIR}/postmaster/flex_process_init.c) #${CMAKE_CURRENT_SOURCE_DIR}/postmaster/launch_backend.c 
#${CMAKE_CURRENT_SOURCE_DIR}/postmaster/flex_process.cc
target_link_libraries(flex_postmaster PUBLIC flex_postgres_backend  flex_postgres_wal)


#flex_server flex_graph_db

#flex_postgres_utils_sort flex_postgres_utils_mmgr flex_postgres_init flex_postgres_hash flex_postgres_fmgr flex_postgres_error flex_postgres_cache flex_postgres_activity flex_postgres_adt