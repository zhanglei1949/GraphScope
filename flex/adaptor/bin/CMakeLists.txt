add_executable(flex_adaptor main.c postmaster.c)

target_link_libraries(flex_adaptor PUBLIC flex_postgres_backend ${POSTGRESS_PORT_LIB_PATH} ${POSTGRESS_COMMON_LIB_PATH} ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT} ZLIB::ZLIB)
target_link_libraries(flex_adaptor PUBLIC m)
# add compiler option -rdynamic -pedantic -O0 -g
target_compile_options(flex_adaptor PUBLIC -rdynamic -pedantic -O0 -g)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../third_party/postgres/src/interfaces/libpq)

add_executable(flex_initdb "initdb.c" "findtimezone.c" "localtime.c")
#add -lm for flex_initdb

target_link_libraries(flex_initdb PUBLIC ${POSTGRESS_LIB_FE_UTILS_FULL_PATH} ${POSTGRESS_COMMON_FULL_LIB_PATH} ${POSTGRESS_PORT_FULL_LIB_PATH}  ${POSTGRESS_LIB_PQ_PATH})
target_link_libraries(flex_initdb PUBLIC m)