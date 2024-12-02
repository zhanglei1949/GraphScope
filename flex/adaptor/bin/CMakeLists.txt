add_executable(flex_adaptor postmaster.c main.c)

target_link_libraries(flex_adaptor PUBLIC ${POSTGRESS_LIB_PATH} ${POSTGRESS_PORT_LIB_PATH} ${POSTGRESS_COMMON_LIB_PATH} ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT} ZLIB::ZLIB)
target_link_libraries(flex_adaptor PUBLIC m)
# add compiler option -rdynamic -pedantic -O0 -g
target_compile_options(flex_adaptor PUBLIC -rdynamic -pedantic -O0 -g)
