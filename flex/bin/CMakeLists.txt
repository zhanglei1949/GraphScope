# if(Hiactor_FOUND)
#         add_executable(rt_server rt_server.cc)
#         target_link_libraries(rt_server flex_utils flex_rt_mutable_graph flex_graph_db flex_server)
#         install_without_export_flex_target(rt_server)
# endif()

# if(Hiactor_FOUND)
#         include_directories(../engines/http_server)
#         add_executable(rt_bench rt_bench.cc)
#         target_link_libraries(rt_bench flex_utils flex_rt_mutable_graph flex_graph_db flex_server)
#         install_without_export_flex_target(rt_bench)
# endif()

# add_executable(rt_admin rt_admin.cc)
# target_link_libraries(rt_admin flex_utils flex_rt_mutable_graph flex_graph_db)
# install_without_export_flex_target(rt_admin)


# add_executable(adhoc_runner adhoc_runner.cc)
# target_link_libraries(adhoc_runner flex_utils flex_graph_db)
# install_without_export_flex_target(adhoc_runner)

# add_executable(flex_analytical_engine flex_analytical_engine.cc)
# target_link_libraries(flex_analytical_engine flex_immutable_graph flex_bsp ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})
# install_without_export_flex_target(flex_analytical_engine)


# if(Hiactor_FOUND)
#         add_executable(interactive_server interactive_server.cc)
#         target_link_libraries(interactive_server flex_utils flex_graph_db flex_postgres_wal flex_server flex_plan_proto flex_utils ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES})
#         if (OPENTELEMETRY_CPP_FOUND)
#                 target_link_libraries(interactive_server otel)
#         endif()
#         install_without_export_flex_target(interactive_server)
# endif()
# install the script
install(PROGRAMS load_plan_and_gen.sh DESTINATION bin)

# find_package(ICU REQUIRED)  
# if (ICU_FOUND)  
#     message(STATUS "ICU include: ${ICU_INCLUDE_DIRS}")  
#     message(STATUS "ICU libraries: ${ICU_LIBRARIES}")
#     include_directories(${ICU_INCLUDE_DIRS})  
# else()  
#     message(FATAL_ERROR "ICU not found")
# endif()


# include_directories(${Boost_INCLUDE_DIRS})
# add_executable(bulk_loader bulk_loader.cc)
# target_link_libraries(bulk_loader flex_rt_mutable_graph flex_utils ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${Boost_LIBRARIES})
# install_without_export_flex_target(bulk_loader)

# add_executable(stored_procedure_runner stored_procedure_runner.cc)
# target_link_libraries(stored_procedure_runner flex_rt_mutable_graph flex_utils flex_graph_db ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${Boost_LIBRARIES})
# install_without_export_flex_target(stored_procedure_runner)

# add_executable(wal_sender wal_sender.c)
# add_executable(wal_writer wal_writer.c)
# add -lm to link math library

# target_link_libraries(wal_sender PUBLIC ${POSTGRESS_LIB_PATH} ${POSTGRESS_PORT_LIB_PATH} ${POSTGRESS_COMMON_LIB_PATH} ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT} ZLIB::ZLIB)
# target_link_libraries(wal_sender PUBLIC m)

# target_link_libraries(wal_writer PUBLIC ${POSTGRESS_LIB_PATH} ${POSTGRESS_PORT_LIB_PATH} ${POSTGRESS_COMMON_LIB_PATH} ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT} ZLIB::ZLIB)
# target_link_libraries(wal_writer PUBLIC m)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../engines/http_server/)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils/)

add_executable(flex_postmaster ${CMAKE_CURRENT_SOURCE_DIR}/postmaster.c ${CMAKE_CURRENT_SOURCE_DIR}/post_master_main.c ${CMAKE_CURRENT_SOURCE_DIR}/syslogger.c ${CMAKE_CURRENT_SOURCE_DIR}/launch_backend.c
                                ${CMAKE_CURRENT_SOURCE_DIR}/flex_process.cc ${CMAKE_CURRENT_SOURCE_DIR}/flex_process_init.c)
target_link_libraries(flex_postmaster PUBLIC flex_postgres_backend flex_postgres_wal flex_graph_db flex_server ${GLOG_LIBRARY} ${POSTGRESS_PORT_LIB_PATH} ${POSTGRESS_COMMON_LIB_PATH} ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT} ZLIB::ZLIB m)