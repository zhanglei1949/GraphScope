
file(GLOB_RECURSE METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

if (NOT BUILD_SQLITE_META_STORE)
    # remove files start with sqlite
    list(REMOVE_ITEM METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/sqlite*")
endif()

add_library(flex_metadata_store SHARED ${METADATA_SRC_FILES})
target_link_libraries(flex_metadata_store ${YAML_CPP_LIBRARIES} flex_utils)
if (BUILD_SQLITE_META_STORE)
    target_link_libraries(flex_metadata_store ${SQLite3_LIBRARIES})
endif()
install_flex_target(flex_metadata_store)




