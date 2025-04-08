# - Config file for the Flex package
#
# It defines the following variables
#
#  FLEX_INCLUDE_DIR         - include directory for src
#  FLEX_INCLUDE_DIRS        - include directories for src
#  FLEX_LIBRARIES           - libraries to link against

set(FLEX_HOME "${CMAKE_CURRENT_LIST_DIR}/../../..")
include("${CMAKE_CURRENT_LIST_DIR}/src-targets.cmake")

set(FLEX_LIBRARIES src::flex_utils src::flex_rt_mutable_graph src::flex_graph_db src::flex_bsp src::flex_immutable_graph src::flex_plan_proto)
set(FLEX_INCLUDE_DIR "${FLEX_HOME}/include")
set(FLEX_INCLUDE_DIRS "${FLEX_INCLUDE_DIR}")

include(FindPackageMessage)
find_package_message(src
    "Found Flex: ${CMAKE_CURRENT_LIST_FILE} (found version \"@FLEX_VERSION@\")"
    "Flex version: @FLEX_VERSION@\nFlex libraries: ${FLEX_LIBRARIES}, include directories: ${FLEX_INCLUDE_DIRS}"
)
