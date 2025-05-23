cmake_minimum_required (VERSION 3.5)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/../VERSION FLEX_VERSION)
# Strip trailing newline
string(REGEX REPLACE "\n$" "" FLEX_VERSION "${FLEX_VERSION}")

project (
  Flex
  VERSION ${FLEX_VERSION}
  LANGUAGES CXX)


option(BUILD_TEST "Whether to build test" ON)
option(BUILD_DOC "Whether to build doc" OFF)
option(BUILD_ODPS_FRAGMENT_LOADER "Whether to build odps fragment loader" OFF)
option(USE_PTHASH "Whether to use pthash" OFF)
option(OPTIMIZE_FOR_HOST "Whether to optimize on host" ON) # Whether to build optimized code on host
option(USE_STATIC_ARROW "Whether to use static arrow" OFF) # Whether to link arrow statically, default is OFF
option(BUILD_WITH_OTEL "Whether to build with opentelemetry-cpp" OFF) # Whether to build with opentelemetry-cpp, default is OFF
option(BUILD_WITH_OSS "Whether to build with oss support" OFF) # Whether to build with oss support, default is OFF

#print options
message(STATUS "Build test: ${BUILD_TEST}")
message(STATUS "Build doc: ${BUILD_DOC}")
message(STATUS "Build odps fragment loader: ${BUILD_ODPS_FRAGMENT_LOADER}")
message(STATUS "Use pthash indexer : ${USE_PTHASH}")

# ------------------------------------------------------------------------------
# cmake configs
# ------------------------------------------------------------------------------

include(CheckLibraryExists)
include(GNUInstallDirs)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# reference: https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling#always-full-rpath
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

if(USE_PTHASH)
    message("Use PTHash")
    add_definitions(-DUSE_PTHASH)
endif()

execute_process(COMMAND uname -r OUTPUT_VARIABLE LINUX_KERNEL_VERSION)
string(STRIP ${LINUX_KERNEL_VERSION} LINUX_KERNEL_VERSION)
message(${LINUX_KERNEL_VERSION})
if(LINUX_KERNEL_VERSION VERSION_GREATER_EQUAL "4.5")
    message("Use copy file range")
    add_definitions(-DUSE_COPY_FILE_RANGE)
endif()

if (BUILD_WITH_OSS)
    add_definitions(-DBUILD_WITH_OSS)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/aliyun-oss-cpp-sdk/sdk/include)
    set(TARGET_OUTPUT_NAME_PREFIX "alibabacloud-oss-" CACHE STRING "The target's output name prefix")
    add_subdirectory(third_party/aliyun-oss-cpp-sdk)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../)

if(USE_PTHASH)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/pthash)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/murmurhash)
endif()

set(DEFAULT_BUILD_TYPE "Release")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE
      STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
               "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

add_compile_definitions(FLEX_VERSION="${FLEX_VERSION}")

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
else ()
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lopen-pal")
    else ()
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-rpath,$ORIGIN")
    endif ()
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -Werror -std=c++17 -Wall -fPIC")
if (OPTIMIZE_FOR_HOST)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

find_package(libgrapelite REQUIRED)
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS})
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)

# find Threads------------------------------------------------------------------
set(CMAKE_THREAD_PREFER_PTHREAD ON)
find_package(Threads REQUIRED)

# find glog---------------------------------------------------------------------
include("cmake/FindGlog.cmake")
if (NOT GLOG_FOUND)
    message(FATAL_ERROR "glog not found, please install the glog library")
else ()
    include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
endif ()

# find gflags-------------------------------------------------------------------
include("cmake/FindGFlags.cmake")
if (NOT GFLAGS_FOUND)
    message(STATUS "gflags not found, build without gflags")
else ()
    include_directories(SYSTEM ${GFLAGS_INCLUDE_DIRS})
endif ()

# find yaml-cpp-----------------------------------------------------------------
find_package(yaml-cpp REQUIRED)
include_directories(SYSTEM ${yaml-cpp_INCLUDE_DIRS})
if (NOT yaml-cpp_FOUND)
    message(FATAL_ERROR "yaml-cpp not found, please install the yaml-cpp library")
endif ()

#find boost----------------------------------------------------------------------
find_package(Boost REQUIRED COMPONENTS system filesystem
             # required by folly
             context program_options regex thread date_time)
add_definitions("-DBOOST_BIND_GLOBAL_PLACEHOLDERS")
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

#find hiactor----------------------------------------------------------------------
find_package(Hiactor)
if (NOT Hiactor_FOUND)
    message(STATUS "hiactor not found, please install the hiactor library")
else ()
    include_directories(SYSTEM ${Hiactor_INCLUDE_DIRS})
endif()

#find arrow----------------------------------------------------------------------
find_package(Arrow REQUIRED)
if (NOT Arrow_FOUND)
    message(FATAL_ERROR "arrow not found, please install the arrow library")
else ()
    include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
    if (USE_STATIC_ARROW)
        if (TARGET Arrow::arrow_static)
            set(ARROW_LIB Arrow::arrow_static)
        elseif (TARGET arrow_static) # For backward compatibility, see https://arrow.apache.org/docs/cpp/build_system.html#available-variables-and-targets
            set(ARROW_LIB arrow_static)
        else()
            message(FATAL_ERROR "Building with USE_STATIC_ARROW=ON, but arrow_static target not found")
        endif()
    else ()
        if (TARGET Arrow::arrow_shared)
            set(ARROW_LIB Arrow::arrow_shared)
        elseif (TARGET arrow_shared) # For backward compatibility
            set(ARROW_LIB arrow_shared)
        else()
            message(FATAL_ERROR "Building with USE_STATIC_ARROW=OFF, but arrow_shared target not found")
        endif()
    endif()
endif ()

#find protobuf-------------------------------------------------------------------
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

if (BUILD_WITH_OTEL)
    find_package(opentelemetry-cpp CONFIG)
    if (OPENTELEMETRY_CPP_FOUND)
        message(STATUS "opentelemetry-cpp found")
        find_package(CURL REQUIRED)
        add_definitions(-DHAVE_OPENTELEMETRY_CPP)
    else()
        message(STATUS "opentelemetry-cpp not found, build without opentelemetry-cpp")
    endif ()
endif()

# Find Doxygen
if (BUILD_DOC)
    find_package(Doxygen)
    # Add a target to generate the documentation
    if(DOXYGEN_FOUND)
        set(DOXYGEN_IN ${CMAKE_CURRENT_SOURCE_DIR}/docs/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        add_custom_target(doc ALL
                COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
                WORKING_DIRECTORY ..
                COMMENT "Generating API documentation with Doxygen"
                VERBATIM)
    endif(DOXYGEN_FOUND)
endif()

find_package(RapidJSON REQUIRED)
if (DEFINED RapidJSON_INCLUDE_DIRS)  # rapidjson > 1.1.0
    include_directories(${RapidJSON_INCLUDE_DIRS})
else ()  # rapidjson <= 1.1.0
    include_directories(${RAPIDJSON_INCLUDE_DIRS})
endif ()

add_definitions(-DRAPIDJSON_HAS_CXX11=1)
add_definitions(-DRAPIDJSON_HAS_STDSTRING=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RVALUE_REFS=1)
add_definitions(-DRAPIDJSON_HAS_CXX11_RANGE_FOR=1)

if (BUILD_ODPS_FRAGMENT_LOADER)
    include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/odps/include)
endif()
include_directories(SYSTEM ${CMAKE_SOURCE_DIR}/third_party/parallel-hashmap)

macro(install_flex_target target)
    install(TARGETS ${target}
            EXPORT flex-targets
            RUNTIME DESTINATION bin
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
    )
endmacro()

macro(install_without_export_flex_target target)
    install(TARGETS ${target}
            RUNTIME DESTINATION bin
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
    )
endmacro()

add_subdirectory(utils)
add_subdirectory(codegen)
add_subdirectory(storages)
add_subdirectory(engines)
add_subdirectory(bin)
if (OPENTELEMETRY_CPP_FOUND)
   add_subdirectory(otel)
endif()
if (BUILD_TEST)
   add_subdirectory(tests)
endif()


configure_file(${CMAKE_CURRENT_SOURCE_DIR}/flex-config.in.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/flex-config.cmake @ONLY)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/flex-config-version.in.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/flex-config-version.cmake @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/flex-config.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/flex-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/flex)

install(EXPORT flex-targets
        FILE flex-targets.cmake
	NAMESPACE flex::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/flex)

file(GLOB_RECURSE FILES_NEED_LINT
        "engines/*.cc"
        "engines/*.h"
        "codegen/*.cc"
        "codegen/*.h"
        "bin/*.cc"
        "storages/*.h"
        "storages/*.cc"
        "otel/*.h"
        "otel/*.cc"
        "test/*.h"
        "test/*.cc"
        "utils/*.h"
        "utils/*.cc"
        EXCEPT "*.act.h" "*.actg.h" "*.autogen.h" "*.autogen.cc")
list(FILTER FILES_NEED_LINT EXCLUDE REGEX ".*\.act.h$|.*\.actg.h$|.*\.autogen.h$|.*\.autogen.cc$")
# gsa_clformat
add_custom_target(flex_clformat
    COMMAND clang-format --style=file -i ${FILES_NEED_LINT}
    COMMENT "Running clang-format, using clang-format-8 from https://github.com/muttleyxd/clang-tools-static-binaries/releases"
    VERBATIM)

if (NOT DEFINED CPACK_PACKAGE_NAME)
  set(CPACK_PACKAGE_NAME "graphscope_flex")
endif ()
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Flex module of GraphScope")
set(CPACK_PACKAGE_VENDOR "GraphScope")
set(CPACK_PACKAGE_VERSION ${FLEX_VERSION})
set(CPACK_PACKAGE_RELEASE 1)
set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_CONTACT "graphscope@alibaba-inc.com")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "graphscope")
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
set(CPACK_DEB_COMPONENT_INSTALL YES)


#install CMakeLists.txt.template to resources/
install(FILES resources/hqps/CMakeLists.txt.template DESTINATION lib/flex/)

if(USE_PTHASH)
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/murmurhash
                      ${PROJECT_SOURCE_DIR}/third_party/pthash
            DESTINATION include
            FILES_MATCHING
            PATTERN "*.h"
            PATTERN "*.hpp"
    )
endif()

include(CPack)
