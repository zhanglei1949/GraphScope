cmake_minimum_required (VERSION 3.5)

include(CheckLibraryExists)
include(GNUInstallDirs)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})


project (
  Flex
  VERSION 0.1
  LANGUAGES CXX)

# reference: https://gitlab.kitware.com/cmake/community/-/wikis/doc/cmake/RPATH-handling#always-full-rpath
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../)

if(USE_PTHASH)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/pthash)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/murmurhash)
endif()
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/parallel-hashmap)
include_directories("/opt/graphscope/include")

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
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

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

find_package(Boost REQUIRED COMPONENTS system filesystem
             # required by folly
             context program_options regex thread date_time)
add_definitions("-DBOOST_BIND_GLOBAL_PLACEHOLDERS")

find_package(MPI REQUIRED)
include_directories(SYSTEM ${MPI_CXX_INCLUDE_PATH})

find_package(libgrapelite REQUIRED)
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS})
include_directories(SYSTEM ${LIBGRAPELITE_INCLUDE_DIRS}/grape/analytical_apps)


add_subdirectory(utils)
add_subdirectory(engines)



