cmake_minimum_required(VERSION 3.5)
project(mako)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
message (STATUS "${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}")

if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are forbidden and unsupported")
endif()

# default to 307
if(NOT FDB_API_VERSION)
  message(STATUS "Setting FDB API version to 307")
  set(FDB_API_VERSION 307)
endif()

# we don't want rpath to be set
set(CMAKE_SKIP_BUILD_RPATH TRUE)

if(FDB_BUILD_DIR)
   set(FDB_LIB_DIR ${FDB_BUILD_DIR}/lib)
   if (FDB_SOURCE_DIR)
      set(FDB_INCLUDE_DIR ${FDB_SOURCE_DIR}/bindings/c ${FDB_BUILD_DIR}/bindings/c/foundationdb)
   endif()
else()
  if (APPLE)
    set(FDB_LIB_DIR ${FDB_DIR}/usr/local/lib)
    set(FDB_INCLUDE_DIR ${FDB_DIR}/usr/local/include)
  else()
    set(FDB_LIB_DIR ${FDB_DIR}/usr/lib64)
    set(FDB_INCLUDE_DIR ${FDB_DIR}/usr/include)
  endif()
endif()

link_directories(${FDB_LIB_DIR})
include_directories(${FDB_INCLUDE_DIR})

file(GLOB SOURCES "*.c")
add_executable(mako ${SOURCES})
if(APPLE)
  target_link_libraries(mako ${PROJECT_LINK_LIBS} pthread m fdb_c)
else()
  target_link_libraries(mako ${PROJECT_LINK_LIBS} pthread rt m fdb_c)
endif()
target_compile_options(mako PUBLIC -g -fPIC -O2 -DFDB_API_VERSION=${FDB_API_VERSION})
