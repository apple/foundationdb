add_library(jemalloc INTERFACE)

set(USE_JEMALLOC ON)
# We don't want to use jemalloc on Windows
# Nor on FreeBSD, where jemalloc is the default system allocator
if(USE_SANITIZER OR WIN32 OR (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD") OR APPLE)
  set(USE_JEMALLOC OFF)
  return()
endif()

add_library(im_jemalloc_pic STATIC IMPORTED)
add_library(im_jemalloc STATIC IMPORTED)
include(ExternalProject)
set(JEMALLOC_DIR "${CMAKE_BINARY_DIR}/jemalloc")
ExternalProject_add(Jemalloc_project
  SOURCE_DIR "${CMAKE_SOURCE_DIR}/flow/jemalloc-5.2.1"
  DOWNLOAD_COMMAND ""
  BUILD_BYPRODUCTS "${JEMALLOC_DIR}/include/jemalloc/jemalloc.h"
  "${JEMALLOC_DIR}/lib/libjemalloc.a"
  "${JEMALLOC_DIR}/lib/libjemalloc_pic.a"
  CONFIGURE_COMMAND "${CMAKE_SOURCE_DIR}/flow/jemalloc-5.2.1/configure"
  --prefix=${JEMALLOC_DIR} --enable-static --disable-cxx --without-export
  --enable-stats --with-jemalloc-prefix=je_ --enable-prof
  --disable-initial-exec-tls # https://github.com/jemalloc/jemalloc/issues/937
  BUILD_COMMAND make
  INSTALL_DIR "${JEMALLOC_DIR}"
  INSTALL_COMMAND make install)
add_dependencies(im_jemalloc Jemalloc_project)
add_dependencies(im_jemalloc_pic Jemalloc_project)
set_target_properties(im_jemalloc_pic PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc_pic.a")
set_target_properties(im_jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc.a")
target_include_directories(jemalloc INTERFACE "${JEMALLOC_DIR}/include")
target_link_libraries(jemalloc INTERFACE im_jemalloc_pic im_jemalloc)
