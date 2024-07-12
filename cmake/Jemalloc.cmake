add_library(jemalloc INTERFACE)

if(NOT USE_JEMALLOC)
  return()
endif()

add_library(jemalloc::jemalloc STATIC IMPORTED)
add_library(jemalloc_pic::jemalloc_pic STATIC IMPORTED)

include(ExternalProject)
set(JEMALLOC_DIR "${CMAKE_BINARY_DIR}/jemalloc")
ExternalProject_add(Jemalloc_project
  URL "https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2"
  URL_HASH SHA256=2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa
  BUILD_BYPRODUCTS "${JEMALLOC_DIR}/include/jemalloc/jemalloc.h"
  "${JEMALLOC_DIR}/lib/libjemalloc.a"
  "${JEMALLOC_DIR}/lib/libjemalloc_pic.a"
  CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ./configure --prefix=${JEMALLOC_DIR} --enable-static --disable-cxx --enable-prof
  BUILD_IN_SOURCE ON
  BUILD_COMMAND make
  INSTALL_DIR "${JEMALLOC_DIR}"
  INSTALL_COMMAND make install)

add_dependencies(jemalloc::jemalloc Jemalloc_project)
add_dependencies(jemalloc_pic::jemalloc_pic Jemalloc_project)

set_target_properties(jemalloc::jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc.a")
set_target_properties(jemalloc_pic::jemalloc_pic PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc_pic.a")
