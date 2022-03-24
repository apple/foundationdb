add_library(jemalloc INTERFACE)

if(NOT USE_JEMALLOC)
  return()
endif()

add_library(im_jemalloc_pic STATIC IMPORTED)
add_library(im_jemalloc STATIC IMPORTED)
include(ExternalProject)
set(JEMALLOC_DIR "${CMAKE_BINARY_DIR}/jemalloc")
ExternalProject_add(Jemalloc_project
  URL "https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2"
  URL_HASH SHA256=34330e5ce276099e2e8950d9335db5a875689a4c6a56751ef3b1d8c537f887f6
  BUILD_BYPRODUCTS "${JEMALLOC_DIR}/include/jemalloc/jemalloc.h"
  "${JEMALLOC_DIR}/lib/libjemalloc.a"
  "${JEMALLOC_DIR}/lib/libjemalloc_pic.a"
  PATCH_COMMAND patch -p1 < ${CMAKE_SOURCE_DIR}/cmake/jemalloc.patch
  CONFIGURE_COMMAND ./configure --prefix=${JEMALLOC_DIR} --enable-static --disable-cxx --enable-prof
  BUILD_IN_SOURCE ON
  BUILD_COMMAND make
  INSTALL_DIR "${JEMALLOC_DIR}"
  INSTALL_COMMAND make install)
add_dependencies(im_jemalloc Jemalloc_project)
add_dependencies(im_jemalloc_pic Jemalloc_project)
set_target_properties(im_jemalloc_pic PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc_pic.a")
set_target_properties(im_jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc.a")
target_include_directories(jemalloc INTERFACE "${JEMALLOC_DIR}/include")
target_link_libraries(jemalloc INTERFACE im_jemalloc_pic im_jemalloc)