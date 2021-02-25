add_library(jemalloc INTERFACE)

set(USE_JEMALLOC ON)

# We don't want to use jemalloc on Windows
# FreeBSD already uses jemalloc
if(WIN32 OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  set(USE_JEMALLOC OFF)
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
  CONFIGURE_COMMAND ./configure --prefix=${JEMALLOC_DIR} --enable-static --disable-cxx --without-export --enable-stats --with-jemalloc-prefix=je_ --enable-prof
  BUILD_IN_SOURCE ON
  BUILD_COMMAND make
  INSTALL_DIR "${JEMALLOC_DIR}"
  INSTALL_COMMAND make install)
add_dependencies(im_jemalloc Jemalloc_project)
  add_dependencies(im_jemalloc_pic Jemalloc_project)
set_target_properties(im_jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc.a")
  set_target_properties(im_jemalloc_pic PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc_pic.a")
target_include_directories(jemalloc INTERFACE "${JEMALLOC_DIR}/include")
target_link_libraries(jemalloc INTERFACE im_jemalloc_pic im_jemalloc)
