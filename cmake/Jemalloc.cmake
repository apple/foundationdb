add_library(jemalloc INTERFACE)

if(NOT USE_JEMALLOC)
  return()
endif()

add_library(im_jemalloc_pic STATIC IMPORTED)
add_library(im_jemalloc STATIC IMPORTED)
if(EXISTS /opt/jemalloc_5.3.0)
  set(JEMALLOC_DIR /opt/jemalloc_5.3.0)
else()
  include(ExternalProject)
  set(JEMALLOC_DIR "${CMAKE_BINARY_DIR}/jemalloc")
  ExternalProject_add(Jemalloc_project
    URL "https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2"
    URL_HASH SHA256=2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa
    BUILD_BYPRODUCTS "${JEMALLOC_DIR}/include/jemalloc/jemalloc.h"
    "${JEMALLOC_DIR}/lib/libjemalloc.a"
    "${JEMALLOC_DIR}/lib/libjemalloc_pic.a"
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ./configure --prefix=${JEMALLOC_DIR} --enable-static --disable-cxx --enable-prof --enable-stats --
    with-malloc-conf=prof:true,prof_prefix:/var/tmp/fdbserver
    BUILD_IN_SOURCE ON
    BUILD_COMMAND make
    INSTALL_DIR "${JEMALLOC_DIR}"
    INSTALL_COMMAND make install)
endif()
add_dependencies(im_jemalloc Jemalloc_project)
add_dependencies(im_jemalloc_pic Jemalloc_project)
set_target_properties(im_jemalloc_pic PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc_pic.a")
set_target_properties(im_jemalloc PROPERTIES IMPORTED_LOCATION "${JEMALLOC_DIR}/lib/libjemalloc.a")
target_include_directories(jemalloc INTERFACE "${JEMALLOC_DIR}/include")
target_link_libraries(jemalloc INTERFACE im_jemalloc_pic im_jemalloc)
