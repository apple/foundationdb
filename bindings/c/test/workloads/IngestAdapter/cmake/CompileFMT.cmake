include(ExternalProject)
ExternalProject_add(fmtProject
  GIT_REPOSITORY https://github.com/fmtlib/fmt.git
  GIT_TAG 4.1.0
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR> -DCMAKE_INSTALL_LIBDIR=lib -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DFMT_USE_CPP14=ON
  BUILD_BYPRODUCTS <INSTALL_DIR>/lib/libfmt.a)

ExternalProject_Get_property(fmtProject INSTALL_DIR)
set(FMT_INCLUDE_DIR ${INSTALL_DIR}/include)
set(FMT_LIBRARY ${INSTALL_DIR}/lib/libfmt.a)

add_library(fmt INTERFACE)
add_dependencies(fmt fmtProject)
target_link_libraries(fmt INTERFACE ${FMT_LIBRARY})
target_include_directories(fmt INTERFACE ${FMT_INCLUDE_DIR})

