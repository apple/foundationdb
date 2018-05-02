include(ExternalProject)
ExternalProject_add(boostProject
  URL "https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.bz2"
  URL_HASH SHA256=2684c972994ee57fc5632e03bf044746f6eb45d4920c343937a465fd67a5adba
  CONFIGURE_COMMAND ${CMAKE_COMMAND} -E env CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ./bootstrap.sh --with-libraries=system,python --with-python=${PYTHON_EXECUTABLE}
  BUILD_COMMAND ${CMAKE_COMMAND} -E env CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ./b2 link=static --user-config=${CMAKE_BINARY_DIR}/user-config.jam
  BUILD_IN_SOURCE ON
  INSTALL_COMMAND ""
  UPDATE_COMMAND ""
  BUILD_BYPRODUCTS <SOURCE_DIR>/stage/lib/libboost_system.a <SOURCE_DIR>/stage/lib/libboost_python.a
  )

if(APPLE)
  set(BOOST_TOOLSET "darwin")
else()
  set(BOOST_TOOLSET "gcc")
endif()

set(BOOST_COMPILER_FLAGS -fvisibility=hidden -fPIC -std=c++14 -w)
set(BOOST_ADDITIONAL_COMPILE_OPTIOINS "")
foreach(flag IN LISTS BOOST_COMPILER_FLAGS)
  string(APPEND BOOST_ADDITIONAL_COMPILE_OPTIOINS "<cxxflags>${flag} ")
endforeach()
ExternalProject_Get_property(boostProject SOURCE_DIR)
configure_file(${CMAKE_SOURCE_DIR}/cmake/user-config.jam.cmake ${CMAKE_BINARY_DIR}/user-config.jam)

set(BOOST_INCLUDE_DIR ${SOURCE_DIR})
set(BOOST_LIBDIR ${SOURCE_DIR}/stage/lib)
set(BOOST_SYSTEM_LIBRARY ${BOOST_LIBDIR}/libboost_system.a)
set(BOOST_PYTHON_LIBRARY ${BOOST_LIBDIR}/libboost_python.a)
message(STATUS "Boost include dir ${BOOST_INCLUDE_DIR}")

add_library(boost_system INTERFACE)
add_dependencies(boost_system boostProject)
target_link_libraries(boost_system INTERFACE ${BOOST_SYSTEM_LIBRARY})
target_include_directories(boost_system INTERFACE ${BOOST_INCLUDE_DIR})

add_library(boost_python INTERFACE)
add_dependencies(boost_python boostProject)
target_link_libraries(boost_python INTERFACE ${BOOST_PYTHON_LIBRARY})
target_include_directories(boost_python INTERFACE ${BOOST_INCLUDE_DIR})
