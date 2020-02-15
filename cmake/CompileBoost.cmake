find_package(Boost 1.72)

if(Boost_FOUND)
  add_library(boost_target INTERFACE)
  target_link_libraries(boost_target INTERFACE Boost::boost)
else()
  include(ExternalProject)
  ExternalProject_add(boostProject
    URL "https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.bz2"
    URL_HASH SHA256=2684c972994ee57fc5632e03bf044746f6eb45d4920c343937a465fd67a5adba
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    BUILD_IN_SOURCE ON
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    BUILD_BYPRODUCTS <SOURCE_DIR>/boost/config.hpp)

  ExternalProject_Get_property(boostProject SOURCE_DIR)

  set(BOOST_INCLUDE_DIR ${SOURCE_DIR})
  message(STATUS "Boost include dir ${BOOST_INCLUDE_DIR}")

  add_library(boost_target INTERFACE)
  add_dependencies(boost_target boostProject)
  target_include_directories(boost_target INTERFACE ${BOOST_INCLUDE_DIR})
endif()

target_compile_definitions(boost_target INTERFACE BOOST_ALL_NO_LIB)
if(WIN32)
  target_compile_definitions(boost_target INTERFACE BOOST_USE_WINDOWS_H BOOST_ALL_NO_LIB NOMINMAX WIN32_LEAN_AND_MEAN)
endif()
if (${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang" AND "x${CMAKE_CXX_SIMULATE_ID}" STREQUAL "xMSVC")
endif()
