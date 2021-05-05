find_package(Boost 1.72)

if(Boost_FOUND)
  add_library(boost_target INTERFACE)
  target_link_libraries(boost_target INTERFACE Boost::boost)
else()
  include(ExternalProject)
  ExternalProject_add(boostProject
    URL "https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.bz2"
    URL_HASH SHA256=59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722
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
  target_include_directories(boost_target SYSTEM INTERFACE ${BOOST_INCLUDE_DIR})
endif()
