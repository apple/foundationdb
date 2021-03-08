if(NOT BOOSTROOT AND NOT BOOST_ROOT)
  # the boost dir in the official docker file
  # This is a hack because FindBoost.cmake is kind of limited
  # We can fix this for boost versions > 1.70
  set(BOOST_ROOT /opt/boost_1_67_0)
endif()

# Bug in boost subprocess in 1.72! In release-6.3 and later we replaced the
# boost subprocess usage with our own thing, but for release-6.2 it's important
# that we use boost 1.67 exactly.
find_package(Boost 1.67 EXACT)

if(Boost_FOUND)
  add_library(boost_target INTERFACE)
  target_link_libraries(boost_target INTERFACE Boost::boost)
else()
  include(ExternalProject)
  ExternalProject_add(boostProject
    URL "https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.bz2"
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
  target_include_directories(boost_target SYSTEM INTERFACE ${BOOST_INCLUDE_DIR})
endif()
