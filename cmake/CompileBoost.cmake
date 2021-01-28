list(APPEND CMAKE_PREFIX_PATH /opt/boost_1_72_0)
# since boost 1.72 boost installs cmake configs. We will enforce config mode
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.72.0 EXACT COMPONENTS context CONFIG PATHS /opt/boost_1_72_0)
set(FORCE_BOOST_BUILD OFF CACHE BOOL "Forces cmake to build boost and ignores any installed boost")

if(Boost_FOUND AND NOT FORCE_BOOST_BUILD)
  add_library(boost_target INTERFACE)
  target_link_libraries(boost_target INTERFACE Boost::boost Boost::context)
else()
  if(FORCE_BOOST_BUILD)
    message(STATUS "Compile boost because FORCE_BOOST_BUILD is set")
  else()
    message(STATUS "Didn't find Boost -- will compile from source")
  endif()
  # Configure the boost toolset to use
  set(BOOTSTRAP_COMMAND "./bootstrap.sh --with-libraries=context")
  set(BOOST_COMPILER_FLAGS -fvisibility=hidden -fPIC -std=c++14 -w)
  if(APPLE)
    set(BOOST_TOOLSET "darwin")
  elseif(CLANG)
    set(BOOST_TOOLSET "clang")
    set(BOOTSTRAP_COMMAND "${BOOTSTRAP_COMMAND} --with-toolset=clang")
  else()
    set(BOOST_TOOLSET "gcc")
  endif()
  if(APPLE OR USE_LIBCXX)
    list(APPEND BOOST_COMPILER_FLAGS -stdlib=libc++)
  endif()
  set(BOOST_ADDITIONAL_COMPILE_OPTIOINS "")
  foreach(flag IN LISTS BOOST_COMPILER_FLAGS)
    string(APPEND BOOST_ADDITIONAL_COMPILE_OPTIOINS "<cxxflags>${flag} ")
  endforeach()
  configure_file(${CMAKE_SOURCE_DIR}/cmake/user-config.jam.cmake ${CMAKE_BINARY_DIR}/user-config.jam)

  set(USER_CONFIG_FLAG --user-config=${CMAKE_BINARY_DIR}/user-config.jam)
  if(APPLE)
    # don't set user-config on mac as the behavior is weird
    # and we don't support multiple compilers on macOS anyways
    set(USER_CONFIG_FLAG "cxxflags=-std=c++14")
  elseif(WIN32)
    set(USER_CONFIG_FLAG "")
  endif()

  if(WIN32)
    set(BOOTSTRAP_COMMAND bootstrap)
    set(B2_COMMAND "b2")
  else()
    set(BOOTSTRAP_COMMAND ./bootstrap.sh)
    set(B2_COMMAND "./b2")
  endif()

  include(ExternalProject)
  set(BOOST_INSTALL_DIR "${CMAKE_BINARY_DIR}/boost_install")
  ExternalProject_add(boostProject
    URL "https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.bz2"
    URL_HASH SHA256=59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722
    CONFIGURE_COMMAND ${BOOTSTRAP_COMMAND} --with-libraries=context
    BUILD_COMMAND ${B2_COMMAND} link=static --prefix=${BOOST_INSTALL_DIR} ${USER_CONFIG_FLAG} install
    BUILD_IN_SOURCE ON
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    BUILD_BYPRODUCTS "${BOOST_INSTALL_DIR}/boost/config.hpp"
                     "${BOOST_INSTALL_DIR}/lib/libboost_context.a")

  add_library(boost_context STATIC IMPORTED)
  add_dependencies(boost_context boostProject)
  set_target_properties(boost_context PROPERTIES IMPORTED_LOCATION "${BOOST_INSTALL_DIR}/lib/libboost_context.a")

  add_library(boost_target INTERFACE)
  add_dependencies(boost_target boostProject)
  target_include_directories(boost_target SYSTEM INTERFACE ${BOOST_INSTALL_DIR}/include)
  target_link_libraries(boost_target INTERFACE boost_context)
endif()
