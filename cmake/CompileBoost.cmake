find_package(Boost 1.72 EXACT COMPONENTS context QUIET)

if(Boost_FOUND)
  add_library(boost_target INTERFACE)
  target_link_libraries(boost_target INTERFACE Boost::boost Boost::coroutine2)
else()
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
  if(APPLE OR WIN32)
    # don't set user-config on mac as the behavior is weird
    # and we don't support multiple compilers on macOS anyways
    set(USER_CONFIG_FLAG "")
  endif()

  include(ExternalProject)
  set(BOOST_INSTALL_DIR "${CMAKE_BINARY_DIR}/boost_install")
  ExternalProject_add(boostProject
    URL "https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.bz2"
    URL_HASH SHA256=59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722
    CONFIGURE_COMMAND ./bootstrap.sh --with-libraries=context
    BUILD_COMMAND ./b2 link=static --prefix=${BOOST_INSTALL_DIR} ${USER_CONFIG_FLAG} install
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
