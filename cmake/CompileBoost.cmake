function(compile_boost)
  set(options)
  set(oneValueArgs TARGET)
  set(multiValueArgs BUILD_ARGS CXXFLAGS LDFLAGS)
  cmake_parse_arguments(MY "${options}" "${oneValueArgs}"
                          "${multiValueArgs}" ${ARGN} )
  # Configure the boost toolset to use
  set(BOOTSTRAP_ARGS "--with-libraries=context")
  set(B2_COMMAND "./b2")
  set(BOOST_COMPILER_FLAGS -fvisibility=hidden -fPIC -std=c++14 -w)
  set(BOOST_CXX_COMPILER "${CMAKE_CXX_COMPILER}")
  if(APPLE)
    set(BOOST_TOOLSET "clang-darwin")
    # this is to fix a weird macOS issue -- by default
    # cmake would otherwise pass a compiler that can't
    # compile boost
    set(BOOST_CXX_COMPILER "/usr/bin/clang++")
  elseif(CLANG)
    set(BOOST_TOOLSET "clang")
    list(APPEND BOOTSTRAP_ARGS "${BOOTSTRAP_COMMAND} --with-toolset=clang")
  else()
    set(BOOST_TOOLSET "gcc")
  endif()
  if(APPLE OR USE_LIBCXX)
    list(APPEND BOOST_COMPILER_FLAGS -stdlib=libc++)
  endif()
  set(BOOST_ADDITIONAL_COMPILE_OPTIOINS "")
  foreach(flag IN LISTS BOOST_COMPILER_FLAGS MY_CXXFLAGS)
    string(APPEND BOOST_ADDITIONAL_COMPILE_OPTIOINS "<cxxflags>${flag} ")
  endforeach()
  foreach(flag IN LISTS MY_LDFLAGS)
    string(APPEND BOOST_ADDITIONAL_COMPILE_OPTIOINS "<linkflags>${flag} ")
  endforeach()
  configure_file(${CMAKE_SOURCE_DIR}/cmake/user-config.jam.cmake ${CMAKE_BINARY_DIR}/user-config.jam)

  set(USER_CONFIG_FLAG --user-config=${CMAKE_BINARY_DIR}/user-config.jam)

  include(ExternalProject)
  set(BOOST_INSTALL_DIR "${CMAKE_BINARY_DIR}/boost_install")
  ExternalProject_add("${MY_TARGET}Project"
    URL "https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.bz2"
    URL_HASH SHA256=59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722
    CONFIGURE_COMMAND ./bootstrap.sh ${BOOTSTRAP_ARGS}
    BUILD_COMMAND ${B2_COMMAND} link=static ${MY_BUILD_ARGS} --prefix=${BOOST_INSTALL_DIR} ${USER_CONFIG_FLAG} install
    BUILD_IN_SOURCE ON
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    BUILD_BYPRODUCTS "${BOOST_INSTALL_DIR}/boost/config.hpp"
                     "${BOOST_INSTALL_DIR}/lib/libboost_context.a")

  add_library(${MY_TARGET}_context STATIC IMPORTED)
  add_dependencies(${MY_TARGET}_context ${MY_TARGET}Project)
  set_target_properties(${MY_TARGET}_context PROPERTIES IMPORTED_LOCATION "${BOOST_INSTALL_DIR}/lib/libboost_context.a")

  add_library(${MY_TARGET} INTERFACE)
  target_include_directories(${MY_TARGET} SYSTEM INTERFACE ${BOOST_INSTALL_DIR}/include)
  target_link_libraries(${MY_TARGET} INTERFACE ${MY_TARGET}_context)
endfunction()

if(USE_SANITIZER)
  if(WIN32)
    message(FATAL_ERROR "Sanitizers are not supported on Windows")
  endif()
  message(STATUS "A sanitizer is enabled, need to build boost from source")
  if (USE_VALGRIND)
    compile_boost(TARGET boost_asan BUILD_ARGS valgrind=on
      CXXFLAGS ${SANITIZER_COMPILE_OPTIONS} LDFLAGS ${SANITIZER_LINK_OPTIONS})
  else()
    compile_boost(TARGET boost_asan BUILD_ARGS context-impl=ucontext
      CXXFLAGS ${SANITIZER_COMPILE_OPTIONS} LDFLAGS ${SANITIZER_LINK_OPTIONS})
  endif()
  return()
endif()

list(APPEND CMAKE_PREFIX_PATH /opt/boost_1_72_0)
# since boost 1.72 boost installs cmake configs. We will enforce config mode
set(Boost_USE_STATIC_LIBS ON)
set(BOOST_HINT_PATHS /opt/boost_1_72_0)
if(BOOST_ROOT)
  list(APPEND BOOST_HINT_PATHS ${BOOST_ROOT})
endif()

if(WIN32)
  # this should be done with the line below -- but apparently the CI is not set up
  # properly for config mode. So we use the old way on Windows
  #  find_package(Boost 1.72.0 EXACT QUIET REQUIRED CONFIG PATHS ${BOOST_HINT_PATHS})
  # I think depending on the cmake version this will cause weird warnings
  find_package(Boost 1.72)
  add_library(boost_target INTERFACE)
  target_link_libraries(boost_target INTERFACE Boost::boost)
  return()
endif()

find_package(Boost 1.72.0 EXACT QUIET COMPONENTS context CONFIG PATHS ${BOOST_HINT_PATHS})
set(FORCE_BOOST_BUILD OFF CACHE BOOL "Forces cmake to build boost and ignores any installed boost")

if(Boost_FOUND AND NOT FORCE_BOOST_BUILD)
  add_library(boost_target INTERFACE)
  target_link_libraries(boost_target INTERFACE Boost::boost Boost::context)
elseif(WIN32)
  message(FATAL_ERROR "Could not find Boost")
else()
  if(FORCE_BOOST_BUILD)
    message(STATUS "Compile boost because FORCE_BOOST_BUILD is set")
  else()
    message(STATUS "Didn't find Boost -- will compile from source")
  endif()
  compile_boost(TARGET boost_target)
endif()
