set(FORCE_ALL_COMPONENTS OFF CACHE BOOL "Fails cmake if not all dependencies are found")

################################################################################
# jemalloc
################################################################################

include(Jemalloc)

################################################################################
# Valgrind
################################################################################

if(USE_VALGRIND)
  find_package(Valgrind REQUIRED)
endif()

################################################################################
# SSL
################################################################################

include(CheckSymbolExists)

set(DISABLE_TLS OFF CACHE BOOL "Don't try to find OpenSSL and always build without TLS support")
if(DISABLE_TLS)
  set(WITH_TLS OFF)
else()
  set(OPENSSL_USE_STATIC_LIBS TRUE)
  if(WIN32)
    set(OPENSSL_MSVC_STATIC_RT ON)
  endif()
  find_package(OpenSSL)
  if(OPENSSL_FOUND)
    set(CMAKE_REQUIRED_INCLUDES ${OPENSSL_INCLUDE_DIR})
    set(WITH_TLS ON)
    add_compile_options(-DHAVE_OPENSSL)
  else()
    message(STATUS "OpenSSL was not found - Will compile without TLS Support")
    message(STATUS "You can set OPENSSL_ROOT_DIR to help cmake find it")
    set(WITH_TLS OFF)
  endif()
endif()

################################################################################
# Python Bindings
################################################################################

find_package(Python COMPONENTS Interpreter)
if(Python_Interpreter_FOUND)
  set(WITH_PYTHON ON)
else()
  message(WARNING "Could not found a suitable python interpreter")
  set(WITH_PYTHON OFF)
endif()

option(BUILD_PYTHON_BINDING "build python binding" ON)
if(NOT BUILD_PYTHON_BINDING OR NOT WITH_PYTHON)
  set(WITH_PYTHON_BINDING OFF)
else()
  if(WITH_PYTHON)
    set(WITH_PYTHON_BINDING ON)
  else()
    message(WARNING "Python binding depends on Python, but a python interpreter is not found")
    set(WITH_PYTHON_BINDING OFF)
  endif()
endif()

################################################################################
# C Bindings
################################################################################

option(BUILD_C_BINDING "build C binding" ON)
if(BUILD_C_BINDING AND NOT WITH_PYTHON)
    message(WARNING "C binding depends on Python, but a python interpreter is not found")
endif()
if(BUILD_C_BINDING AND WITH_PYTHON)
  set(WITH_C_BINDING ON)
else()
  set(WITH_C_BINDING OFF)
endif()

################################################################################
# Java Bindings
################################################################################

option(BUILD_JAVA_BINDING "build java binding" ON)
if(BUILD_JAVA_BINDING AND NOT WITH_C_BINDING)
  message(WARNING "Java binding depends on C binding, but C binding is not enabled")
endif()
if(NOT BUILD_JAVA_BINDING OR NOT WITH_C_BINDING)
  set(WITH_JAVA_BINDING OFF)
else()
  set(WITH_JAVA_BINDING OFF)
  find_package(JNI 1.8)
  find_package(Java 1.8 COMPONENTS Development)
  # leave FreeBSD JVM compat for later
  if(JNI_FOUND AND Java_FOUND AND Java_Development_FOUND AND NOT (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD") AND WITH_C_BINDING)
    set(WITH_JAVA_BINDING ON)
    include(UseJava)
    enable_language(Java)
  else()
    set(WITH_JAVA_BINDING OFF)
  endif()
endif()

################################################################################
# Pip
################################################################################

option(BUILD_DOCUMENTATION "build documentation" ON)
find_package(Python3 COMPONENTS Interpreter)
if (WITH_PYTHON AND Python3_Interpreter_FOUND AND BUILD_DOCUMENTATION)
  set(WITH_DOCUMENTATION ON)
else()
  set(WITH_DOCUMENTATION OFF)
endif()

################################################################################
# GO
################################################################################

option(BUILD_GO_BINDING "build go binding" ON)
if(BUILD_GO_BINDING AND NOT WITH_C_BINDING)
  message(WARNING "Go binding depends on C binding, but C binding is not enabled")
endif()
if(NOT BUILD_GO_BINDING OR NOT BUILD_C_BINDING)
  set(WITH_GO_BINDING OFF)
else()
  find_program(GO_EXECUTABLE go)
  # building the go binaries is currently not supported on Windows
  if(GO_EXECUTABLE AND NOT WIN32 AND WITH_C_BINDING)
    set(WITH_GO_BINDING ON)
  else()
    set(WITH_GO_BINDING OFF)
  endif()
  if (USE_SANITIZER)
    # Disable building go for sanitizers, since _stacktester doesn't link properly
    set(WITH_GO_BINDING OFF)
  endif()
endif()

################################################################################
# Ruby
################################################################################

option(BUILD_RUBY_BINDING "build ruby binding" ON)
if(BUILD_RUBY_BINDING AND NOT WITH_C_BINDING)
  message(WARNING "Ruby binding depends on C binding, but C binding is not enabled")
endif()
if(NOT BUILD_RUBY_BINDING OR NOT BUILD_C_BINDING)
  set(WITH_RUBY_BINDING OFF)
else()
  find_program(GEM_EXECUTABLE gem)
  set(WITH_RUBY_BINDING OFF)
  if(GEM_EXECUTABLE AND WITH_C_BINDING)
    set(GEM_COMMAND ${RUBY_EXECUTABLE} ${GEM_EXECUTABLE})
    set(WITH_RUBY_BINDING ON)
  endif()
endif()

################################################################################
# RocksDB
################################################################################

set(SSD_ROCKSDB_EXPERIMENTAL ON CACHE BOOL "Build with experimental RocksDB support")
set(PORTABLE_ROCKSDB ON CACHE BOOL "Compile RocksDB in portable mode") # Set this to OFF to compile RocksDB with `-march=native`
set(WITH_LIBURING OFF CACHE BOOL "Build with liburing enabled") # Set this to ON to include liburing
# RocksDB is currently enabled by default for GCC but does not build with the latest
# Clang.
if (SSD_ROCKSDB_EXPERIMENTAL AND GCC)
  set(WITH_ROCKSDB_EXPERIMENTAL ON)
else()
  set(WITH_ROCKSDB_EXPERIMENTAL OFF)
endif()

################################################################################
# TOML11
################################################################################

# TOML can download and install itself into the binary directory, so it should
# always be available.
find_package(toml11 QUIET)
if(toml11_FOUND)
  add_library(toml11_target INTERFACE)
  add_dependencies(toml11_target INTERFACE toml11::toml11)
else()
  include(ExternalProject)

  ExternalProject_add(toml11Project
    URL "https://github.com/ToruNiina/toml11/archive/v3.4.0.tar.gz"
    URL_HASH SHA256=bc6d733efd9216af8c119d8ac64a805578c79cc82b813e4d1d880ca128bd154d
    CMAKE_CACHE_ARGS
      -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/toml11
      -Dtoml11_BUILD_TEST:BOOL=OFF
    BUILD_ALWAYS ON)
  add_library(toml11_target INTERFACE)
  add_dependencies(toml11_target toml11Project)
  target_include_directories(toml11_target SYSTEM INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/toml11/include)
endif()

################################################################################
# Coroutine implementation
################################################################################

set(DEFAULT_COROUTINE_IMPL boost)
if(WIN32)
  # boost coroutine not available in windows build environment for now.
  set(DEFAULT_COROUTINE_IMPL libcoro)
elseif(NOT APPLE AND NOT USE_SANITIZER AND CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^x86")
  # revert to libcoro for x86 linux while we investigate a performance regression
  set(DEFAULT_COROUTINE_IMPL libcoro)
endif()

set(COROUTINE_IMPL ${DEFAULT_COROUTINE_IMPL} CACHE STRING "Which coroutine implementation to use. Options are boost and libcoro")

################################################################################
# AWS SDK
################################################################################

set(BUILD_AWS_BACKUP OFF CACHE BOOL "Build AWS S3 SDK backup client")
if (BUILD_AWS_BACKUP)
  set(WITH_AWS_BACKUP ON)
else()
  set(WITH_AWS_BACKUP OFF)
endif()

################################################################################

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/packages)
add_custom_target(packages)

function(print_components)
  message(STATUS "=========================================")
  message(STATUS "   Components Build Overview ")
  message(STATUS "=========================================")
  message(STATUS "Build Bindings (depends on Python):   ${WITH_PYTHON}")
  message(STATUS "Build C Bindings:                     ${WITH_C_BINDING}")
  message(STATUS "Build Python Bindings:                ${WITH_PYTHON_BINDING}")
  message(STATUS "Build Java Bindings:                  ${WITH_JAVA_BINDING}")
  message(STATUS "Build Go bindings:                    ${WITH_GO_BINDING}")
  message(STATUS "Build Ruby bindings:                  ${WITH_RUBY_BINDING}")
  message(STATUS "Build with TLS support:               ${WITH_TLS}")
  message(STATUS "Build Documentation (make html):      ${WITH_DOCUMENTATION}")
  message(STATUS "Build Python sdist (make package):    ${WITH_PYTHON_BINDING}")
  message(STATUS "Configure CTest (depends on Python):  ${WITH_PYTHON}")
  message(STATUS "Build with RocksDB:                   ${WITH_ROCKSDB_EXPERIMENTAL}")
  message(STATUS "Build with AWS SDK:                   ${WITH_AWS_BACKUP}")
  message(STATUS "=========================================")
endfunction()

if(FORCE_ALL_COMPONENTS)
  if(NOT WITH_C_BINDING OR NOT WITH_JAVA_BINDING OR NOT WITH_TLS OR NOT WITH_GO_BINDING OR NOT WITH_RUBY_BINDING OR NOT WITH_PYTHON_BINDING OR NOT WITH_DOCUMENTATION)
    print_components()
    message(FATAL_ERROR "FORCE_ALL_COMPONENTS is set but not all dependencies could be found")
  endif()
endif()
