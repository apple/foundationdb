set(FORCE_ALL_COMPONENTS OFF CACHE BOOL "Fails cmake if not all dependencies are found")

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
  find_package(OpenSSL)
  if(OPENSSL_FOUND)
    set(CMAKE_REQUIRED_INCLUDES ${OPENSSL_INCLUDE_DIR})
    check_symbol_exists("OPENSSL_INIT_NO_ATEXIT" "openssl/crypto.h" OPENSSL_HAS_NO_ATEXIT)
    if(OPENSSL_HAS_NO_ATEXIT)
      set(WITH_TLS ON)
      add_compile_options(-DHAVE_OPENSSL)
    else()
      message(WARNING "An OpenSSL version was found, but it doesn't support OPENSSL_INIT_NO_ATEXIT - Will compile without TLS Support")
      set(WITH_TLS OFF)
    endif()
  else()
    message(STATUS "OpenSSL was not found - Will compile without TLS Support")
    message(STATUS "You can set OPENSSL_ROOT_DIR to help cmake find it")
    set(WITH_TLS OFF)
  endif()
  if(WIN32)
    message(STATUS "TLS is temporarilty disabled on macOS while libressl -> openssl transition happens")
    set(WITH_TLS OFF)
  endif()
endif()

################################################################################
# Java Bindings
################################################################################

set(WITH_JAVA OFF)
find_package(JNI 1.8)
find_package(Java 1.8 COMPONENTS Development)
# leave FreeBSD JVM compat for later
if(JNI_FOUND AND Java_FOUND AND Java_Development_FOUND AND NOT (CMAKE_SYSTEM_NAME STREQUAL "FreeBSD"))
  set(WITH_JAVA ON)
  include(UseJava)
  enable_language(Java)
else()
  set(WITH_JAVA OFF)
endif()

################################################################################
# Python Bindings
################################################################################

find_package(Python COMPONENTS Interpreter)
if(Python_Interpreter_FOUND)
  set(WITH_PYTHON ON)
else()
  #message(FATAL_ERROR "Could not found a suitable python interpreter")
  set(WITH_PYTHON OFF)
endif()

################################################################################
# Pip
################################################################################

find_package(Python3 COMPONENTS Interpreter)
if (Python3_Interpreter_FOUND)
  set(WITH_DOCUMENTATION ON)
else()
  set(WITH_DOCUMENTATION OFF)
endif()

################################################################################
# GO
################################################################################

find_program(GO_EXECUTABLE go)
# building the go binaries is currently not supported on Windows
if(GO_EXECUTABLE AND NOT WIN32)
  set(WITH_GO ON)
else()
  set(WITH_GO OFF)
endif()

################################################################################
# Ruby
################################################################################

find_program(GEM_EXECUTABLE gem)
set(WITH_RUBY OFF)
if(GEM_EXECUTABLE)
  set(GEM_COMMAND ${RUBY_EXECUTABLE} ${GEM_EXECUTABLE})
  set(WITH_RUBY ON)
endif()

################################################################################
# RocksDB
################################################################################

set(SSD_ROCKSDB_EXPERIMENTAL ON CACHE BOOL "Build with experimental RocksDB support")
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

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/packages)
add_custom_target(packages)

function(print_components)
  message(STATUS "=========================================")
  message(STATUS "   Components Build Overview ")
  message(STATUS "=========================================")
  message(STATUS "Build Java Bindings:                  ${WITH_JAVA}")
  message(STATUS "Build with TLS support:               ${WITH_TLS}")
  message(STATUS "Build Go bindings:                    ${WITH_GO}")
  message(STATUS "Build Ruby bindings:                  ${WITH_RUBY}")
  message(STATUS "Build Python sdist (make package):    ${WITH_PYTHON}")
  message(STATUS "Build Documentation (make html):      ${WITH_DOCUMENTATION}")
  message(STATUS "Build Bindings (depends on Python):   ${WITH_PYTHON}")
  message(STATUS "Configure CTest (depends on Python):  ${WITH_PYTHON}")
  message(STATUS "Build with RocksDB:                   ${WITH_ROCKSDB_EXPERIMENTAL}")
  message(STATUS "=========================================")
endfunction()

if(FORCE_ALL_COMPONENTS)
  if(NOT WITH_JAVA OR NOT WITH_TLS OR NOT WITH_GO OR NOT WITH_RUBY OR NOT WITH_PYTHON OR NOT WITH_DOCUMENTATION)
    print_components()
    message(FATAL_ERROR "FORCE_ALL_COMPONENTS is set but not all dependencies could be found")
  endif()
endif()
