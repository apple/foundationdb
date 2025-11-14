# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
Findsccache
-------

Find sccache, Shared Compilation Cache

CMAKE_C_COMPILER_LAUNCHER and CMAKE_CXX_COMPILER_LAUNCHER will be set if sccache is found

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``sccache_FOUND``
  If false, do not try to use sccache.
#]=======================================================================]

if(NOT sccache_ROOT)
  set(sccache_ROOT $ENV{sccache_ROOT})
endif()

find_program(
  SCCACHE_EXECUTABLE
  NAMES sccache
  HINTS ${sccache_ROOT})

if(NOT SCCACHE_EXECUTABLE)
  message(STATUS "sccache is not found")
  set(sccache_FOUND FALSE)
else()
  message(STATUS "sccache found, configuring sccache as the C/C++ compiler launcher")
  set(CMAKE_C_COMPILER_LAUNCHER ${SCCACHE_EXECUTABLE})
  set(CMAKE_CXX_COMPILER_LAUNCHER ${SCCACHE_EXECUTABLE})
  set(sccache_FOUND TRUE)
endif()