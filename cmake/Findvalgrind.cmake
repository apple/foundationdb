# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
Findvalgrind
-------

Find valgrind, an instrumentation framework for building dynamic analysis tools.

valgrind_ROOT variable can be used for HINTS for different version of valgrind.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``valgrind_FOUND``
  If false, do not try to use valgrind.
``valgrind_INCLUDE_DIRS``
  path to valgrind include files, if found
#]=======================================================================]

include(FindPackageHandleStandardArgs)
include(FindPackageMessage)


# TODO it might be better to use pkg-config to set up valgrind

macro(_finalize_find_package_valgrind)
  find_package_handle_standard_args(
    valgrind
    FOUND_VAR valgrind_FOUND
    REQUIRED_VARS valgrind_INCLUDE_DIRS)

  mark_as_advanced(valgrind_INCLUDE_DIRS valgrind_FOUND)
endmacro()

if(NOT valgrind_ROOT)
  set(valgrind_ROOT $ENV{valgrind_ROOT})
endif()

find_path(
  valgrind_INCLUDE_DIRS
  NAMES valgrind.h
  PATH_SUFFIXES valgrind valgrind/include
  HINTS ${valgrind_ROOT})
if(NOT valgrind_INCLUDE_DIRS)
  _finalize_find_package_valgrind()
  return()
endif()

_finalize_find_package_valgrind()
