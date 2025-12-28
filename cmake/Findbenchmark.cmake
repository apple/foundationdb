# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
Findbenchmark
-------

Find Google Benchmark

benchmark_ROOT variable can be used for HINTS for different version of Google benchmark.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``benchmark_FOUND``
  If false, do not try to use Google Benchmark.
``benchmark_INCLUDE_DIR``
  path to benchmark include directory
``benchmark_LIBRARY``
  path to benchmark library
``benchmark_main_LIBRARY``
  path to benchmark main library

This module will also define the following imported target:

``benchmark::benchmark``
  An imported target for Google Benchmark
#]=======================================================================]

include(FindPackageHandleStandardArgs)
include(FindPackageMessage)

macro(_finalize_find_package_benchmark)
  find_package_handle_standard_args(
    benchmark
    FOUND_VAR benchmark_FOUND
    REQUIRED_VARS benchmark_INCLUDE_DIR benchmark_LIBRARY benchmark_main_LIBRARY)

  if(benchmark_FOUND)
    add_library(benchmark::benchmark UNKNOWN IMPORTED)
    set_target_properties(
      benchmark::benchmark PROPERTIES
      INTERFACE_INCLUDE_DIRECTORIES "${benchmark_INCLUDE_DIR}"
      IMPORTED_LOCATION "${benchmark_LIBRARY}")
    add_library(benchmark::benchmark_main UNKNOWN IMPORTED)
    set_target_properties(
        benchmark::benchmark_main PROPERTIES
        IMPORTED_LOCATION "${benchmark_main_LIBRARY}")
  endif()

  mark_as_advanced(benchmark_FOUND benchmark_INCLUDE_DIR benchmark_LIBRARY benchmark_main_LIBRARY)
endmacro()

if(NOT benchmark_ROOT)
  set(benchmark_ROOT $ENV{benchmark_ROOT})
endif()

find_path(benchmark_INCLUDE_DIR
  NAMES benchmark/benchmark.h
  HINTS ${benchmark_ROOT}
  DOC "Google Benchmark include directory")
if(NOT benchmark_INCLUDE_DIR)
  _finalize_find_package_benchmark()
  return()
endif()

find_library(benchmark_LIBRARY
  NAMES libbenchmark.a
  HINTS ${benchmark_ROOT}
  DOC "Google Benchmark library")
find_library(benchmark_main_LIBRARY
  NAMES libbenchmark_main.a
  HINTS ${benchmark_ROOT}
  DOC "Google Benchmark main library")
if(NOT benchmark_LIBRARY OR NOT benchmark_main_LIBRARY)
  _finalize_find_package_benchmark()
  return()
endif()

_finalize_find_package_benchmark()