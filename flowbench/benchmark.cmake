cmake_minimum_required(VERSION 3.13)
project(googlebenchmark-download NONE)

include(ExternalProject)
ExternalProject_Add(googlebenchmark
  GIT_REPOSITORY    https://github.com/google/benchmark.git
  GIT_TAG           f91b6b42b1b9854772a90ae9501464a161707d1e # v1.6.0
  GIT_SHALLOW       ON
  GIT_CONFIG        advice.detachedHead=false
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-build"
  CMAKE_ARGS        "-DCMAKE_BUILD_TYPE=Release -DBENCHMARK_ENABLE_LTO=true"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND     ""
  INSTALL_COMMAND   ""
  TEST_COMMAND      ""
)

include(ExternalProject)
ExternalProject_Add(googletest DEPENDS googlebenchmark
  GIT_REPOSITORY    https://github.com/google/googletest.git
  GIT_TAG           2fe3bd994b3189899d93f1d5a881e725e046fdc2 # release-1.8.1
  GIT_SHALLOW       ON
  GIT_CONFIG        advice.detachedHead=false
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-src/googletest"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/googlebenchmark-build/googletest"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND     ""
  INSTALL_COMMAND   ""
  TEST_COMMAND      ""
)
