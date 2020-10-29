project(azurestorage-download)

include(ExternalProject)
ExternalProject_Add(azurestorage
  GIT_REPOSITORY    https://github.com/Azure/azure-storage-cpplite.git
  GIT_TAG           11e1f98b021446ef340f4886796899a6eb1ad9a5 # v0.3.0
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/azurestorage-src"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/azurestorage-build"
  CMAKE_ARGS        "-DCMAKE_BUILD_TYPE=Release"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND     ""
  INSTALL_COMMAND   ""
  TEST_COMMAND      ""
  BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/libazure-storage-lite.a"
)
