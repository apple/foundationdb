project(awss3sdk-download NONE)

include(ExternalProject)
ExternalProject_Add(awss3sdk
  GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG           1.9.200
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-src"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build"
  GIT_CONFIG        advice.detachedHead=false
  CMAKE_ARGS        -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF -DBUILD_ONLY=core -DSIMPLE_INSTALL=ON -DCMAKE_INSTALL_PREFIX=install
  TEST_COMMAND      ""
  BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-cpp-sdk-core.a"
)
