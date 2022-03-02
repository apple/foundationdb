project(awss3sdk-download)

include(ExternalProject)
ExternalProject_Add(awss3sdk
  GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG           502b3999084911185bc52c6ab1f6687f0ea9a7d5 # v1.9.205
  GIT_CONFIG        advice.detachedHead=false
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-src"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build"
 CMAKE_ARGS        -DBUILD_ONLY=s3 -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF
  BUILD_COMMAND     ""
  INSTALL_COMMAND   ""
  TEST_COMMAND      ""
  BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/libaws-s3-sdk.a"
)
