project(awss3sdk-download NONE)

include(ExternalProject)
ExternalProject_Add(awss3sdk
  GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG           1.9.200
  GIT_CONFIG        advice.detachedHead=false
  CMAKE_ARGS        -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF -DBUILD_ONLY=core -DCMAKE_INSTALL_PREFIX=pleasegohere
  TEST_COMMAND      ""
  BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/libaws-s3-sdk.a"
)
