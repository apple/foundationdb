project(awss3sdk-download NONE)

include(ExternalProject)
ExternalProject_Add(awss3sdk_project
  GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG           2af3ce543c322cb259471b3b090829464f825972 # v1.9.200
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-src"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build"
  GIT_CONFIG        advice.detachedHead=false
  CMAKE_ARGS        -DBUILD_SHARED_LIBS=OFF -DENABLE_TESTING=OFF -DBUILD_ONLY=core -DSIMPLE_INSTALL=ON -DCMAKE_INSTALL_PREFIX=install # TODO use absolute path for install
  TEST_COMMAND      ""
  BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-cpp-sdk-core.a"
)

add_library(awss3sdk_core STATIC IMPORTED)
add_dependencies(awss3sdk_core awss3sdk_project)
set_target_properties(awss3sdk_core PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-cpp-sdk-core.a")

add_library(awss3sdk_target INTERFACE)
#
# target_include_directories(awss3sdk_target SYSTEM INTERFACE ${SOURCE_DIR}/aws-cpp-sdk-core/include/)

# /mnt/ephemeral/jslocum/build/hackathon.linux.clang.x86_64/fdbclient/awss3sdk-build/install/include/aws/core/Aws.h
target_include_directories(awss3sdk_target SYSTEM INTERFACE ${BINARY_DIR}/install/include)
target_link_libraries(awss3sdk_target INTERFACE awss3sdk_core)

# When I try to include s3 instead of code:
# multiple static libs is do multiple add_library, add one target that depends on anything else, that one needs to be an interface library (do target link libraries, add other ones as system interface)

