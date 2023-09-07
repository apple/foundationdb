project(awssdk-download NONE)

# Compile the sdk with clang and libc++, since otherwise we get libc++ vs libstdc++ link errors when compiling fdb with clang
set(AWSSDK_COMPILER_FLAGS "")
if(APPLE OR USE_LIBCXX)
  set(AWSSDK_COMPILER_FLAGS "-stdlib=libc++ -nostdlib++")
endif()


set(AWSSDK_MODULES "identity-management")

include(ExternalProject)
ExternalProject_Add(awssdk_project
  GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG           e4b4b310d8631bc7e9a797b6ac03a73c6f210bf6 # v1.9.331
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awssdk-src"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build"
  GIT_CONFIG        advice.detachedHead=false
  CMAKE_ARGS        -DBUILD_SHARED_LIBS=OFF        # SDK builds shared libs by default, we want static libs
                    -DENABLE_TESTING=OFF
                    -DBUILD_ONLY=${AWSSDK_MODULES}             # git repo contains SDK for every AWS product, we only want the core auth libraries
                    -DSIMPLE_INSTALL=ON
                    -DCMAKE_INSTALL_PREFIX=install # need to specify an install prefix so it doesn't install in /usr/lib - FIXME: use absolute path
                    -DBYO_CRYPTO=ON                # we have our own crypto libraries that conflict if we let aws sdk build and link its own
                    -DBUILD_CURL=ON
                    -DBUILD_ZLIB=ON
                    
                    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                    -DCMAKE_CXX_FLAGS=${AWSSDK_COMPILER_FLAGS}
  TEST_COMMAND      ""
  # the sdk build produces a ton of artifacts, with their own dependency tree, so there is a very specific dependency order they must be linked in
  BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-cpp-sdk-core.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-crt-cpp.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-s3.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-auth.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-event-stream.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-http.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-mqtt.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-sdkutils.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-io.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-checksums.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-compression.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-cal.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-common.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-cpp-sdk-sts.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-cpp-sdk-identity-management.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/external-install/curl/lib/libcurl.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/external-install/zlib/lib/libz.a"
)

add_library(awssdk_cpp_identity STATIC IMPORTED)
add_dependencies(awssdk_cpp_identity awssdk_project)
set_target_properties(awssdk_cpp_identity PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-cpp-sdk-identity-management.a")

add_library(awssdk_cpp_sts STATIC IMPORTED)
add_dependencies(awssdk_cpp_sts awssdk_project)
set_target_properties(awssdk_cpp_sts PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-cpp-sdk-sts.a")

add_library(awssdk_core STATIC IMPORTED)
add_dependencies(awssdk_core awssdk_project)
set_target_properties(awssdk_core PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-cpp-sdk-core.a")

add_library(awssdk_crt STATIC IMPORTED)
add_dependencies(awssdk_crt awssdk_project)
set_target_properties(awssdk_crt PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-crt-cpp.a")

# TODO: can we remove c_s3? It seems to be a dependency of libaws-crt
add_library(awssdk_c_s3 STATIC IMPORTED)
add_dependencies(awssdk_c_s3 awssdk_project)
set_target_properties(awssdk_c_s3 PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-s3.a")

add_library(awssdk_c_auth STATIC IMPORTED)
add_dependencies(awssdk_c_auth awssdk_project)
set_target_properties(awssdk_c_auth PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-auth.a")

add_library(awssdk_c_eventstream STATIC IMPORTED)
add_dependencies(awssdk_c_eventstream awssdk_project)
set_target_properties(awssdk_c_eventstream PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-event-stream.a")

add_library(awssdk_c_http STATIC IMPORTED)
add_dependencies(awssdk_c_http awssdk_project)
set_target_properties(awssdk_c_http PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-http.a")

add_library(awssdk_c_mqtt STATIC IMPORTED)
add_dependencies(awssdk_c_mqtt awssdk_project)
set_target_properties(awssdk_c_mqtt PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-mqtt.a")

add_library(awssdk_c_io STATIC IMPORTED)
add_dependencies(awssdk_c_io awssdk_project)
set_target_properties(awssdk_c_io PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-io.a")

add_library(awssdk_c_sdkutils STATIC IMPORTED)
add_dependencies(awssdk_c_sdkutils awssdk_project)
set_target_properties(awssdk_c_sdkutils PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-sdkutils.a")

add_library(awssdk_checksums STATIC IMPORTED)
add_dependencies(awssdk_checksums awssdk_project)
set_target_properties(awssdk_checksums PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-checksums.a")

add_library(awssdk_c_compression STATIC IMPORTED)
add_dependencies(awssdk_c_compression awssdk_project)
set_target_properties(awssdk_c_compression PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-compression.a")

add_library(awssdk_c_cal STATIC IMPORTED)
add_dependencies(awssdk_c_cal awssdk_project)
set_target_properties(awssdk_c_cal PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-cal.a")

add_library(awssdk_c_common STATIC IMPORTED)
add_dependencies(awssdk_c_common awssdk_project)
set_target_properties(awssdk_c_common PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/lib64/libaws-c-common.a")

add_library(curl STATIC IMPORTED)
add_dependencies(curl awssdk_project)
set_property(TARGET curl PROPERTY IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/external-install/curl/lib/libcurl.a")

add_library(zlib STATIC IMPORTED)
add_dependencies(zlib awssdk_project)
set_property(TARGET zlib PROPERTY IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/external-install/zlib/lib/libz.a")

# link them all together in one interface target
add_library(awssdk_target INTERFACE)
target_include_directories(awssdk_target SYSTEM INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/awssdk-build/install/include)
target_link_libraries(awssdk_target INTERFACE awssdk_cpp_identity awssdk_cpp_sts awssdk_core awssdk_crt awssdk_c_s3 awssdk_c_auth awssdk_c_eventstream awssdk_c_http awssdk_c_mqtt awssdk_c_sdkutils awssdk_c_io awssdk_checksums awssdk_c_compression awssdk_c_cal awssdk_c_common curl zlib)
