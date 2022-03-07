project(awss3sdk-download NONE)

include(ExternalProject)
ExternalProject_Add(awss3sdk_project
  GIT_REPOSITORY    https://github.com/aws/aws-sdk-cpp.git
  GIT_TAG           2af3ce543c322cb259471b3b090829464f825972 # v1.9.200
  SOURCE_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-src"
  BINARY_DIR        "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build"
  BUILD_COMMAND     ninja -v # TODO REMOVE
  GIT_CONFIG        advice.detachedHead=false
  CMAKE_ARGS        -DBUILD_SHARED_LIBS=OFF
                    -DENABLE_TESTING=OFF
                    -DBUILD_ONLY=s3-crt
                    -DSIMPLE_INSTALL=ON
                    -DCMAKE_INSTALL_PREFIX=install


                    #-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} 
                    #-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} 
                    #-DCMAKE_CXX_COMPILE_FLAGS=-stdlib=libc++
                    #-DCMAKE_CXX_FLAGS=-stdlib=libc++
                    # -DCMAKE_STATIC_LINKER_FLAGS=${CMAKE_STATIC_LINKER_FLAGS}
                    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
                    -DCMAKE_VERBOSE_BUILD=ON

                    # does this fix crypto warning + runtime isuse?
                    # -DUSE_OPENSSL=OFF
                    # and/or this?
                    -DBYO_CRYPTO=ON
                    
                    # TODO use absolute path for install 
  TEST_COMMAND      ""
  BUILD_ALWAYS      TRUE
  # the s3 build produces a ton of artifcats, with their own dependency tree, so there is a very specific dependency order they must be linked in
  BUILD_BYPRODUCTS  "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-cpp-sdk-s3-crt.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-cpp-sdk-core.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-crt-cpp.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-s3.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-auth.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-event-stream.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-http.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-mqtt.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-io.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-checksums.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-compression.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-cal.a"
                    "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-common.a"
                    # "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libs2n.a"
)

# TODO since we don't actually use s3 api we could just remove that one? Still need everything below that though

add_library(awss3sdk_s3 STATIC IMPORTED)
add_dependencies(awss3sdk_s3 awss3sdk_project)
set_target_properties(awss3sdk_s3 PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-cpp-sdk-s3-crt.a")

add_library(awss3sdk_core STATIC IMPORTED)
add_dependencies(awss3sdk_core awss3sdk_project)
set_target_properties(awss3sdk_core PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-cpp-sdk-core.a")

add_library(awss3sdk_crt STATIC IMPORTED)
add_dependencies(awss3sdk_crt awss3sdk_project)
set_target_properties(awss3sdk_crt PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-crt-cpp.a")

add_library(awss3sdk_c_s3 STATIC IMPORTED)
add_dependencies(awss3sdk_c_s3 awss3sdk_project)
set_target_properties(awss3sdk_c_s3 PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-s3.a")

add_library(awss3sdk_c_auth STATIC IMPORTED)
add_dependencies(awss3sdk_c_auth awss3sdk_project)
set_target_properties(awss3sdk_c_auth PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-auth.a")

add_library(awss3sdk_c_eventstream STATIC IMPORTED)
add_dependencies(awss3sdk_c_eventstream awss3sdk_project)
set_target_properties(awss3sdk_c_eventstream PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-event-stream.a")

add_library(awss3sdk_c_http STATIC IMPORTED)
add_dependencies(awss3sdk_c_http awss3sdk_project)
set_target_properties(awss3sdk_c_http PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-http.a")

add_library(awss3sdk_c_mqtt STATIC IMPORTED)
add_dependencies(awss3sdk_c_mqtt awss3sdk_project)
set_target_properties(awss3sdk_c_mqtt PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-mqtt.a")

add_library(awss3sdk_c_io STATIC IMPORTED)
add_dependencies(awss3sdk_c_io awss3sdk_project)
set_target_properties(awss3sdk_c_io PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-io.a")

add_library(awss3sdk_checksums STATIC IMPORTED)
add_dependencies(awss3sdk_checksums awss3sdk_project)
set_target_properties(awss3sdk_checksums PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-checksums.a")

add_library(awss3sdk_c_compression STATIC IMPORTED)
add_dependencies(awss3sdk_c_compression awss3sdk_project)
set_target_properties(awss3sdk_c_compression PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-compression.a")

add_library(awss3sdk_c_cal STATIC IMPORTED)
add_dependencies(awss3sdk_c_cal awss3sdk_project)
set_target_properties(awss3sdk_c_cal PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-cal.a")

add_library(awss3sdk_c_common STATIC IMPORTED)
add_dependencies(awss3sdk_c_common awss3sdk_project)
set_target_properties(awss3sdk_c_common PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libaws-c-common.a")

# add_library(awss3sdk_s2n STATIC IMPORTED)
# add_dependencies(awss3sdk_s2n awss3sdk_project)
# set_target_properties(awss3sdk_s2n PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/lib64/libs2n.a")


# link them all together in one interface target
add_library(awss3sdk_target INTERFACE)
target_include_directories(awss3sdk_target SYSTEM INTERFACE ${CMAKE_CURRENT_BINARY_DIR}/awss3sdk-build/install/include)
# target_link_libraries(awss3sdk_target INTERFACE awss3sdk_s3 awss3sdk_core awss3sdk_crt awss3sdk_c_s3 awss3sdk_c_auth awss3sdk_c_eventstream awss3sdk_c_http awss3sdk_c_mqtt awss3sdk_c_io awss3sdk_checksums awss3sdk_c_compression awss3sdk_c_cal awss3sdk_c_common awss3sdk_s2n crypto)
target_link_libraries(awss3sdk_target INTERFACE awss3sdk_s3 awss3sdk_core awss3sdk_crt awss3sdk_c_s3 awss3sdk_c_auth awss3sdk_c_eventstream awss3sdk_c_http awss3sdk_c_mqtt awss3sdk_c_io awss3sdk_checksums awss3sdk_c_compression awss3sdk_c_cal awss3sdk_c_common)