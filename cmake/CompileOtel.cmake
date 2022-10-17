# function(target_create _target _lib)
#   add_library(${_target} STATIC IMPORTED)
#   set_target_properties(
#     ${_target} PROPERTIES IMPORTED_LOCATION
#                           "${opentelemetry_BINARY_DIR}/${_lib}")
# endfunction()

function(build_opentelemetry)

message("=============================================== cxx flags ${CMAKE_CXX_FLAGS}")

set(protobuf_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf")
set(protobuf_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf-build")
set(protobuf_INSTALL_DIR "${CMAKE_CURRENT_BINARY_DIR}/protobuf-install")
set(protobuf_CMAKE_ARGS -Dprotobuf_BUILD_TESTS=OFF
                        -Dprotobuf_BUILD_EXAMPLES=OFF
                        -Dprotobuf_BUILD_LIBPROTOC=ON
                        -Dprotobuf_BUILD_SHARED_LIBS=OFF
                        -DCMAKE_INSTALL_PREFIX=${protobuf_INSTALL_DIR}
                        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
                        -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
)

add_library(libprotobuf STATIC IMPORTED)
include(ExternalProject)
ExternalProject_Add(
    protobuf
    URL https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protobuf-cpp-3.21.8.tar.gz
    SOURCE_DIR ${protobuf_SOURCE_DIR}
    BINARY_DIR ${protobuf_BINARY_DIR}
    BUILD_COMMAND ${CMAKE_COMMAND}
                  --build ${protobuf_BINARY_DIR}
                  --target install
                  --parallel 20
    INSTALL_COMMAND ""
    CMAKE_ARGS       ${protobuf_CMAKE_ARGS})

ExternalProject_Add(
    protoc
    URL https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protobuf-cpp-3.21.8.tar.gz
    SOURCE_DIR ${protobuf_SOURCE_DIR}
    BINARY_DIR ${protobuf_BINARY_DIR}-protoc
    BUILD_COMMAND ${CMAKE_COMMAND}
                  --build ${protobuf_BINARY_DIR}-protoc
                  --target install
                  --parallel 20
    INSTALL_COMMAND ""
    CMAKE_ARGS       -Dprotobuf_BUILD_TESTS=OFF
                        -Dprotobuf_BUILD_EXAMPLES=OFF
                        -Dprotobuf_BUILD_LIBPROTOC=ON
                        -Dprotobuf_BUILD_SHARED_LIBS=OFF
                        -DCMAKE_INSTALL_PREFIX=${protobuf_INSTALL_DIR}-protoc
                        )

                    # set(PROTOBUF_PROTOC_EXECUTABLE ${protobuf_BINARY_DIR})
                    # set(CMAKE_MODULE_PATH_SEARCH ${protobuf_BINARY_DIR})
                    # set(CMAKE_MODULE_PATH ${protobuf_BINARY_DIR})
                    set(PROTOBUF_PROTOC_EXECUTABLE ${protobuf_BINARY_DIR}-protoc)
                    set(CMAKE_MODULE_PATH_SEARCH ${protobuf_BINARY_DIR}-protoc)
                    set(CMAKE_MODULE_PATH ${protobuf_BINARY_DIR}-protoc)

  find_package( Protobuf REQUIRED HINTS ${protobuf_BINARY_DIR}-protoc )
if ( Protobuf_FOUND )
    message( STATUS "===============================Protobuf version : ${Protobuf_VERSION}" )
    message( STATUS "Protobuf include path : ${Protobuf_INCLUDE_DIRS}" )
    message( STATUS "Protobuf libraries : ${Protobuf_LIBRARIES}" )
    message( STATUS "Protobuf compiler libraries : ${Protobuf_PROTOC_LIBRARIES}")
    message( STATUS "Protobuf lite libraries : ${Protobuf_LITE_LIBRARIES}")
else()
    message( WARNING "Protobuf package not found -> specify search path via PROTOBUF_ROOT variable")
endif()

  set(opentelemetry_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-cpp")
  set(opentelemetry_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-build")
  set(opentelemetry_cpp_targets all)
  set(opentelemetry_CMAKE_ARGS
           -DCMAKE_POSITION_INDEPENDENT_CODE=ON
           -DCMAKE_MODULE_PATH=${protobuf_BINARY_DIR}-protoc
           -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
           -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
           -DBUILD_TESTING=OFF
           -DWITH_EXAMPLES=OFF
           -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
           -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
           -DWITH_OTLP=ON
           -DWITH_ZIPKIN=OFF
           -DWITH_OTLP_HTTP=OFF
           -DWITH_OTLP_GRPC=OFF
           # -DCMAKE_PREFIX_PATH=${protobuf_INSTALL_DIR}/include/
           -DPROTOBUF_PROTOC_EXECUTABLE=${protobuf_BINARY_DIR}-protoc/protoc
           -DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH_SEARCH}
           # -DCMAKE_INCLUDE_PATH=${protobuf_INSTALL_DIR}/include/
           -DCMAKE_PROJECT_opentelemetry-cpp_INCLUDE=${CMAKE_SOURCE_DIR}/cmake/FixOtel.cmake
           -DWITH_ZPAGES=OFF
           -DBUILD_W3CTRACECONTEXT_TEST=OFF
           -DWITH_ETW=OFF)
add_dependencies(libprotobuf protobuf)
set_target_properties(libprotobuf PROPERTIES IMPORTED_LOCATION "${protobuf_BINARY_DIR}/libprotobuf.a")
# set_target_properties(libprotobuf PROPERTIES IMPORTED_LOCATION "${protobuf_BINARY_DIR}/libprotobuf.a" INTERFACE_INCLUDE_DIRECTORIES "${protobuf_INSTALL_DIR}/include/")

# set(PROTOBUF_PROTOC_EXECUTABLE "${protobuf_BINARY_DIR}/protoc")
  # list(APPEND CMAKE_MODULE_PATH "${protobuf_BINARY_DIR}")


  set(opentelemetry_libs
     ${opentelemetry_BINARY_DIR}/sdk/src/metrics/libopentelemetry_metrics.a
     ${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc_metrics.a
     ${opentelemetry_BINARY_DIR}/sdk/src/common/libopentelemetry_common.a
     ${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc_client.a
     ${opentelemetry_BINARY_DIR}/sdk/src/resource/libopentelemetry_resources.a
     ${opentelemetry_BINARY_DIR}/sdk/src/trace/libopentelemetry_trace.a
     ${opentelemetry_BINARY_DIR}/libopentelemetry_proto.a
     ${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc.a
     ${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_otlp_recordable.a
     ${opentelemetry_BINARY_DIR}/sdk/src/version/libopentelemetry_version.a
     ${protobuf_BINARY_DIR}/libprotobuf.a
   )

  set(opentelemetry_include_dir ${opentelemetry_SOURCE_DIR}/api/include/
      ${opentelemetry_SOURCE_DIR}/sdk/include/
      ${opentelemetry_SOURCE_DIR}/exporters/otlp/include
   )

  set(opentelemetry_deps opentelemtry_metrics opentelemetry_grpc_metrics)
  add_library(libopentelemetry INTERFACE)
  # add_library(otel_resource STATIC IMPORTED)
  # add_library(otel_common STATIC IMPORTED)
  # add_library(otel_grpc_client STATIC IMPORTED)
  # add_library(otel_metrics STATIC IMPORTED)
  # add_library(otel_grpc_metrics STATIC IMPORTED)
  # add_library(otel_trace STATIC IMPORTED)
  add_library(otel_proto STATIC IMPORTED)
  # add_library(otel_grpc STATIC IMPORTED)
  # add_library(otel_record STATIC IMPORTED)
  # add_library(otel_version STATIC IMPORTED)
  # add_library(google_grpc STATIC IMPORTED)

include(ExternalProject)
ExternalProject_Add(
    opentelemetry-cpp
    URL https://github.com/open-telemetry/opentelemetry-cpp/archive/refs/tags/v1.6.1.tar.gz
    URL_HASH SHA256=1fc371be049b3220b8b9571c8b713f03e9a84f3c5684363f64ccc814638391a5
    SOURCE_DIR ${opentelemetry_SOURCE_DIR}
    BINARY_DIR ${opentelemetry_BINARY_DIR}
    BUILD_COMMAND ${CMAKE_COMMAND}
                  --build ${opentelemetry_BINARY_DIR}
                  --target all
    INSTALL_COMMAND ""
    CMAKE_ARGS       ${opentelemetry_CMAKE_ARGS}
    BUILD_BYPRODUCTS ${opentelemetry_libs})

   #target_create("opentelemetry_metrics"
   #              "${opentelemetry_BINARY_DIR}/sdk/src/metrics/libopentelemetry_metrics.a")
   # target_create("opentelemetry_grpc_metrics"
   #               "${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc_metrics.a")
  #  add_dependencies(otel_common opentelemetry-cpp)
  #  add_dependencies(otel_grpc_client opentelemetry-cpp)
  #  add_dependencies(otel_metrics opentelemetry-cpp)
  #  add_dependencies(otel_grpc_metrics opentelemetry-cpp)
  #  add_dependencies(otel_resource opentelemetry-cpp)
  #  add_dependencies(otel_trace opentelemetry-cpp)
  add_dependencies(opentelemetry-cpp libprotobuf)
  # add_dependencies(otel_proto libprotobuf)
  add_dependencies(otel_proto opentelemetry-cpp)
  #  add_dependencies(otel_grpc opentelemetry-cpp)
  #  add_dependencies(otel_record opentelemetry-cpp)
  #  add_dependencies(otel_version opentelemetry-cpp)
   #add_dependencies(google_grpc opentelemetry-cpp)
    # add_library(libopentelemetry INTERFACE IMPORTED)
    # #add_dependencies(opentelemetry::libopentelemetry opentelemetry-cpp)
    # set_target_properties(
    #   opentelemetry::libopentelemetry
    #    PROPERTIES
    #    INTERFACE_LINK_LIBRARIES "${opentelemetry_deps}")
  # set_target_properties(otel_common PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/sdk/src/common/libopentelemetry_common.a")
  # set_target_properties(otel_grpc_client PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc_client.a")
  # set_target_properties(otel_metrics PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/sdk/src/metrics/libopentelemetry_metrics.a")
  # set_target_properties(otel_grpc_metrics PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc_metrics.a")
  # set_target_properties(otel_resource PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/sdk/src/resource/libopentelemetry_resources.a")
  # set_target_properties(otel_trace PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/sdk/src/trace/libopentelemetry_trace.a")
  set_target_properties(otel_proto PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/libopentelemetry_proto.a")
  # set_target_properties(otel_grpc PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_exporter_otlp_grpc.a")
  # set_target_properties(otel_record PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/exporters/otlp/libopentelemetry_otlp_recordable.a")
  #set_target_properties(otel_version PROPERTIES IMPORTED_LOCATION "${opentelemetry_BINARY_DIR}/sdk/src/version/libopentelemetry_version.a")
  #set_target_properties(google_grpc PROPERTIES IMPORTED_LOCATION "/usr/local/lib/libgrpc++.a")

  # target_include_directories(libprotobuf INTERFACE 
  #     ${protobuf_INSTALL_DIR}/include)

target_include_directories(libopentelemetry INTERFACE 
${CMAKE_BINARY_DIR}/opentelemetry-cpp/api/include
${CMAKE_BINARY_DIR}/opentelemetry-cpp/sdk/include
${CMAKE_BINARY_DIR}/opentelemetry-cpp/exporters/otlp/include
${CMAKE_BINARY_DIR}/opentelemetry-build/generated/third_party/opentelemetry-proto)
#target_link_libraries(libopentelemetry INTERFACE google_proto google_grpc otel_common otel_metrics otel_grpc_client otel_grpc_metrics otel_resource otel_trace otel_proto otel_grpc otel_record otel_version)
target_link_libraries(otel_proto INTERFACE libprotobuf)
target_link_libraries(libopentelemetry INTERFACE otel_proto)
  #include_directories(SYSTEM ${opentelemetry_include_dir})
endfunction()
