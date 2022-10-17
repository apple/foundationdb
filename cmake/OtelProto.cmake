set(otel_SOURCE_DIR "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-proto")
set(otel_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/opentelemetry-proto-build")

include(ExternalProject)
ExternalProject_Add(
  opentelemetry-proto
  GIT_REPOSITORY https://github.com/open-telemetry/opentelemetry-proto.git
  SOURCE_DIR ${otel_SOURCE_DIR}
  BINARY_DIR ${otel_GEN_DIR}
  GIT_TAG "v0.19.0"
  UPDATE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ""
  CONFIGURE_COMMAND ""
  TEST_AFTER_INSTALL 0
  DOWNLOAD_NO_PROGRESS 1
  LOG_CONFIGURE 1
  LOG_BUILD 1
  LOG_INSTALL 1)

set(COMMON_PROTO "${otel_SOURCE_DIR}/opentelemetry/proto/common/v1/common.proto")
set(RESOURCE_PROTO
    "${otel_SOURCE_DIR}/opentelemetry/proto/resource/v1/resource.proto")
set(TRACE_PROTO "${otel_SOURCE_DIR}/opentelemetry/proto/trace/v1/trace.proto")
set(LOGS_PROTO "${otel_SOURCE_DIR}/opentelemetry/proto/logs/v1/logs.proto")
set(METRICS_PROTO "${otel_SOURCE_DIR}/opentelemetry/proto/metrics/v1/metrics.proto")

set(TRACE_SERVICE_PROTO
    "${otel_SOURCE_DIR}/opentelemetry/proto/collector/trace/v1/trace_service.proto")
set(LOGS_SERVICE_PROTO
    "${otel_SOURCE_DIR}/opentelemetry/proto/collector/logs/v1/logs_service.proto")
set(METRICS_SERVICE_PROTO
    "${otel_SOURCE_DIR}/opentelemetry/proto/collector/metrics/v1/metrics_service.proto"
)

set(GENERATED_PROTOBUF_PATH
    "${otel_GEN_DIR}/generated/third_party/opentelemetry-proto")

file(MAKE_DIRECTORY "${GENERATED_PROTOBUF_PATH}")

set(COMMON_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/common/v1/common.pb.cc")
set(COMMON_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/common/v1/common.pb.h")
set(RESOURCE_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/resource/v1/resource.pb.cc")
set(RESOURCE_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/resource/v1/resource.pb.h")
set(TRACE_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/trace/v1/trace.pb.cc")
set(TRACE_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/trace/v1/trace.pb.h")
set(LOGS_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/logs/v1/logs.pb.cc")
set(LOGS_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/logs/v1/logs.pb.h")
set(METRICS_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/metrics/v1/metrics.pb.cc")
set(METRICS_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/metrics/v1/metrics.pb.h")

set(TRACE_SERVICE_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/collector/trace/v1/trace_service.pb.cc"
)
set(TRACE_SERVICE_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/collector/trace/v1/trace_service.pb.h"
)
set(LOGS_SERVICE_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/collector/logs/v1/logs_service.pb.cc"
)
set(LOGS_SERVICE_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
)
set(METRICS_SERVICE_PB_CPP_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/collector/metrics/v1/metrics_service.pb.cc"
)
set(METRICS_SERVICE_PB_H_FILE
    "${GENERATED_PROTOBUF_PATH}/opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
)

foreach(IMPORT_DIR ${PROTOBUF_IMPORT_DIRS})
  list(APPEND PROTOBUF_INCLUDE_FLAGS "-I${IMPORT_DIR}")
endforeach()


set(PROTOBUF_PROTOC_EXECUTABLE "protoc")
add_custom_command(
    OUTPUT ${COMMON_PB_H_FILE}
           ${COMMON_PB_CPP_FILE}
           ${RESOURCE_PB_H_FILE}
           ${RESOURCE_PB_CPP_FILE}
           ${TRACE_PB_H_FILE}
           ${TRACE_PB_CPP_FILE}
           ${LOGS_PB_H_FILE}
           ${LOGS_PB_CPP_FILE}
           ${METRICS_PB_H_FILE}
           ${METRICS_PB_CPP_FILE}
           ${TRACE_SERVICE_PB_H_FILE}
           ${TRACE_SERVICE_PB_CPP_FILE}
           ${LOGS_SERVICE_PB_H_FILE}
           ${LOGS_SERVICE_PB_CPP_FILE}
           ${METRICS_SERVICE_PB_H_FILE}
           ${METRICS_SERVICE_PB_CPP_FILE}
    COMMAND
      ${PROTOBUF_PROTOC_EXECUTABLE} ARGS "--experimental_allow_proto3_optional"
      "--proto_path=${PROTO_PATH}" ${PROTOBUF_INCLUDE_FLAGS}
      "--cpp_out=${GENERATED_PROTOBUF_PATH}" ${COMMON_PROTO} ${RESOURCE_PROTO}
      ${TRACE_PROTO} ${LOGS_PROTO} ${METRICS_PROTO} ${TRACE_SERVICE_PROTO}
      ${LOGS_SERVICE_PROTO} ${METRICS_SERVICE_PROTO})