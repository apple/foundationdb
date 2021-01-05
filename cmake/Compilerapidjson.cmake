# Findrapidjson

find_package(rapidjson)

include(ExternalProject)

if (rapidjson_FOUND)
  message(STATUS "Found rapidjson library!")
  ExternalProject_Add(rapidjson
    PREFIX ${CMAKE_BINARY_DIR}/rapidjson
    DOWNLOAD_COMMAND ""
    INSTALL_COMMAND ""
  )
else()
  message(STATUS "rapidjson not found. Downloading ...")
  ExternalProject_Add(
    rapidjson
    PREFIX ${CMAKE_BINARY_DIR}/rapidjson
    GIT_REPOSITORY https://github.com/Tencent/rapidjson.git
    GIT_TAG f54b0e47a08782a6131cc3d60f94d038fa6e0a51 # v1.1.0
    TIMEOUT 10s
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
  )

  ExternalProject_Get_Property(rapidjson SOURCE_DIR)
  set (RAPIDJSON_INCLUDE_DIR "${SOURCE_DIR}/include")

  # set(rapidjson_FOUND TRUE)
endif()

message(STATUS "Found rapidjson includes: ${RAPIDJSON_INCLUDE_DIR}")

mark_as_advanced(
    RAPIDJSON_INCLUDE_DIR
)
