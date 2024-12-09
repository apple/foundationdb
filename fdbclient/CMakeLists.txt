fdb_find_sources(FDBCLIENT_SRCS)
list(APPEND FDBCLIENT_SRCS sha1/SHA1.cpp)

message(STATUS "FDB version is ${FDB_VERSION}")
message(STATUS "FDB package name is ${FDB_PACKAGE_NAME}")

set(options_srcs ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)

make_directory(${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/)
vexillographer_compile(TARGET fdboptions_vex LANG cpp OUT ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/FDBOptions.g.h
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/FDBOptions.g.h)

vexillographer_compile(TARGET fdboptions_c LANG c OUT ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/fdb_c_options.g.h
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/fdb_c_options.g.h)

add_custom_target(fdboptions DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/FDBOptions.g.h)
add_dependencies(fdboptions fdboptions_c)

################################################################################
# Build information
################################################################################

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/BuildFlags.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/BuildFlags.h)


set(BUILD_AZURE_BACKUP OFF CACHE BOOL "Build Azure backup client")
if(BUILD_AZURE_BACKUP)
  add_compile_definitions(BUILD_AZURE_BACKUP)
  set(FDBCLIENT_SRCS
    ${FDBCLIENT_SRCS}
    azure_backup/BackupContainerAzureBlobStore.actor.cpp)

  configure_file(azurestorage.cmake azurestorage-download/CMakeLists.txt)

  execute_process(
    COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE results
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-download
  )

  if(results)
    message(FATAL_ERROR "Configuration step for AzureStorage has Failed. ${results}")
  endif()

  execute_process(
    COMMAND ${CMAKE_COMMAND} --build . --config Release
    RESULT_VARIABLE results
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-download
  )

  if(results)
    message(FATAL_ERROR "Build step for AzureStorage has Failed. ${results}")
  endif()

  add_subdirectory(
    ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-src
    ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-build
  )
endif()


if(WITH_AWS_BACKUP)
  add_compile_definitions(WITH_AWS_BACKUP)
  include(awssdk)
endif()

add_flow_target(STATIC_LIBRARY NAME fdbclient SRCS ${FDBCLIENT_SRCS} ADDL_SRCS ${options_srcs})
target_include_directories(fdbclient PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/versions.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/versions.h)
add_dependencies(fdbclient fdboptions)
target_link_libraries(fdbclient PUBLIC fdbrpc msgpack PRIVATE rapidxml)

add_flow_target(EXECUTABLE NAME s3client SRCS S3Client_cli.actor.cpp)
target_include_directories(s3client PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
target_link_libraries(s3client PRIVATE fdbclient)

# Setup the Swift sources in FDBClient.
if(WITH_SWIFT)
  include(FindSwiftLibs)
  add_library(fdbclient_swift STATIC
          notified_support.swift
          )

  target_include_directories(fdbclient_swift PUBLIC
          "${CMAKE_BINARY_DIR}/flow/include"
          "${CMAKE_BINARY_DIR}/flow/"
          "${CMAKE_BINARY_DIR}/fdbclient/include"
          "${CMAKE_SOURCE_DIR}/fdbrpc/include"
          "${CMAKE_BINARY_DIR}/fdbrpc/include"
          "${CMAKE_SOURCE_DIR}/contrib/md5/include"
          "${CMAKE_SOURCE_DIR}/contrib/libb64/include"
          "${CMAKE_SOURCE_DIR}/contrib/sqlite"
          "${Boost_DIR}/../../../include"
          "${msgpack_DIR}/include"
          )

  # Generate the module map for FDBClient.
  include(GenerateModulemap)
  set(FDBCLIENT_BINARY_DIR "${CMAKE_BINARY_DIR}/fdbclient")
  generate_modulemap("${CMAKE_BINARY_DIR}/fdbclient/include" "FDBClient" fdbclient)

  # TODO: the TBD validation skip is because of swift_job_run_generic, though it seems weird why we need to do that?
  target_compile_options(fdbclient_swift PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL:-Xcc -std=c++20 -Xfrontend -validate-tbd-against-ir=none -Xcc -DNO_INTELLISENSE -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/flow/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbclient/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbclient/include/headeroverlay.yaml>")

  add_dependencies(fdbclient_swift flow_swift fdbclient_actors fdbrpc_actors fdboptions)
  # This does not work! (see rdar://99107402)
  # target_link_libraries(flow PRIVATE flow_swift)
  add_dependencies(fdbclient fdbclient_swift)

  target_link_options(fdbclient_swift PUBLIC "$<TARGET_OBJECTS:flow_swift>")
  target_link_options(fdbclient PRIVATE "$<TARGET_OBJECTS:flow_swift>")

  target_link_options(fdbclient PRIVATE "$<TARGET_OBJECTS:fdbclient_swift>")
endif()

# Create a separate fdbclient library with sampling enabled. This lets
# fdbserver retain sampling functionality in client code while disabling
# sampling for pure clients.
add_flow_target(STATIC_LIBRARY NAME fdbclient_sampling SRCS ${FDBCLIENT_SRCS} ADDL_SRCS ${options_srcs})
target_include_directories(fdbclient_sampling PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
add_dependencies(fdbclient_sampling fdboptions)
target_link_libraries(fdbclient_sampling PUBLIC fdbrpc_sampling msgpack PRIVATE rapidxml)
target_compile_definitions(fdbclient_sampling PRIVATE -DENABLE_SAMPLING)
if(WIN32)
  add_dependencies(fdbclient_sampling_actors fdbclient_actors)
endif()

add_flow_target(LINK_TEST NAME fdbclientlinktest SRCS LinkTest.cpp)
target_link_libraries(fdbclientlinktest PRIVATE fdbclient rapidxml) # re-link rapidxml due to private link interface

if(BUILD_AZURE_BACKUP)
  target_link_libraries(fdbclient PRIVATE curl azure-storage-lite)
  target_link_libraries(fdbclient_sampling PRIVATE curl azure-storage-lite)
endif()

if(WITH_AWS_BACKUP)
  target_link_libraries(fdbclient PUBLIC awssdk_target)
  target_link_libraries(fdbclient_sampling PUBLIC awssdk_target)
endif()

if (NOT WIN32 AND NOT OPEN_FOR_IDE)
  enable_testing()
  add_test(NAME s3client_test COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/tests/s3client_test.sh ${CMAKE_BINARY_DIR})
endif()
