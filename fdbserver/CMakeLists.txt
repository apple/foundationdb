fdb_find_sources(FDBSERVER_SRCS)

if(${COROUTINE_IMPL} STREQUAL libcoro)
  list(APPEND FDBSERVER_SRCS coroimpl/CoroFlowCoro.actor.cpp)
else()
  list(APPEND FDBSERVER_SRCS coroimpl/CoroFlow.actor.cpp)
endif()


if(WITH_ROCKSDB)
  include(CompileRocksDB)
  # CompileRocksDB sets `lz4_LIBRARIES` to be the shared lib, we want to link
  # statically, so find the static library here.
  find_library(lz4_STATIC_LIBRARIES
    NAMES liblz4.a REQUIRED)
  if(WITH_LIBURING)
    find_package(uring)
  endif()
endif()

file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/workloads)

add_flow_target(EXECUTABLE NAME fdbserver SRCS ${FDBSERVER_SRCS})

if (WITH_SWIFT)
  # Setup the Swift sources in FDBServer.
  include(FindSwiftLibs)

  add_definitions(-DWITH_SWIFT)

  add_library(fdbserver_swift STATIC
          masterserver.swift
          swift_fdbserver_cxx_swift_value_conformance.swift
          swift_fdbserver_collections.swift
          swift_fdbserver_stream_support.swift
          UID.swift
          # tests
          swift/tests/Rainbow.swift
          swift/tests/SimpleSwiftTestSuite.swift
          swift/tests/swift_tests.swift
          swift/tests/swift_test_task.swift
          swift/tests/swift_test_streams.swift
          )

  target_include_directories(fdbserver_swift PUBLIC
      "${CMAKE_BINARY_DIR}/flow/include"
      "${CMAKE_BINARY_DIR}/flow/"
      "${CMAKE_BINARY_DIR}/fdbclient"
      "${CMAKE_BINARY_DIR}/fdbclient/include"
      "${CMAKE_BINARY_DIR}/fdbserver/include"
      "${CMAKE_SOURCE_DIR}/fdbrpc/include"
      "${CMAKE_BINARY_DIR}/fdbrpc/include"
      "${CMAKE_SOURCE_DIR}/metacluster/include"
      "${CMAKE_SOURCE_DIR}/contrib/md5/include"
      "${CMAKE_SOURCE_DIR}/contrib/libb64/include"
      "${CMAKE_SOURCE_DIR}/contrib/sqlite"
      "${Boost_DIR}/../../../include"
      "${msgpack_DIR}/include"
      "${CMAKE_SOURCE_DIR}/contrib/rapidjson"
  )

  # TODO: the TBD validation skip is because of swift_job_run_generic, though it seems weird why we need to do that?
  target_compile_options(fdbserver_swift PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL:-Xcc -std=c++20 -Xfrontend -validate-tbd-against-ir=none -Xcc -DNO_INTELLISENSE -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/flow/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbclient/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbserver/include/headeroverlay.yaml>")

  # Ensure that C++ code in fdbserver can import Swift using a compatibility header.
  include(SwiftToCXXInterop)

  add_swift_to_cxx_header_gen_target(
    fdbserver_swift
    fdbserver_swift_to_cxx_conformance_header
    "${CMAKE_CURRENT_BINARY_DIR}/include/SwiftModules/FDBServer_CxxTypeConformances.h"
    SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/swift_fdbserver_cxx_swift_value_conformance.swift"
     FLAGS
  -Xcc -fmodules-cache-path=${CLANG_MODULE_CACHE_PATH}
  -Xcc -std=c++20 -Xcc -DNO_INTELLISENSE -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/flow/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbclient/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbserver/include/headeroverlay.yaml
  # Important: This is needed to avoid including header that depends on this generated header.
  -Xcc -DFDBSERVER_FORWARD_DECLARE_SWIFT_APIS
  -Xcc -DFOUNDATIONDB_FDBSERVER_STREAM_SUPPORT_H
  )

  add_swift_to_cxx_header_gen_target(
    fdbserver_swift
    fdbserver_swift_header
    "${CMAKE_CURRENT_BINARY_DIR}/include/SwiftModules/FDBServer"
     FLAGS
  -Xcc -fmodules-cache-path=${CLANG_MODULE_CACHE_PATH}
  -Xcc -std=c++20 -Xcc -DNO_INTELLISENSE -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/flow/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbclient/include/headeroverlay.yaml -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/fdbserver/include/headeroverlay.yaml
  # Important: This is needed to avoid including the generated header while generating it.
  -DFDBSERVER_FORWARD_DECLARE_SWIFT_APIS
  -Xcc -DFDBSERVER_FORWARD_DECLARE_SWIFT_APIS
  )

  add_dependencies(fdbserver_swift_to_cxx_conformance_header flow_swift flow_swift_header flow_actors fdbclient_actors fdbrpc_actors fdbserver_actors fdboptions)
  add_dependencies(fdbserver_swift_header fdbserver_swift_to_cxx_conformance_header)
  add_dependencies(fdbserver_swift fdbserver_swift_header)

  add_dependencies(fdbserver_swift flow_swift)
  add_dependencies(fdbserver_swift flow_actors)
  add_dependencies(fdbserver_swift fdbclient_swift)
  add_dependencies(fdbserver_swift fdbserver_actors)
  # This does not work! (see rdar://99107402)
  # target_link_libraries(flow PRIVATE flow_swift)
  add_dependencies(fdbserver fdbserver_swift)

  # TODO(swift): This probably should already work, try using link_libraries again
  # target_link_libraries(fdbserver PRIVATE flow_swift fdbserver_swift)
  add_dependencies(fdbserver flow_swift fdbclient_swift fdbserver_swift)
  add_custom_target(show_fdbserver_swift_objs COMMAND ${CMAKE_COMMAND} -E echo "$<TARGET_OBJECTS:fdbserver_swift>")

  target_link_options(fdbserver_swift PUBLIC "$<TARGET_OBJECTS:flow_swift>")
  target_link_options(fdbserver PRIVATE "$<TARGET_OBJECTS:flow_swift>")

  target_link_options(fdbserver_swift PUBLIC "$<TARGET_OBJECTS:fdbclient_swift>")
  target_link_options(fdbserver PRIVATE "$<TARGET_OBJECTS:fdbclient_swift>")
  # This dependency ensures that fdbserver depends on fdbclient_swift at link time.
  set_source_files_properties(IConfigConsumer.cpp PROPERTIES OBJECT_DEPENDS "${CMAKE_BINARY_DIR}/lib/libfdbclient_swift.a")

  target_link_options(fdbserver PRIVATE "$<TARGET_OBJECTS:fdbserver_swift>")
  # This dependency ensures that fdbserver depends on fdbserver_swift at link time.
  set_source_files_properties(IConfigConsumer.cpp PROPERTIES OBJECT_DEPENDS "${CMAKE_BINARY_DIR}/lib/libfdbserver_swift.a")

  swift_get_linker_search_paths(SWIFT_LINK_PATHS)
  target_link_directories(fdbserver PRIVATE "${SWIFT_LINK_PATHS}")

  # Generate the module map for FDBServer.
  include(GenerateModulemap)
  set(FDBSERVER_BINARY_DIR "${CMAKE_BINARY_DIR}/fdbserver")
  generate_modulemap("${CMAKE_BINARY_DIR}/fdbserver/include" "FDBServer" fdbserver
     OMIT
     ArtMutationBuffer.h # actually a textual include
     art_impl.h # actually a textual include
  )
endif()

target_include_directories(fdbserver PRIVATE
  ${CMAKE_SOURCE_DIR}/bindings/c
  ${CMAKE_BINARY_DIR}/bindings/c
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_BINARY_DIR}/include)
if (WITH_ROCKSDB)
  add_dependencies(fdbserver rocksdb)
  if(WITH_LIBURING)
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR} ${uring_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient metacluster sqlite ${ROCKSDB_LIBRARIES} ${uring_LIBRARIES} ${lz4_STATIC_LIBRARIES})
    target_compile_definitions(fdbserver PRIVATE BOOST_ASIO_HAS_IO_URING=1 BOOST_ASIO_DISABLE_EPOLL=1)
  else()
    target_include_directories(fdbserver PRIVATE ${ROCKSDB_INCLUDE_DIR})
    target_link_libraries(fdbserver PRIVATE fdbclient metacluster sqlite ${ROCKSDB_LIBRARIES} ${lz4_STATIC_LIBRARIES})
  endif()
else()
  target_link_libraries(fdbserver PRIVATE fdbclient metacluster sqlite)
endif()

if(USE_JEMALLOC)
  target_include_directories(fdbserver INTERFACE $<TARGET_PROPERTY:jemalloc::jemalloc, INTERFACE_INCLUDE_DIRECTORIES>)
  target_link_libraries(fdbserver PRIVATE jemalloc::jemalloc)
endif()

target_link_libraries(fdbserver PRIVATE toml11_target rapidjson)
if(WITH_ROCKSDB)
  target_compile_definitions(fdbserver PRIVATE WITH_ROCKSDB)
endif()

if (WITH_SWIFT)
  target_link_libraries(fdbserver PRIVATE swiftCxx swiftCxxStdlib)
endif()
# target_compile_definitions(fdbserver PRIVATE -DENABLE_SAMPLING)

if(GPERFTOOLS_FOUND)
  target_link_libraries(fdbserver PRIVATE gperftools)
endif()

if(NOT OPEN_FOR_IDE)
  if(GENERATE_DEBUG_PACKAGES)
    fdb_install(TARGETS fdbserver DESTINATION sbin COMPONENT server)
  else()
    add_custom_target(prepare_fdbserver_install ALL DEPENDS strip_only_fdbserver)
    fdb_install(PROGRAMS ${CMAKE_BINARY_DIR}/packages/bin/fdbserver DESTINATION sbin COMPONENT server)
  endif()
endif()
