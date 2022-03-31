set(SRCS
  DirectoryLayer.actor.cpp
  DirectoryLayer.h
  DirectoryPartition.h
  DirectorySubspace.cpp
  DirectorySubspace.h
  FDBLoanerTypes.h
  HighContentionAllocator.actor.cpp
  HighContentionAllocator.h
  IDirectory.h
  Node.actor.cpp
  Subspace.cpp
  Subspace.h
  Tuple.cpp
  Tuple.h
  fdb_flow.actor.cpp
  fdb_flow.h)

add_flow_target(STATIC_LIBRARY NAME fdb_flow SRCS ${SRCS})
target_link_libraries(fdb_flow PUBLIC fdb_c)
target_link_libraries(fdb_flow PUBLIC fdbclient)
target_include_directories(fdb_flow PUBLIC
  "${CMAKE_CURRENT_BINARY_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${CMAKE_CURRENT_SOURCE_DIR}/tester"
  "${CMAKE_CURRENT_BINARY_DIR}/tester"
  )

add_subdirectory(tester)

if(NOT OPEN_FOR_IDE)
  # generate flow-package
  foreach(f IN LISTS SRCS)
    if(f MATCHES ".*\\.h$")
      list(APPEND headers ${CMAKE_CURRENT_SOURCE_DIR}/${f})
    endif()
  endforeach()

  if(NOT FDB_RELEASE)
    set(not_fdb_release_string "-SNAPSHOT")
  else()
    set(not_fdb_release_string "")
  endif()

  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/packages)
  file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/packages)
  set(package_dir ${CMAKE_CURRENT_BINARY_DIR}/packages/fdb-flow-${FDB_VERSION})
  set(tar_file ${CMAKE_BINARY_DIR}/packages/fdb-flow-${FDB_VERSION}${not_fdb_release_string}-x86_64.tar.gz)
  add_custom_command(OUTPUT ${tar_file}
    COMMAND
    ${CMAKE_COMMAND} -E make_directory ${package_dir} &&
    ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fdb_flow> ${headers} ${package_dir} &&
    ${CMAKE_COMMAND} -E tar czf ${tar_file} ${package_dir}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/packages
    COMMENT "Build fdb_flow package")
  add_custom_target(package_flow DEPENDS ${tar_file})
  add_dependencies(packages package_flow)
endif()
