set(SRCS
  src/_stacktester/directory.go
  src/fdb/directory/allocator.go
  src/fdb/directory/node.go
  src/fdb/futures.go
  src/fdb/subspace/subspace.go
  src/_stacktester/stacktester.go
  src/fdb/directory/directory.go
  src/fdb/doc.go
  src/fdb/transaction.go
  src/fdb/directory/directoryLayer.go
  src/fdb/errors.go
  src/fdb/keyselector.go
  src/fdb/tuple/tuple.go
  src/fdb/cluster.go
  src/fdb/directory/directoryPartition.go
  src/fdb/fdb.go
  src/fdb/range.go
  src/fdb/tuple/tuple_test.go
  src/fdb/database.go
  src/fdb/directory/directorySubspace.go
  src/fdb/tenant.go
  src/fdb/fdb_test.go
  src/fdb/snapshot.go

  go.mod)

set(GOPATH ${CMAKE_CURRENT_BINARY_DIR})
set(GO_PACKAGE_ROOT github.com/apple/foundationdb/bindings/go)
set(GO_IMPORT_PATH ${GO_PACKAGE_ROOT}/src)
set(GO_DEST ${GOPATH}/src/${GO_PACKAGE_ROOT})

if(APPLE)
  set(GOPLATFORM darwin_amd64)
elseif(WIN32)
  set(GOPLATFORM windows_amd64)
else()
  set(GOPLATFORM linux_amd64)
endif()

set(GO_PACKAGE_OUTDIR ${GOPATH}/pkg/${GOPLATFORM}/${GO_IMPORT_PATH})

file(MAKE_DIRECTORY ${GOPATH}
                    ${GO_DEST})
set(go_options_file ${GO_DEST}/src/fdb/generated.go)

set(go_env GOPATH=${GOPATH}
  CGO_CFLAGS="-I${CMAKE_BINARY_DIR}/bindings/c/foundationdb;-I${CMAKE_SOURCE_DIR}/bindings/c"
  CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/lib
  GO111MODULE=auto)

foreach(src_file IN LISTS SRCS)
  set(dest_file ${GO_DEST}/${src_file})
  get_filename_component(dest_dir ${dest_file} DIRECTORY)
  list(APPEND SRCS_OUT ${dest_file})
  add_custom_command(OUTPUT ${dest_file}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${dest_dir} &&
            ${CMAKE_COMMAND} -E copy ${src_file} ${dest_file}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${src_file}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Creating fdb_go_path")
endforeach()
add_custom_target(copy_go_sources DEPENDS ${SRCS_OUT})
add_custom_command(OUTPUT ${go_options_file}
  COMMAND ${GO_EXECUTABLE} run ${CMAKE_CURRENT_SOURCE_DIR}/src/_util/translate_fdb_options.go
          -in ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
          -out ${go_options_file}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/_util/translate_fdb_options.go
          ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
  COMMENT "Generate FDBOptions for GO")
add_custom_target(go_options_file DEPENDS ${go_options_file})
add_dependencies(go_options_file copy_go_sources)

function(build_go_package)
  set(options LIBRARY EXECUTABLE INCLUDE_TEST)
  set(oneValueArgs NAME PATH)
  set(multiValueArgs)
  cmake_parse_arguments(BGP "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")

  if(NOT BGP_NAME OR NOT BGP_PATH)
    message(FATAL_ERROR "NAME and PATH arguments are missing")
  endif()
  if(BGP_LIBRARY AND BGP_EXECUTABLE)
    message(FATAL_ERROR "Package can't be a library and an executable")
  endif()
  if(NOT BGP_LIBRARY AND NOT BGP_EXECUTABLE)
    message(FATAL_ERROR "Missing type")
  endif()

  if(BGP_LIBRARY)
    if(WIN32)
      set(outfile ${GO_PACKAGE_OUTDIR}/${BGP_PATH}.lib)
    else()
      set(outfile ${GO_PACKAGE_OUTDIR}/${BGP_PATH}.a)
    endif()
  else()
    get_filename_component(exec_filename ${BGP_PATH} NAME)
    if(WIN32)
      set(outfile ${GOPATH}/bin/${exec_filename}.exe)
    else()
      set(outfile ${GOPATH}/bin/${exec_filename})
    endif()
  endif()
  add_custom_command(OUTPUT ${outfile}
    COMMAND ${CMAKE_COMMAND} -E env ${go_env}
            ${GO_EXECUTABLE} install ${GO_IMPORT_PATH}/${BGP_PATH}
    DEPENDS ${fdb_options_file}
    COMMENT "Building ${BGP_NAME}")
  add_custom_target(${BGP_NAME} ALL DEPENDS ${outfile})
  if(BGP_INCLUDE_TEST)
    set(testfile ${CMAKE_CURRENT_BINARY_DIR}/${BGP_NAME}_test)
    add_custom_command(OUTPUT ${testfile}
      COMMAND ${CMAKE_COMMAND} -E env ${go_env}
              ${GO_EXECUTABLE} test -c ${GO_IMPORT_PATH}/${BGP_PATH} -o ${testfile}
      DEPENDS ${fdb_options_file} fdb_c ${BGP_NAME}
      COMMENT "Building ${BGP_NAME} test")
    add_custom_target(${BGP_NAME}_test_target ALL DEPENDS ${testfile})
    set(library_path LD_LIBRARY_PATH)
    if (APPLE)
      set(library_path DYLD_LIBRARY_PATH)
    endif()
    add_fdbclient_test(
      NAME ${BGP_PATH}_go_test
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/${BGP_PATH}
      COMMAND ${CMAKE_COMMAND} -E env ${library_path}=${CMAKE_BINARY_DIR}/lib ${testfile}
    )
  endif()
endfunction()

build_go_package(LIBRARY NAME fdb_go PATH fdb INCLUDE_TEST)
add_dependencies(fdb_go fdb_c go_options_file)

build_go_package(LIBRARY NAME tuple_go PATH fdb/tuple INCLUDE_TEST)
add_dependencies(tuple_go fdb_go)

build_go_package(LIBRARY NAME subspace_go PATH fdb/subspace)
add_dependencies(subspace_go tuple_go)

build_go_package(LIBRARY NAME directory_go PATH fdb/directory)
add_dependencies(directory_go tuple_go)

build_go_package(EXECUTABLE NAME fdb_go_tester PATH _stacktester)
add_dependencies(fdb_go_tester directory_go)

# If this fails, then you need to update bindings/go/src/fdb/generated.go
# Ideally this wouldn't be necessary, but it looks like we distribute the go
# bindings directly from the source on github.
add_test(
  NAME update_bindings_go_src_fdb_generated_go
  COMMAND ${CMAKE_COMMAND} -E compare_files ${go_options_file} ${CMAKE_CURRENT_SOURCE_DIR}/src/fdb/generated.go
)
