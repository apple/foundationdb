# sets out_var to YES if filename has extension .h or .hpp, NO otherwise
function(is_header out_var filename)
  set(res "NO")
  get_filename_component(ext "${filename}" LAST_EXT)
  if((ext STREQUAL ".h") OR (ext STREQUAL ".hpp"))
    set(res "YES")
  endif()
  set("${out_var}" "${res}" PARENT_SCOPE)
endfunction()

function(remove_prefix out prefix str)
  string(LENGTH "${prefix}" len)
  string(SUBSTRING "${str}" ${len} -1 res)
  set("${out}" "${res}" PARENT_SCOPE)
endfunction()

function(is_prefix out prefix str)
  string(LENGTH "${prefix}" plen)
  string(LENGTH "${str}" slen)
  if(plen GREATER slen)
    set(res NO)
  else()
    string(SUBSTRING "${str}" 0 ${plen} pstr)
    if(pstr STREQUAL prefix)
      set(res YES)
    else()
      set(res NO)
    endif()
  endif()
  set(${out} ${res} PARENT_SCOPE)
endfunction()

function(create_build_dirs)
  foreach(src IN LISTS ARGV)
    get_filename_component(d "${src}" DIRECTORY)
    if(IS_ABSOLUTE "${d}")
      file(RELATIVE_PATH d "${CMAKE_CURRENT_SOURCE_DIR}" "${src}")
    endif()
    list(APPEND dirs "${d}")
  endforeach()
  list(REMOVE_DUPLICATES dirs)
  foreach(dir IN LISTS dirs)
    make_directory("${CMAKE_CURRENT_BINARY_DIR}/${dir}")
  endforeach()
endfunction()

function(fdb_find_sources out)
  file(GLOB res
    LIST_DIRECTORIES false
    RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
    CONFIGURE_DEPENDS "*.cpp" "*.cc" "*.c" "*.h" "*.hpp")
  file(GLOB_RECURSE res_includes
    LIST_DIRECTORIES false
    RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}/include"
    CONFIGURE_DEPENDS "include/*.cpp" "include/*.cc" "include/*.c" "include/*.h" "include/*.hpp")
  file(GLOB_RECURSE res_workloads
    LIST_DIRECTORIES false
    RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}/workloads"
    CONFIGURE_DEPENDS "workloads/*.cpp" "workloads/*.c" "workloads/*.h" "workloads/*.hpp")

  foreach(f IN LISTS res_includes)
    list(APPEND res "include/${f}")
  endforeach()
  foreach(f IN LISTS res_workloads)
    list(APPEND res "workloads/${f}")
  endforeach()
  set(${out} "${res}" PARENT_SCOPE)
endfunction()

function(package_name_to_path result_var pkg_name)
  set(_pkg_name "${pkg_name}")
  string(REPLACE "." "/" _pkg_name ${_pkg_name})
  set(${result_var} ${_pkg_name} PARENT_SCOPE)
endfunction()

function(package_name_to_proto_target result_var pkg_name)
  set(_pkg_name "${pkg_name}")
  string(REPLACE "." "_" _pkg_name ${_pkg_name})
  set(${result_var} "proto_${_pkg_name}" PARENT_SCOPE)
endfunction()

# Args: package_name proto_file ...
function(generate_grpc_protobuf pkg_name)
  set(proto_files ${ARGN})
  package_name_to_proto_target(target_name ${pkg_name})
  package_name_to_path(out_rel_path ${pkg_name})

  add_library(${target_name} STATIC ${proto_files})
  target_include_directories(${target_name} PUBLIC ${CMAKE_BINARY_DIR}/generated/)
  target_include_directories(${target_name} PUBLIC ${Protobuf_INCLUDE_DIRS} ${gRPC_INCLUDE_DIRS})
  target_link_libraries(${target_name} PUBLIC gRPC::grpc++)

  set(protoc_out_dir "${CMAKE_BINARY_DIR}/generated/${out_rel_path}")
  message(STATUS "Generating protobuf target = ${target_name}, out_path = ${protoc_out_dir}, files = ${ARGN}")

  protobuf_generate(
      TARGET ${target_name}
      PROTOC_OUT_DIR ${protoc_out_dir}
      GENERATE_EXTENSIONS .pb.h .pb.cc
      APPEND_PATH ${out_rel_path}
      PROTOS ${proto_files}
  )

  protobuf_generate(
      TARGET ${target_name}
      LANGUAGE grpc
      PROTOC_OUT_DIR ${protoc_out_dir}
      PLUGIN protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
      GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
      APPEND_PATH ${out_rel_path}
      PROTOS ${proto_files}
  )
endfunction()
