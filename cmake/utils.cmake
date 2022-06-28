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
    CONFIGURE_DEPENDS "*.cpp" "*.c" "*.h" "*.hpp")
  file(GLOB_RECURSE res_includes
    LIST_DIRECTORIES false
    RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}/include"
    CONFIGURE_DEPENDS "include/*.cpp" "include/*.c" "include/*.h" "include/*.hpp")
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
