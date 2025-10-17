function(fdb_install_packages)
  set(FDB_INSTALL_PACKAGES ${ARGV} PARENT_SCOPE)
endfunction()

function(fdb_install_dirs)
  set(FDB_INSTALL_DIRS ${ARGV} PARENT_SCOPE)
endfunction()

function(install_symlink_impl)
  if (NOT WIN32)
    return()
  endif()
  set(options "")
  set(one_value_options TO DESTINATION)
  set(multi_value_options COMPONENTS)
  cmake_parse_arguments(SYM "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/symlinks)
  get_filename_component(fname ${SYM_DESTINATION} NAME)
  get_filename_component(dest_dir ${SYM_DESTINATION} DIRECTORY)
  set(sl ${CMAKE_CURRENT_BINARY_DIR}/symlinks/${fname})
  execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${SYM_TO} ${sl})
  foreach(component IN LISTS SYM_COMPONENTS)
    install(FILES ${sl} DESTINATION ${dest_dir} COMPONENT ${component})
  endforeach()
endfunction()

function(install_symlink)
  if(NOT WIN32 AND NOT OPEN_FOR_IDE)
    return()
  endif()
  set(options "")
  set(one_value_options COMPONENT LINK_DIR FILE_DIR LINK_NAME FILE_NAME)
  set(multi_value_options "")
  cmake_parse_arguments(IN "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

  set(rel_path "")
  string(REGEX MATCHALL "\\/" slashes "${IN_LINK_NAME}")
  foreach(ignored IN LISTS slashes)
    set(rel_path "../${rel_path}")
  endforeach()
  if("${IN_FILE_DIR}" MATCHES "bin")
    if("${IN_LINK_DIR}" MATCHES "lib")
      install_symlink_impl(
        TO "../${rel_path}bin/${IN_FILE_NAME}"
        DESTINATION "lib/${IN_LINK_NAME}"
        COMPONENTS "${IN_COMPONENT}-tgz")
      install_symlink_impl(
        TO "../${rel_path}bin/${IN_FILE_NAME}"
        DESTINATION "usr/lib64/${IN_LINK_NAME}"
        COMPONENTS
        "${IN_COMPONENT}-el9"
        "${IN_COMPONENT}-deb")
      install_symlink_impl(
        TO "../${rel_path}bin/${IN_FILE_NAME}"
        DESTINATION "usr/lib64/${IN_LINK_NAME}"
        COMPONENTS "${IN_COMPONENT}-deb")
    elseif("${IN_LINK_DIR}" MATCHES "bin")
      install_symlink_impl(
        TO "../${rel_path}bin/${IN_FILE_NAME}"
        DESTINATION "bin/${IN_LINK_NAME}"
        COMPONENTS "${IN_COMPONENT}-tgz")
      install_symlink_impl(
        TO "../${rel_path}bin/${IN_FILE_NAME}"
        DESTINATION "usr/bin/${IN_LINK_NAME}"
        COMPONENTS
        "${IN_COMPONENT}-el9"
        "${IN_COMPONENT}-deb")
    elseif("${IN_LINK_DIR}" MATCHES "fdbmonitor")
      install_symlink_impl(
        TO "../../${rel_path}bin/${IN_FILE_NAME}"
        DESTINATION "lib/foundationdb/${IN_LINK_NAME}"
        COMPONENTS "${IN_COMPONENT}-tgz")
      install_symlink_impl(
        TO "../../${rel_path}bin/${IN_FILE_NAME}"
        DESTINATION "usr/lib/foundationdb/${IN_LINK_NAME}"
        COMPONENTS
        "${IN_COMPONENT}-el9"
        "${IN_COMPONENT}-deb")
    else()
      message(FATAL_ERROR "Unknown LINK_DIR ${IN_LINK_DIR}")
    endif()
  else()
    message(FATAL_ERROR "Unknown FILE_DIR ${IN_FILE_DIR}")
  endif()
endfunction()

function(symlink_files)
  if (NOT WIN32)
    set(options "")
    set(one_value_options LOCATION SOURCE)
    set(multi_value_options TARGETS)
    cmake_parse_arguments(SYM "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

    file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/${SYM_LOCATION})
    foreach(component IN LISTS SYM_TARGETS)
      execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink ${SYM_SOURCE} ${CMAKE_BINARY_DIR}/${SYM_LOCATION}/${component} WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/${SYM_LOCATION})
    endforeach()
  endif()
endfunction()

function(pop_front)
  if(ARGC LESS 2)
    message(FATAL_ERROR "USAGE: pop_front(<list> <out-var> [<count>])")
  endif()
  set(count ${ARGV2})
  if(NOT count)
    set(count 1)
  endif()
  set(result)
  foreach(elem IN LISTS ${ARGV0})
    if(count GREATER 0)
      math(EXPR count "${count} - 1")
    else()
      list(APPEND result ${elem})
    endif()
  endforeach()
  set(${ARGV1} ${result} PARENT_SCOPE)
endfunction()

function(install_destinations)
  if(NOT ARGV0)
    message(FATAL_ERROR "No package passed")
  endif()
  set(package ${ARGV0})
  set(REST_ARGS ${ARGV})
  pop_front(REST_ARGS REST_ARGS)
  list(FIND FDB_INSTALL_PACKAGES ${package} idx)
  if(idx LESS 0)
    message(FATAL_ERROR "Package ${package} does not exist")
  endif()
  cmake_parse_arguments(MY "" "${FDB_INSTALL_DIRS}" "" ${REST_ARGS})
  foreach(dir IN LISTS FDB_INSTALL_DIRS)
    if(MY_${dir})
      set(var ${MY_${dir}})
      set(__install_dest_${package}_${dir} ${MY_${dir}} PARENT_SCOPE)
    endif()
  endforeach()
endfunction()

function(get_install_dest)
  if(ARGC LESS 3)
    message(FATAL_ERROR "USAGE: get_install_dest(<pkg> <dir> <out-var> [<var-name>])")
  endif()
  set(package ${ARGV0})
  set(dir ${ARGV1})
  set(out ${ARGV2})
  set(${out} ${__install_dest_${package}_${dir}} PARENT_SCOPE)
  if(ARGV3)
    set(${ARGV3} "__install_dest_${package}_${dir}")
  endif()
endfunction()

function(print_install_destinations)
  foreach(pkg IN LISTS FDB_INSTALL_PACKAGES)
    message(STATUS "Destinations for ${pkg}")
    set(old_indent ${CMAKE_MESSAGE_INDENT})
    set(CMAKE_MESSAGE_INDENT "${CMAKE_MESSAGE_INDENT}  ")
    foreach(dir IN LISTS FDB_INSTALL_DIRS)
      get_install_dest(${pkg} ${dir} d)
      message(STATUS "${dir} -> ${d}")
    endforeach()
    set(CMAKE_MESSAGE_INDENT ${old_indent})
  endforeach()
endfunction()

function(get_install_var)
  if(NOT ARGC EQUAL 3)
    message(FATAL_ERROR "USAGE: get_install_var(<pkg> <dir> <out-var>)")
  endif()
  set(${ARGV2} "__install_dest_${ARGV0}_${ARGV1}" PARENT_SCOPE)
endfunction()

function(copy_install_destinations)
  if(ARGC LESS 2)
    message(FATAL_ERROR "USAGE: copy_install_destinations(<from> <to> [PREFIX prefix])")
  endif()
  set(from ${ARGV0})
  set(to ${ARGV1})
  set(REST_ARGS ${ARGV})
  pop_front(REST_ARGS REST_ARGS 2)
  cmake_parse_arguments(MY "" "PREFIX" "" ${REST_ARGS})
  foreach(dir IN LISTS FDB_INSTALL_DIRS)
    get_install_dest(${from} ${dir} d)
    get_install_var(${to} ${dir} name)
    if(MY_PREFIX)
      set(d "${MY_PREFIX}${d}")
    endif()
    set(${name} ${d} PARENT_SCOPE)
  endforeach()
endfunction()

function(fdb_configure_and_install)
  if(NOT WIN32 AND NOT OPEN_FOR_IDE)
    set(one_value_options COMPONENT DESTINATION FILE DESTINATION_SUFFIX)
    cmake_parse_arguments(IN "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")
    foreach(pkg IN LISTS FDB_INSTALL_PACKAGES)
      string(TOLOWER "${pkg}" package)
      string(TOUPPER "${IN_DESTINATION}" destination)
      get_install_dest(${pkg} INCLUDE INCLUDE_DIR)
      get_install_dest(${pkg} LIB LIB_DIR)
      get_install_dest(${pkg} ${destination} install_path)
      string(REGEX REPLACE "\.in$" "" name "${IN_FILE}")
      get_filename_component(name "${name}" NAME)
      set(generated_file_name "${generated_dir}/${package}/${name}")
      configure_file("${IN_FILE}" "${generated_file_name}" @ONLY)
      install(
        FILES "${generated_file_name}"
        DESTINATION "${install_path}${IN_DESTINATION_SUFFIX}"
        COMPONENT "${IN_COMPONENT}-${package}")
    endforeach()
  endif()
endfunction()

function(fdb_install)
  if(NOT WIN32 AND NOT OPEN_FOR_IDE)
    set(one_value_options COMPONENT DESTINATION EXPORT DESTINATION_SUFFIX RENAME)
    set(multi_value_options TARGETS FILES PROGRAMS DIRECTORY)
    cmake_parse_arguments(IN "${options}" "${one_value_options}" "${multi_value_options}" "${ARGN}")

    set(install_export 0)
    if(IN_TARGETS)
      set(args TARGETS ${IN_TARGETS})
    elseif(IN_FILES)
      set(args FILES ${IN_FILES})
    elseif(IN_PROGRAMS)
      set(args PROGRAMS ${IN_PROGRAMS})
    elseif(IN_DIRECTORY)
      set(args DIRECTORY ${IN_DIRECTORY})
    elseif(IN_EXPORT)
      set(install_export 1)
    else()
      message(FATAL_ERROR "Expected FILES, PROGRAMS, DIRECTORY, or TARGETS")
    endif()
    string(TOUPPER "${IN_DESTINATION}" destination)
    foreach(pkg IN LISTS FDB_INSTALL_PACKAGES)
      get_install_dest(${pkg} ${destination} install_path)
      string(TOLOWER "${pkg}" package)
      if(install_export)
        if(IN_RENAME)
          message(FATAL_ERROR "RENAME for EXPORT target not implemented")
        endif()
        install(
          EXPORT "${IN_EXPORT}-${package}"
          DESTINATION "${install_path}${IN_DESTINATION_SUFFIX}"
          FILE "${IN_EXPORT}.cmake"
          COMPONENT "${IN_COMPONENT}-${package}")
      else()
        set(export_args "")
        if (IN_EXPORT)
          set(export_args EXPORT "${IN_EXPORT}-${package}")
        endif()
        if(NOT ${install_path} STREQUAL "")
          if(IN_RENAME)
            install(
              ${args}
              ${export_args}
              DESTINATION "${install_path}${IN_DESTINATION_SUFFIX}"
              COMPONENT "${IN_COMPONENT}-${package}"
              RENAME ${IN_RENAME})
          else()
            install(
              ${args}
              ${export_args}
              DESTINATION "${install_path}${IN_DESTINATION_SUFFIX}"
              COMPONENT "${IN_COMPONENT}-${package}")
          endif()
        endif()
      endif()
    endforeach()
  endif()
endfunction()
