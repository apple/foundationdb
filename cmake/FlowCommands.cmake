define_property(TARGET PROPERTY SOURCE_FILES
  BRIEF_DOCS "Source files a flow target is built off"
  FULL_DOCS "When compiling a flow target, this property contains a list of the non-generated source files. \
This property is set by the add_flow_target function")

define_property(TARGET PROPERTY COVERAGE_FILTERS
  BRIEF_DOCS "List of filters for the coverage tool"
  FULL_DOCS "Holds a list of regular expressions. All filenames matching any regular \
expression in this list will be ignored when the coverage.target.xml file is \
generated. This property is set through the add_flow_target function.")

if(WIN32)
  set(compilation_unit_macro_default OFF)
else()
  set(compilation_unit_macro_default ON)
endif()

set(PASS_COMPILATION_UNIT "${compilation_unit_macro_default}" CACHE BOOL
  "Pass path to compilation unit as macro to each compilation unit (useful for code probes)")

function(generate_coverage_xml)
  if(NOT (${ARGC} EQUAL "1"))
    message(FATAL_ERROR "generate_coverage_xml expects one argument")
  endif()
  set(target_name ${ARGV0})
  get_target_property(sources ${target_name} SOURCE_FILES)
  get_target_property(filters ${target_name} COVERAGE_FILTER_OUT)
  foreach(src IN LISTS sources)
    set(include TRUE)
    foreach(f IN LISTS filters)
      if("${f}" MATCHES "${src}")
        set(include FALSE)
      endif()
    endforeach()
    if(include)
      list(APPEND in_files ${src})
    endif()
  endforeach()
  set(target_file ${CMAKE_CURRENT_SOURCE_DIR}/coverage_target_${target_name})
  # we can't get the targets output dir through a generator expression as this would
  # create a cyclic dependency.
  # Instead we follow the following rules:
  # - For executable we place the coverage file into the directory EXECUTABLE_OUTPUT_PATH
  # - For static libraries we place it into the directory LIBRARY_OUTPUT_PATH
  # - For dynamic libraries we place it into LIBRARY_OUTPUT_PATH on Linux and MACOS
  #   and to EXECUTABLE_OUTPUT_PATH on Windows
  get_target_property(type ${target_name} TYPE)
  # STATIC_LIBRARY, MODULE_LIBRARY, SHARED_LIBRARY, OBJECT_LIBRARY, INTERFACE_LIBRARY, EXECUTABLE
  if(type STREQUAL "STATIC_LIBRARY")
    set(target_file ${LIBRARY_OUTPUT_PATH}/coverage.${target_name}.xml)
  elseif(type STREQUAL "SHARED_LIBRARY")
    if(WIN32)
      set(target_file ${EXECUTABLE_OUTPUT_PATH}/coverage.${target_name}.xml)
    else()
      set(target_file ${LIBRARY_OUTPUT_PATH}/coverage.${target_name}.xml)
    endif()
  elseif(type STREQUAL "EXECUTABLE")
    set(target_file ${EXECUTABLE_OUTPUT_PATH}/coverage.${target_name}.xml)
  endif()
  if(WIN32)
    add_custom_command(
      OUTPUT ${target_file}
      COMMAND $<TARGET_FILE:coveragetool> ${target_file} ${in_files}
      DEPENDS ${in_files}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Generate coverage xml")
  else()
    add_custom_command(
      OUTPUT ${target_file}
      COMMAND ${MONO_EXECUTABLE} ${coveragetool_exe} ${target_file} ${in_files}
      DEPENDS ${in_files}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Generate coverage xml")
  endif()
  add_custom_target(coverage_${target_name} ALL DEPENDS ${target_file})
  add_dependencies(coverage_${target_name} coveragetool)
endfunction()

add_custom_target(strip_targets)
add_dependencies(packages strip_targets)

function(strip_debug_symbols target)
  if(WIN32)
    return()
  endif()
  get_target_property(target_type ${target} TYPE)
  if(target_type STREQUAL "EXECUTABLE")
    set(path ${CMAKE_BINARY_DIR}/packages/bin)
    set(is_exec ON)
  else()
    set(path ${CMAKE_BINARY_DIR}/packages/lib)
  endif()
  file(MAKE_DIRECTORY "${path}")
  set(strip_command strip)
  set(out_name ${target})
  if(APPLE)
    if(NOT is_exec)
      set(out_name "lib${target}.dylib")
      list(APPEND strip_command -S -x)
    endif()
  else()
    if(is_exec)
      list(APPEND strip_command --strip-debug --strip-unneeded)
    else()
      set(out_name "lib${target}.so")
      list(APPEND strip_command --strip-all)
    endif()
  endif()
  set(out_file "${path}/${out_name}")
  list(APPEND strip_command -o "${out_file}")
  add_custom_command(OUTPUT "${out_file}"
    COMMAND ${strip_command} $<TARGET_FILE:${target}>
    DEPENDS ${target}
    COMMENT "Stripping symbols from ${target}")
  add_custom_target(strip_only_${target} DEPENDS ${out_file})
  if(is_exec AND NOT APPLE)
    add_custom_command(OUTPUT "${out_file}.debug"
      DEPENDS strip_only_${target}
      COMMAND objcopy --verbose --only-keep-debug $<TARGET_FILE:${target}> "${out_file}.debug"
      COMMAND objcopy --verbose --add-gnu-debuglink="${out_file}.debug" "${out_file}"
      COMMENT "Copy debug symbols to ${out_name}.debug")
    add_custom_target(strip_${target} DEPENDS "${out_file}.debug")
  else()
    add_custom_target(strip_${target})
    add_dependencies(strip_${target} strip_only_${target})
  endif()
  add_dependencies(strip_targets strip_${target})
endfunction()

# This will copy the header from a flow target into ${CMAKE_BINARY_DIR}/include/target-name
# We're doing this to enforce proper dependencies. In the past we simply added the source
# and binary dir to the include list, which means that for example a compilation unit in
# flow could include a header file that lives in fdbserver. This is a somewhat hacky solution
# but due to our directory structure it seems to be the least invasive one.
function(copy_headers)
  set(options)
  set(oneValueArgs NAME OUT_DIR INC_DIR)
  set(multiValueArgs SRCS)
  cmake_parse_arguments(CP "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  get_filename_component(dir_name ${CMAKE_CURRENT_SOURCE_DIR} NAME)
  set(include_dir "${CMAKE_CURRENT_BINARY_DIR}/include")
  set(incl_dir "${include_dir}/${dir_name}")
  make_directory("${incl_dir}")
  foreach(f IN LISTS CP_SRCS)
    is_prefix(bd "${CMAKE_CURRENT_BINARY_DIR}" "${f}")
    is_prefix(sd "${CMAKE_CURRENT_SOURCE_DIR}" "${f}")
    if(bd OR sd)
      continue()
    endif()
    is_header(hdr "${f}")
    if(NOT hdr)
      continue()
    endif()
    get_filename_component(fname ${f} NAME)
    get_filename_component(dname ${f} DIRECTORY)
    if(dname)
      make_directory(${incl_dir}/${dname})
    endif()
    set(fpath "${incl_dir}/${dname}/${fname}")
    add_custom_command(OUTPUT "${fpath}"
      DEPENDS "${f}"
      COMMAND "${CMAKE_COMMAND}" -E copy "${f}" "${fpath}"
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
    list(APPEND out_files "${fpath}")
  endforeach()
  add_custom_target("${CP_NAME}_incl" DEPENDS ${out_files})
  set("${CP_OUT_DIR}" "${incl_dir}" PARENT_SCOPE)
  set("${CP_INC_DIR}" ${include_dir} PARENT_SCOPE)
endfunction()

function(add_flow_target)
  set(options EXECUTABLE STATIC_LIBRARY
    DYNAMIC_LIBRARY LINK_TEST)
  set(oneValueArgs NAME)
  set(multiValueArgs SRCS COVERAGE_FILTER_OUT DISABLE_ACTOR_DIAGNOSTICS ADDL_SRCS)
  cmake_parse_arguments(AFT "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(NOT AFT_NAME)
    message(FATAL_ERROR "add_flow_target requires option NAME")
  endif()
  if(NOT AFT_SRCS)
    message(FATAL_ERROR "No sources provided")
  endif()
  #foreach(src IN LISTS AFT_SRCS)
  #  is_header(h "${src}")
  #  if(NOT h)
  #    list(SRCS "${CMAKE_CURRENT_SOURCE_DIR}/${src}")
  #  endif()
  #endforeach()
  if(OPEN_FOR_IDE)
    # Intentionally omit ${AFT_DISABLE_ACTOR_DIAGNOSTICS} since we don't want diagnostics
    set(sources ${AFT_SRCS} ${AFT_ADDL_SRCS})
    add_library(${AFT_NAME} OBJECT ${sources})
  else()
    create_build_dirs(${AFT_SRCS} ${AFT_DISABLE_ACTOR_DIAGNOSTICS})
    foreach(src IN LISTS AFT_SRCS AFT_DISABLE_ACTOR_DIAGNOSTICS)
      is_header(hdr ${src})
      set(in_filename "${src}")
      if(${src} MATCHES ".*\\.actor\\.(h|cpp)")
        set(is_actor_file YES)
        if(${src} MATCHES ".*\\.h")
          string(REPLACE ".actor.h" ".actor.g.h" out_filename ${in_filename})
        else()
          string(REPLACE ".actor.cpp" ".actor.g.cpp" out_filename ${in_filename})
        endif()
      else()
        set(is_actor_file NO)
        set(out_filename "${src}")
      endif()

      set(in_file "${CMAKE_CURRENT_SOURCE_DIR}/${in_filename}")
      if(is_actor_file)
        if(hdr)
          list(APPEND HEADER_LIST ${in_file})
        endif()
        set(out_file "${CMAKE_CURRENT_BINARY_DIR}/${out_filename}")
      else()
        set(out_file "${in_file}")
      endif()

      if(hdr)
        list(APPEND HEADER_LIST ${out_file})
      endif()

      list(APPEND sources ${out_file})
      set(actor_compiler_flags "")
      if(is_actor_file)
        list(APPEND actors ${in_file})
        list(APPEND actor_compiler_flags "--generate-probes")
        foreach(s IN LISTS AFT_DISABLE_ACTOR_DIAGNOSTICS)
          if("${s}" STREQUAL "${src}")
            list(APPEND actor_compiler_flags "--disable-diagnostics")
            break()
          endif()
        endforeach()

        list(APPEND generated_files ${out_file})
        if(WIN32)
          add_custom_command(OUTPUT "${out_file}"
            COMMAND $<TARGET_FILE:actorcompiler> "${in_file}" "${out_file}" ${actor_compiler_flags}
            DEPENDS "${in_file}" ${actor_exe}
            COMMENT "Compile actor: ${src}")
        else()
          add_custom_command(OUTPUT "${out_file}"
            COMMAND ${MONO_EXECUTABLE} ${actor_exe} "${in_file}" "${out_file}" ${actor_compiler_flags} > /dev/null
            DEPENDS "${in_file}" ${actor_exe}
            COMMENT "Compile actor: ${src}")
        endif()
      endif()
    endforeach()
    if(PASS_COMPILATION_UNIT)
      foreach(s IN LISTS sources)
        set_source_files_properties("${s}" PROPERTIES COMPILE_DEFINITIONS "COMPILATION_UNIT=${s}")
      endforeach()
    endif()
    if(AFT_EXECUTABLE)
      set(strip_target ON)
      set(target_type exec)
      add_executable(${AFT_NAME} ${sources} ${AFT_ADDL_SRCS})
    endif()
    if(AFT_STATIC_LIBRARY)
      if(target_type)
        message(FATAL_ERROR "add_flow_target can only be of one type")
      endif()
      add_library(${AFT_NAME} STATIC ${sources} ${AFT_ADDL_SRCS})
    endif()
    if(AFT_DYNAMIC_LIBRARY)
      if(target_type)
        message(FATAL_ERROR "add_flow_target can only be of one type")
      endif()
      set(strip_target ON)
      add_library(${AFT_NAME} DYNAMIC ${sources} ${AFT_ADDL_SRCS})
    endif()
    if(AFT_LINK_TEST)
      set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/linktest)
      set(strip_target ON)
      set(target_type exec)
      add_executable(${AFT_NAME} ${sources} ${AFT_ADDL_SRCS})
    endif()

    foreach(src IN LISTS sources AFT_ADDL_SRCS)
      get_filename_component(dname ${CMAKE_CURRENT_SOURCE_DIR} NAME)
      string(REGEX REPLACE "\\..*" "" fname ${src})
      string(REPLACE / _ fname ${fname})
      #set_source_files_properties(${src} PROPERTIES COMPILE_DEFINITIONS FNAME=${dname}_${fname})
    endforeach()

    set_property(TARGET ${AFT_NAME} PROPERTY SOURCE_FILES ${AFT_SRCS})
    set_property(TARGET ${AFT_NAME} PROPERTY HEADER_FILES ${HEADER_LIST})
    set_property(TARGET ${AFT_NAME} PROPERTY COVERAGE_FILTERS ${AFT_SRCS})

    add_custom_target(${AFT_NAME}_actors DEPENDS ${generated_files})
    add_dependencies(${AFT_NAME} ${AFT_NAME}_actors)
    generate_coverage_xml(${AFT_NAME})
    if(strip_target)
      strip_debug_symbols(${AFT_NAME})
    endif()
  endif()
endfunction()
