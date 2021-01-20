define_property(TARGET PROPERTY SOURCE_FILES
  BRIEF_DOCS "Source files a flow target is built off"
  FULL_DOCS "When compiling a flow target, this property contains a list of the non-generated source files. \
This property is set by the add_flow_target function")

define_property(TARGET PROPERTY COVERAGE_FILTERS
  BRIEF_DOCS "List of filters for the coverage tool"
  FULL_DOCS "Holds a list of regular expressions. All filenames matching any regular \
expression in this list will be ignored when the coverage.target.xml file is \
generated. This property is set through the add_flow_target function.")


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
  add_custom_target(coverage_${target_name} DEPENDS ${target_file})
  add_dependencies(coverage_${target_name} coveragetool)
  add_dependencies(${target_name} coverage_${target_name})
endfunction()

# This function asserts that `versions.h` does not exist in the source
# directory. It does this in the prebuild phase of the target.
# This is an ugly hack that should make sure that cmake isn't used with
# a source directory in which FDB was previously built with `make`.
function(assert_no_version_h target)

  message(STATUS "Check versions.h on ${target}")
  set(target_name "${target}_versions_h_check")

  if (DEFINED ENV{VERBOSE})
    add_custom_target("${target_name}"
      COMMAND "${CMAKE_COMMAND}" -DFILE="${CMAKE_SOURCE_DIR}/versions.h"
      -P "${CMAKE_SOURCE_DIR}/cmake/AssertFileDoesntExist.cmake"
      COMMAND echo
      "${CMAKE_COMMAND}" -P "${CMAKE_SOURCE_DIR}/cmake/AssertFileDoesntExist.cmake"
      -DFILE="${CMAKE_SOURCE_DIR}/versions.h"
      COMMENT "Check old build system wasn't used in source dir")
  else()
    add_custom_target("${target_name}"
      COMMAND "${CMAKE_COMMAND}" -DFILE="${CMAKE_SOURCE_DIR}/versions.h"
      -P "${CMAKE_SOURCE_DIR}/cmake/AssertFileDoesntExist.cmake"
      COMMENT "Check old build system wasn't used in source dir")
  endif()

  add_dependencies(${target} ${target_name})
endfunction()

add_custom_target(strip_targets)
add_dependencies(packages strip_targets)

function(strip_debug_symbols target)
  if (WIN32)
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
    add_custom_target(strip_${target} DEPENDS  "${out_file}.debug")
  else()
    add_custom_target(strip_${target})
    add_dependencies(strip_${target} strip_only_${target})
  endif()
  add_dependencies(strip_targets strip_${target})
endfunction()

function(add_flow_target)
  set(options EXECUTABLE STATIC_LIBRARY
    DYNAMIC_LIBRARY)
  set(oneValueArgs NAME)
  set(multiValueArgs SRCS COVERAGE_FILTER_OUT DISABLE_ACTOR_WITHOUT_WAIT_WARNING ADDL_SRCS)
  cmake_parse_arguments(AFT "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(NOT AFT_NAME)
    message(FATAL_ERROR "add_flow_target requires option NAME")
  endif()
  if(NOT AFT_SRCS)
    message(FATAL_ERROR "No sources provided")
  endif()
  if(OPEN_FOR_IDE)
    set(sources ${AFT_SRCS} ${AFT_DISABLE_ACTOR_WITHOUT_WAIT_WARNING} ${AFT_ADDL_SRCS})
    add_library(${AFT_NAME} OBJECT ${sources})
  else()
    foreach(src IN LISTS AFT_SRCS AFT_DISABLE_ACTOR_WITHOUT_WAIT_WARNING)
      set(actor_compiler_flags "")
      if(${src} MATCHES ".*\\.actor\\.(h|cpp)")
        list(APPEND actors ${src})
        list(APPEND actor_compiler_flags "--generate-probes")
        if(${src} MATCHES ".*\\.h")
          string(REPLACE ".actor.h" ".actor.g.h" generated ${src})
        else()
          string(REPLACE ".actor.cpp" ".actor.g.cpp" generated ${src})
        endif()
        foreach(s IN LISTS AFT_DISABLE_ACTOR_WITHOUT_WAIT_WARNING)
          if("${s}" STREQUAL "${src}")
            list(APPEND actor_compiler_flags "--disable-actor-without-wait-warning")
            break()
          endif()
        endforeach()
        list(APPEND sources ${generated})
        list(APPEND generated_files ${CMAKE_CURRENT_BINARY_DIR}/${generated})
        if(WIN32)
          add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${generated}"
            COMMAND $<TARGET_FILE:actorcompiler> "${CMAKE_CURRENT_SOURCE_DIR}/${src}" "${CMAKE_CURRENT_BINARY_DIR}/${generated}" ${actor_compiler_flags}
            DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${src}" ${actor_exe}
            COMMENT "Compile actor: ${src}")
        else()
          add_custom_command(OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${generated}"
            COMMAND ${MONO_EXECUTABLE} ${actor_exe} "${CMAKE_CURRENT_SOURCE_DIR}/${src}" "${CMAKE_CURRENT_BINARY_DIR}/${generated}" ${actor_compiler_flags} > /dev/null
            DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/${src}" ${actor_exe}
            COMMENT "Compile actor: ${src}")
        endif()
      else()
        list(APPEND sources ${src})
      endif()
    endforeach()
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

    foreach(src IN LISTS sources AFT_ADDL_SRCS)
      get_filename_component(dname ${CMAKE_CURRENT_SOURCE_DIR} NAME)
      string(REGEX REPLACE "\\..*" "" fname ${src})
      string(REPLACE / _ fname ${fname})
      #set_source_files_properties(${src} PROPERTIES COMPILE_DEFINITIONS FNAME=${dname}_${fname})
    endforeach()

    set_property(TARGET ${AFT_NAME} PROPERTY SOURCE_FILES ${AFT_SRCS})
    set_property(TARGET ${AFT_NAME} PROPERTY COVERAGE_FILTERS ${AFT_SRCS})

    add_custom_target(${AFT_NAME}_actors DEPENDS ${generated_files})
    add_dependencies(${AFT_NAME}_actors actorcompiler)
    add_dependencies(${AFT_NAME} ${AFT_NAME}_actors)
    if(NOT WIN32)
      assert_no_version_h(${AFT_NAME}_actors)
    endif()
    generate_coverage_xml(${AFT_NAME})
    if(strip_target)
      strip_debug_symbols(${AFT_NAME})
    endif()
  endif()
  target_include_directories(${AFT_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
endfunction()
