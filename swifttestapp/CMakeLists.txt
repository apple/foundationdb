add_executable(swifttestapp
        main.swift
        future_compat.swift
        )

target_link_libraries(swifttestapp flow)

target_include_directories(swifttestapp PUBLIC
    "${CMAKE_SOURCE_DIR}/flow/include"
    "${CMAKE_BINARY_DIR}/flow/include"
    "${CMAKE_SOURCE_DIR}/contrib/fmt-8.1.1/include"
)

# TODO: We need a platform-agnostic way to find the C++ stdlib searchpaths
target_include_directories(swifttestapp PRIVATE
    "/opt/boost_1_78_0_clang"
    "/opt/rh/devtoolset-8/root/usr/include/c++/8/x86_64-redhat-linux/"
    "/opt/rh/devtoolset-8/root/usr/lib/gcc/x86_64-redhat-linux/8/../../../../include/c++/8"
)

target_link_directories(swifttestapp PRIVATE
  "/opt/rh/devtoolset-8/root/usr/lib/gcc/x86_64-redhat-linux/8"
)

# TODO: the TBD validation skip is because of swift_job_run_generic, though it seems weird why we need to do that?
target_compile_options(swifttestapp PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL:-Xcc -std=c++17 -enable-experimental-cxx-interop -Xfrontend -validate-tbd-against-ir=none> -Xcc -DNO_INTELLISENSE")

# Generate flow modulemap

function(generate_modulemap out module target)
  cmake_parse_arguments(ARG "" "" "OMIT" ${ARGN})

  set(MODULE_NAME "${module}")
  get_target_property(MODULE_HEADERS ${target} HEADER_FILES)
  foreach(header ${MODULE_HEADERS})
    get_filename_component(fname ${header} NAME)
    if(NOT ${fname} IN_LIST ARG_OMIT)
      set(header_list "${header_list}    header \"${header}\"\n")
    endif()
  endforeach()
  set(MODULE_HEADERS "${header_list}")
  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/empty.modulemap" "${out}/module.modulemap" @ONLY)
endfunction()

set(FLOW_BINARY_DIR "${CMAKE_BINARY_DIR}/flow")
generate_modulemap("${CMAKE_BINARY_DIR}/flow/include" "Flow" flow
  OMIT
    sse2neon.h)
