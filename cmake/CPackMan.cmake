set(CPACKMAN_BINARY_DIR "${CMAKE_BINARY_DIR}")

function(cpackman_provide_dependency)
  if(NOT ARGV0 STREQUAL "FIND_PACKAGE")
    message(FATAL_ERROR "Method ${ARGV0} is not supported by CPackMan")
  endif()
  if(NOT Python3_Interpreter_FOUND)
    find_package(Python3 COMPONENTS Interpreter BYPASS_PROVIDER)
    if(NOT Python3_Interpreter_FOUND)
      message(FATAL_ERROR "CPackMan requires Python3 -- but no interpreter was found")
    endif()
  endif()
  list(JOIN ARGV " " py_args)
  if(USE_LIBCXX OR APPLE)
    set(cxx_stdlib "libc++")
  else()
    set(cxx_stdlib "libstdc++")
  endif()
  execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env
      PYTHONPATH=${CMAKE_SOURCE_DIR}/cmake
      CC=${CMAKE_C_COMPILER}
      CXX=${CMAKE_CXX_COMPILER}
      C_COMPILER_ID=${CMAKE_C_COMPILER_ID}
      C_COMPILER_VERSION=${CMAKE_C_COMPILER_VERSION}
      CXX_COMPILER_ID=${CMAKE_CXX_COMPILER_ID}
      CXX_COMPILER_VERSION=${CMAKE_CXX_COMPILER_VERSION}
      CXX_STDLIB=${}
      --
      "${Python3_EXECUTABLE}" -m cpackman ${ARGV}
    WORKING_DIRECTORY "${CPACKMAN_BINARY_DIR}"
    OUTPUT_VARIABLE cpackman_output)
  cmake_language(EVAL CODE "${cpackman_output}")
endfunction()

cmake_language(SET_DEPENDENCY_PROVIDER cpackman_provide_dependency SUPPORTED_METHODS FIND_PACKAGE)
