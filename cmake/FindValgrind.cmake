find_path(VALGRIND_INCLUDE_DIR
  NAMES
  valgrind.h
  PATH_SUFFIXES include valgrind)

find_package_handle_standard_args(Valgrind
  REQUIRED_VARS VALGRIND_INCLUDE_DIR
  FAIL_MESSAGE "Could not find Valgrind header files, try set the path to the Valgrind headers in the variable Valgrind_ROOT")

if(VALGRIND_FOUND)
  add_library(Valgrind INTERFACE)
  target_include_directories(Valgrind INTERFACE "${VALGRIND_INCLUDE_DIR}")
endif()
