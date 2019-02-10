################################################################################
# LibreSSL
################################################################################

set(DISABLE_TLS OFF CACHE BOOL "Don't try to find LibreSSL and always build without TLS support")
if(DISABLE_TLS)
  set(WITH_TLS OFF)
else()
  set(LIBRESSL_USE_STATIC_LIBS TRUE)
  find_package(LibreSSL)
  if(LibreSSL_FOUND)
    set(WITH_TLS ON)
  else()
    message(STATUS "LibreSSL NOT Found - Will compile without TLS Support")
    message(STATUS "You can set LibreSSL_ROOT to the LibreSSL install directory to help cmake find it")
    set(WITH_TLS OFF)
  endif()
endif()

################################################################################
# Java Bindings
################################################################################

set(BUILD_JAVA OFF)
find_package(JNI 1.8 REQUIRED)
find_package(Java 1.8 COMPONENTS Development)
if(JNI_FOUND AND Java_FOUND AND Java_Development_FOUND)
  set(BUILD_JAVA ON)
  include(UseJava)
  enable_language(Java)
else()
  set(BUILD_JAVA OFF)
endif()

################################################################################
# Python Bindings
################################################################################

find_package(Python COMPONENTS Interpreter)
if(Python_Interpreter_FOUND)
  set(WITH_PYTHON ON)
else()
  set(WITH_PYTHON OFF)
endif()

################################################################################
# Pip
################################################################################

find_package(Virtualenv)
if (Virtualenv_FOUND)
  set(BUILD_DOCUMENTATION ON)
else()
  set(BUILD_DOCUMENTATION OFF)
endif()

################################################################################
# GO
################################################################################

find_program(GO_EXECUTABLE go)
if(GO_EXECUTABLE)
  set(WITH_GO ON)
else()
  set(WITH_GO OFF)
endif()

################################################################################
# Ruby
################################################################################

find_package(Ruby)
set(WITH_RUBY OFF)
if(RUBY_FOUND)
  get_filename_component(ruby_exec_dir ${RUBY_EXECUTABLE} DIRECTORY)
  find_program(GEM_EXECUTABLE gem HINTS ${ruby_exec_dir})
  if(GEM_EXECUTABLE)
    set(GEM_COMMAND ${RUBY_EXECUTABLE} ${GEM_EXECUTABLE})
    set(WITH_RUBY ON)
  endif()
endif()

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/packages)
add_custom_target(packages)


function(print_components)
  message(STATUS "=========================================")
  message(STATUS "   Components Build Overview ")
  message(STATUS "=========================================")
  message(STATUS "Build Java Bindings:                  ${BUILD_JAVA}")
  message(STATUS "Build with TLS support:               ${WITH_TLS}")
  message(STATUS "Build Go bindings:                    ${WITH_GO}")
  message(STATUS "Build Ruby bindings:                  ${WITH_RUBY}")
  message(STATUS "Build Python sdist (make package):    ${WITH_PYTHON}")
  message(STATUS "Build Documentation (make html):      ${BUILD_DOCUMENTATION}")
  message(STATUS "=========================================")
endfunction()
