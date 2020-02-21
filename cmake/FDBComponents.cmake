set(FORCE_ALL_COMPONENTS OFF CACHE BOOL "Fails cmake if not all dependencies are found")

################################################################################
# Valgrind
################################################################################

if(USE_VALGRIND)
  find_package(Valgrind REQUIRED)
endif()

################################################################################
# SSL
################################################################################

set(DISABLE_TLS OFF CACHE BOOL "Don't try to find LibreSSL and always build without TLS support")
if(DISABLE_TLS)
  set(WITH_TLS OFF)
else()
  set(OPENSSL_USE_STATIC_LIBS TRUE)
  find_package(OpenSSL)
  if(NOT OPENSSL_FOUND)
    set(LIBRESSL_USE_STATIC_LIBS TRUE)
    find_package(LibreSSL)
		if (LIBRESSL_FOUND)
			add_library(OpenSSL::SSL ALIAS LibreSSL)
		endif()
  endif()
	if(OPENSSL_FOUND OR LIBRESSL_FOUND)
    set(WITH_TLS ON)
    add_compile_options(-DHAVE_OPENSSL)
  else()
    message(STATUS "Neither OpenSSL nor LibreSSL were found - Will compile without TLS Support")
    message(STATUS "You can set OPENSSL_ROOT_DIR or LibreSSL_ROOT to the LibreSSL install directory to help cmake find it")
    set(WITH_TLS OFF)
  endif()
  if(WIN32)
    message(STATUS "TLS is temporarilty disabled on macOS while libressl -> openssl transition happens")
    set(WITH_TLS OFF)
  endif()
endif()

################################################################################
# Java Bindings
################################################################################

set(WITH_JAVA OFF)
find_package(JNI 1.8)
find_package(Java 1.8 COMPONENTS Development)
if(JNI_FOUND AND Java_FOUND AND Java_Development_FOUND)
  set(WITH_JAVA ON)
  include(UseJava)
  enable_language(Java)
else()
  set(WITH_JAVA OFF)
endif()

################################################################################
# Python Bindings
################################################################################

find_package(Python COMPONENTS Interpreter)
if(Python_Interpreter_FOUND)
  set(WITH_PYTHON ON)
else()
  #message(FATAL_ERROR "Could not found a suitable python interpreter")
  set(WITH_PYTHON OFF)
endif()

################################################################################
# Pip
################################################################################

find_package(Virtualenv)
if (Virtualenv_FOUND)
  set(WITH_DOCUMENTATION ON)
else()
  set(WITH_DOCUMENTATION OFF)
endif()

################################################################################
# GO
################################################################################

find_program(GO_EXECUTABLE go)
# building the go binaries is currently not supported on Windows
if(GO_EXECUTABLE AND NOT WIN32)
  set(WITH_GO ON)
else()
  set(WITH_GO OFF)
endif()

################################################################################
# Ruby
################################################################################

find_program(GEM_EXECUTABLE gem)
set(WITH_RUBY OFF)
if(GEM_EXECUTABLE)
  set(GEM_COMMAND ${RUBY_EXECUTABLE} ${GEM_EXECUTABLE})
  set(WITH_RUBY ON)
endif()

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/packages)
add_custom_target(packages)

function(print_components)
  message(STATUS "=========================================")
  message(STATUS "   Components Build Overview ")
  message(STATUS "=========================================")
  message(STATUS "Build Java Bindings:                  ${WITH_JAVA}")
  message(STATUS "Build with TLS support:               ${WITH_TLS}")
  message(STATUS "Build Go bindings:                    ${WITH_GO}")
  message(STATUS "Build Ruby bindings:                  ${WITH_RUBY}")
  message(STATUS "Build Python sdist (make package):    ${WITH_PYTHON}")
  message(STATUS "Build Documentation (make html):      ${WITH_DOCUMENTATION}")
  message(STATUS "Build Bindings (depends on Python):   ${WITH_PYTHON}")
  message(STATUS "Configure CTest (depends on Python):  ${WITH_PYTHON}")
  message(STATUS "=========================================")
endfunction()

if(FORCE_ALL_COMPONENTS)
  if(NOT WITH_JAVA OR NOT WITH_TLS OR NOT WITH_GO OR NOT WITH_RUBY OR NOT WITH_PYTHON OR NOT WITH_DOCUMENTATION)
    print_components()
    message(FATAL_ERROR "FORCE_ALL_COMPONENTS is set but not all dependencies could be found")
  endif()
endif()
