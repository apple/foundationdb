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
endif()

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


function(print_components)
  message(STATUS "=============================")
  message(STATUS "   Components Build Overview ")
  message(STATUS "=============================")
  message(STATUS "Build Python Bindings:    ON")
  message(STATUS "Build Java Bindings:      ${BUILD_JAVA}")
  message(STATUS "Build with TLS support:   ${WITH_TLS}")
  message(STATUS "=============================")
endfunction()
