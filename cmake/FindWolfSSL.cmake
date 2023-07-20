# FindWolfSSL

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES
if(WOLFSSL_USE_STATIC_LIBS)
  if(WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
  else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
  endif()
endif()

find_path(WOLFSSL_ROOT_DIR
  NAMES
    include/wolfssl/options.h
)

find_path(WOLFSSL_INCLUDE_DIR
  NAMES
    wolfssl/ssl.h
  PATHS
   ${WOLFSSL_ROOT_DIR}/include
)

find_library(WOLFSSL_LIBRARY
  NAMES 
    wolfssl
  PATHS
   ${WOLFSSL_ROOT_DIR}/lib
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(WolfSSL
  REQUIRED_VARS
    WOLFSSL_LIBRARY
    WOLFSSL_INCLUDE_DIR
  FAIL_MESSAGE
    "Could NOT find WolfSSL"
)

mark_as_advanced(
  WOLFSSL_ROOT_DIR
  WOLFSSL_LIBRARY
  WOLFSSL_INCLUDE_DIR
)

if(WOLFSSL_FOUND)
  message(STATUS "Found wolfssl library: ${WOLFSSL_LIBRARY}")
  message(STATUS "Found wolfssl includes: ${WOLFSSL_INCLUDE_DIR}")

  set(WOLFSSL_INCLUDE_DIRS ${WOLFSSL_INCLUDE_DIR})
  set(WOLFSSL_LIBRARIES ${WOLFSSL_LIBRARY})

  add_library(WolfSSL UNKNOWN IMPORTED GLOBAL)
  add_library(OpenSSL::SSL ALIAS WolfSSL)
  add_library(OpenSSL::CRYPTO ALIAS WolfSSL)

  target_include_directories(WolfSSL INTERFACE "${WOLFSSL_INCLUDE_DIR}")
  target_link_libraries(WolfSSL INTERFACE "${WOLFSSL_TLS_LIBRARY}" "${WOLFSSL_SSL_LIBRARY}" "${WOLFSSL_CRYPTO_LIBRARY}")
  set_target_properties(WolfSSL PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${WOLFSSL_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${WOLFSSL_LIBRARY}")
endif()
