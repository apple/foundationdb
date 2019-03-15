# FindLibreSSL

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES
if(LIBRESSL_USE_STATIC_LIBS)
  set(_libressl_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  if(WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
  else()
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a )
  endif()
endif()

set(_LibreSSL_HINTS "")
if(WIN32)
  set(_LibreSSL_HINTS "C:\\Program Files\\LibreSSL")
endif()

find_path(LIBRESSL_INCLUDE_DIR
  NAMES
    tls.h
  PATH_SUFFIXES
    include
   HINTS
    "${_LibreSSL_HINTS}"
)

find_library(LIBRESSL_CRYPTO_LIBRARY
  NAMES crypto
  NAMES_PER_DIR ${_LIBRESSL_HINTS_AND_PATHS}
  PATH_SUFFIXES lib
  HINTS "${_LibreSSL_HINTS}")

find_library(LIBRESSL_SSL_LIBRARY
  NAMES ssl
  PATH_SUFFIXES lib
  HINTS  "${_LibreSSL_HINTS}")

find_library(LIBRESSL_TLS_LIBRARY
  NAMES tls
  PATH_SUFFIXES lib
  HINTS  "${_LibreSSL_HINTS}")

mark_as_advanced(LIBRESSL_CRYPTO_LIBRARY LIBRESSL_SSL_LIBRARY LIBRESSL_TLS_LIBRARY)

find_package_handle_standard_args(LibreSSL
  REQUIRED_VARS
    LIBRESSL_CRYPTO_LIBRARY
    LIBRESSL_SSL_LIBRARY
    LIBRESSL_TLS_LIBRARY
    LIBRESSL_INCLUDE_DIR
  FAIL_MESSAGE
    "Could NOT find LibreSSL, try to set the path to LibreSSL root folder in the system variable LibreSSL_ROOT"
)

if(LIBRESSL_FOUND)
  add_library(LibreSSL INTERFACE)
  target_include_directories(LibreSSL INTERFACE "${LIBRESSL_INCLUDE_DIR}")
  # in theory we could make those components. However there are good reasons not to do that:
  # 1. FDB links against all of them anyways
  # 2. The order in which we link them is important and the dependency graph would become kind of complex...
  # So if this module should ever be reused to allow to only link against some of the libraries, this
  # should probably be rewritten
  target_link_libraries(LibreSSL INTERFACE "${LIBRESSL_TLS_LIBRARY}" "${LIBRESSL_SSL_LIBRARY}" "${LIBRESSL_CRYPTO_LIBRARY}")
endif()

if(LIBRESSL_USE_STATIC_LIBS)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_libressl_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
endif()
