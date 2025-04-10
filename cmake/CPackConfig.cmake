if(CPACK_GENERATOR MATCHES "RPM")
  set(CPACK_PACKAGING_INSTALL_PREFIX "/")
  set(CPACK_COMPONENTS_ALL clients-el9 server-el9 clients-versioned server-versioned)
  set(CPACK_RESOURCE_FILE_README ${CMAKE_SOURCE_DIR}/README.md)
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_SOURCE_DIR}/LICENSE)
elseif(CPACK_GENERATOR MATCHES "DEB")
  set(CPACK_PACKAGING_INSTALL_PREFIX "/")
  set(CPACK_COMPONENTS_ALL clients-deb server-deb clients-versioned server-versioned)
  set(CPACK_RESOURCE_FILE_README ${CMAKE_SOURCE_DIR}/README.md)
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_SOURCE_DIR}/LICENSE)
elseif(CPACK_GENERATOR MATCHES "TGZ")
  set(CPACK_STRIP_FILES TRUE)
  set(CPACK_COMPONENTS_ALL clients-tgz server-tgz)
  set(CPACK_RESOURCE_FILE_README ${CMAKE_SOURCE_DIR}/README.md)
  set(CPACK_RESOURCE_FILE_LICENSE ${CMAKE_SOURCE_DIR}/LICENSE)
else()
  message(FATAL_ERROR "Unsupported package format ${CPACK_GENERATOR}")
endif()
