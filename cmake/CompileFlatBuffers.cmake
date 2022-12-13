include(ExternalProject)
set(flatbuffers_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/flatbuffers)

set(FLATC ${flatbuffers_PREFIX}/bin/flatc)

ExternalProject_add(flatbuffers
    GIT_REPOSITORY "https://github.com/google/flatbuffers"
    GIT_TAG v22.12.06
    PREFIX ${flatbuffers_PREFIX}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${flatbuffers_PREFIX}  -DCMAKE_BUILD_TYPE=Release
    BUILD_BYPRODUCTS ${FLATC}
    )

set(FLATBUFFERS_INCLUDE ${flatbuffers_PREFIX}/include)