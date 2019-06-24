include(ExternalProject)
set(flatbuffers_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/flatbuffers)
message(STATUS "CompileFlatbuffers")
message(STATUS "............cmake cur dir: '${CMAKE_CURRENT_BINARY_DIR}'")
message(STATUS "............cmake fb prefix: '${flatbuffers_PREFIX}'")
ExternalProject_add(flatbuffers
    GIT_REPOSITORY "https://github.com/google/flatbuffers"
    # The fix for https://github.com/google/flatbuffers/issues/4741 is not
    # released yet, so I picked a recent commit that passed CI.
    # TODO(anoyes): use the next release when that comes out.
    GIT_TAG 73304367131766aa8dca9c495f75c802bc7991ec
    PREFIX ${flatbuffers_PREFIX}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${flatbuffers_PREFIX}  -DCMAKE_BUILD_TYPE=Release
    )

set(FLATC ${flatbuffers_PREFIX}/bin/flatc)
