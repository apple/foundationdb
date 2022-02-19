set(SRCS
  ${CMAKE_CURRENT_BINARY_DIR}/VersionInfo.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/Program.cs
  Properties/AssemblyInfo.cs)

set(TEST_HARNESS_REFERENCES
  "-r:System,System.Core,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml,System.Runtime.Serialization,${TraceLogHelperDll}")

set(out_file ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/VersionInfo.cs.cmake ${CMAKE_CURRENT_BINARY_DIR}/VersionInfo.cs)

add_custom_command(OUTPUT ${out_file}
  COMMAND ${MCS_EXECUTABLE} ARGS ${TEST_HARNESS_REFERENCES} ${SRCS} "-target:exe" "-out:${out_file}"
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${SRCS} TraceLogHelper
  COMMENT "Compile TestHarness" VERBATIM)
add_custom_target(TestHarness DEPENDS ${out_file})
