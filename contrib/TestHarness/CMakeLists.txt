set(SRCS
  Program.cs
  Properties/AssemblyInfo.cs)

set(TEST_HARNESS_REFERENCES
  "-r:System,System.Core,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml,${TraceLogHelperDll}")

set(out_file ${CMAKE_BINARY_DIR}/packages/bin/TestHarness.exe)

add_custom_command(OUTPUT ${out_file}
  COMMAND ${MCS_EXECUTABLE} ARGS ${TEST_HARNESS_REFERENCES} ${SRCS} "-target:exe" "-out:${out_file}"
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${SRCS}
  COMMENT "Compile TestHarness" VERBATIM)
add_custom_target(TestHarness DEPENDS ${out_file})
add_dependencies(TestHarness TraceLogHelper)
set(TestHarnesExe "${out_file}" PARENT_SCOPE)
