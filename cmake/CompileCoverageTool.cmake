set(COVERAGETOOL_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/coveragetool/Program.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/coveragetool/Properties/AssemblyInfo.cs)
if(WIN32)
  add_executable(coveragetool ${COVERAGETOOL_SRCS})
  target_compile_options(coveragetool PRIVATE "/langversion:6")
  set_property(TARGET coveragetool PROPERTY VS_DOTNET_REFERENCES
    "System"
    "System.Core"
    "System.Xml.Linq"
    "System.Data.DataSetExtensions"
    "Microsoft.CSharp"
    "System.Data"
    "System.Xml")
else()
  set(COVERAGETOOL_COMPILER_REFERENCES
    "-r:System,System.Core,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml")

  add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/coveragetool.exe
    COMMAND ${MCS_EXECUTABLE} ARGS ${COVERAGETOOL_COMPILER_REFERENCES} ${COVERAGETOOL_SRCS} "-target:exe" "-out:coveragetool.exe"
    DEPENDS ${COVERAGETOOL_SRCS}
    COMMENT "Compile coveragetool" VERBATIM)
  add_custom_target(coveragetool DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/coveragetool.exe)
  set(coveragetool_exe "${CMAKE_CURRENT_BINARY_DIR}/coveragetool.exe")
endif()
