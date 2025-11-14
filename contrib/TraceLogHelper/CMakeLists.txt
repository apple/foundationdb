set(TRACELOGHELPER_CSPROJ ${CMAKE_CURRENT_SOURCE_DIR}/TraceLogHelper.csproj)
set(TRACELOGHELPER_SRCS Event.cs JsonParser.cs Properties/AssemblyInfo.cs
                        TraceLogUtil.cs XmlParser.cs)

set(TRACE_LOG_HELPER_REFERENCES
    "-r:System,System.Core,System.Runtime.Serialization,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml"
)

set(out_file ${CMAKE_BINARY_DIR}/packages/bin/TraceLogHelper.dll)

if(CSHARP_USE_MONO)
  add_custom_command(
    OUTPUT ${out_file}
    COMMAND ${CSHARP_COMPILER_EXECUTABLE} ARGS ${TRACE_LOG_HELPER_REFERENCES} ${SRCS}
            "-target:library" "-out:${out_file}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${TRACELOGHELPER_SRCS}
    COMMENT "Compile TraceLogHelper"
    VERBATIM)
  add_custom_target(TraceLogHelper DEPENDS ${out_file})
  set(TraceLogHelperDll "${out_file}")
else()
  dotnet_build(${TRACELOGHELPER_CSPROJ} SOURCES ${TRACELOGHELPER_SRCS})
  set(TraceLogHelperDll "${TraceLogHelper_EXECUTABLE_PATH}")
endif()
