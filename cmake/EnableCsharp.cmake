if(WIN32)
  # C# is currently only supported on Windows. On other platforms we find mono
  # manually
  enable_language(CSharp)
  return()
endif()

find_package(dotnet 9.0)
if(dotnet_FOUND)
  set(CSHARP_USE_MONO FALSE)
  return()
endif()

find_package(mono REQUIRED)
if(mono_FOUND)
  set(CSHARP_USE_MONO TRUE)
  return()
endif()

message(FATAL_ERROR "Unable to find dotnet or mono as C# compiler")
