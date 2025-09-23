if(WIN32)
  # C# is currently only supported on Windows. On other platforms we find mono
  # manually
  enable_language(CSharp)
  return()
endif()

find_package(dotnet 9.0)
if(DOTNET_FOUND)
  set(CSHARP_USE_MONO FALSE)
  return()
endif()

find_package(mono REQUIRED)
set(CSHARP_USE_MONO TRUE)
