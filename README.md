<img alt="FoundationDB logo" src="documentation/FDB_logo.png?raw=true" width="400">

![Build Status](https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiZ1FhRlNwU0JXeHVpZkt0a0k0QlNJK3BEUkplTGVRYnk3azBoT1FOazBQbGlIeDgrYmRJZVhuSUI4RTd3RWJWcjVMT3ZPTzV0NXlCTWpPTGlPVlMzckJJPSIsIml2UGFyYW1ldGVyU3BlYyI6IlB0TWVCM0VYdU5PQWtMUFYiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=master)

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs ACID transactions for all operations. It is especially well-suited for read/write workloads but also has excellent performance for write-intensive workloads. Users interact with the database using API language binding.

To learn more about FoundationDB, visit [foundationdb.org](https://www.foundationdb.org/)

## Documentation

Documentation can be found online at <https://apple.github.io/foundationdb/>. The documentation covers details of API usage, background information on design philosophy, and extensive usage examples. Docs are built from the [source in this repo](documentation/sphinx/source).

## Forums

[The FoundationDB Forums](https://forums.foundationdb.org/) are the home for most of the discussion and communication about the FoundationDB project. We welcome your participation!  We want FoundationDB to be a great project to be a part of and, as part of that, have established a [Code of Conduct](CODE_OF_CONDUCT.md) to establish what constitutes permissible modes of interaction.

## Contributing

Contributing to FoundationDB can be in contributions to the code base, sharing your experience and insights in the community on the Forums, or contributing to projects that make use of FoundationDB. Please see the [contributing guide](CONTRIBUTING.md) for more specifics.

## Getting Started

### Binary downloads

Developers interested in using FoundationDB can get started by downloading and installing a binary package. Please see the [downloads page](https://github.com/apple/foundationdb/releases) for a list of available packages.


### Compiling from source

Developers on an OS for which there is no binary package, or who would like
to start hacking on the code, can get started by compiling from source.

The official docker image for building is [`foundationdb/build`](https://hub.docker.com/r/foundationdb/build) which has all dependencies installed. The Docker image definitions used by FoundationDB team members can be found in the [dedicated repository.](https://github.com/FoundationDB/fdb-build-support).

To build outside the official docker image you'll need at least these dependencies:

1. Install cmake Version 3.13 or higher [CMake](https://cmake.org/)
1. Install [Mono](https://www.mono-project.com/download/stable/)
1. Install [Ninja](https://ninja-build.org/) (optional, but recommended)

If compiling for local development, please set `-DUSE_WERROR=ON` in
cmake. Our CI compiles with `-Werror` on, so this way you'll find out about
compiler warnings that break the build earlier.

Once you have your dependencies, you can run cmake and then build:

1. Check out this repository.
1. Create a build directory (you can have the build directory anywhere you
   like). There is currently a directory in the source tree called build, but you should not use it. See [#3098](https://github.com/apple/foundationdb/issues/3098)
1. `cd <PATH_TO_BUILD_DIRECTORY>`
1. `cmake -G Ninja <PATH_TO_FOUNDATIONDB_DIRECTORY>`
1. `ninja # If this crashes it probably ran out of memory. Try ninja -j1`

### Language Bindings

The language bindings that are supported by cmake will have a corresponding
`README.md` file in the corresponding `bindings/lang` directory.

Generally, cmake will build all language bindings for which it can find all
necessary dependencies. After each successful cmake run, cmake will tell you
which language bindings it is going to build.


### Generating `compile_commands.json`

CMake can build a compilation database for you. However, the default generated
one is not too useful as it operates on the generated files. When running make,
the build system will create another `compile_commands.json` file in the source
directory. This can than be used for tools like
[CCLS](https://github.com/MaskRay/ccls),
[CQuery](https://github.com/cquery-project/cquery), etc. This way you can get
code-completion and code navigation in flow. It is not yet perfect (it will show
a few errors) but we are constantly working on improving the development experience.

CMake will not produce a `compile_commands.json`, you must pass
`-DCMAKE_EXPORT_COMPILE_COMMANDS=ON`.  This also enables the target
`processed_compile_commands`, which rewrites `compile_commands.json` to
describe the actor compiler source file, not the post-processed output files,
and places the output file in the source directory.  This file should then be
picked up automatically by any tooling.

Note that if building inside of the `foundationdb/build` docker
image, the resulting paths will still be incorrect and require manual fixing.
One will wish to re-run `cmake` with `-DCMAKE_EXPORT_COMPILE_COMMANDS=OFF` to
prevent it from reverting the manual changes.

### Using IDEs

CMake has built in support for a number of popular IDEs. However, because flow
files are precompiled with the actor compiler, an IDE will not be very useful as
a user will only be presented with the generated code - which is not what she
wants to edit and get IDE features for.

The good news is, that it is possible to generate project files for editing
flow with a supported IDE. There is a CMake option called `OPEN_FOR_IDE` which
will generate a project which can be opened in an IDE for editing. You won't be
able to build this project, but you will be able to edit the files and get most
edit and navigation features your IDE supports.

For example, if you want to use XCode to make changes to FoundationDB you can
create a XCode-project with the following command:

```sh
cmake -G Xcode -DOPEN_FOR_IDE=ON <FDB_SOURCE_DIRECTORY>
```

You should create a second build-directory which you will use for building and debugging.

#### FreeBSD

1. Check out this repo on your server.
1. Install compile-time dependencies from ports.
1. (Optional) Use tmpfs & ccache for significantly faster repeat builds
1. (Optional) Install a [JDK](https://www.freshports.org/java/openjdk8/)
   for Java Bindings. FoundationDB currently builds with Java 8.
1. Navigate to the directory where you checked out the foundationdb
   repo.
1. Build from source.

    ```shell
    sudo pkg install -r FreeBSD \
        shells/bash devel/cmake devel/ninja devel/ccache  \
        lang/mono lang/python3 \
        devel/boost-libs devel/libeio \
        security/openssl
    mkdir .build && cd .build
    cmake -G Ninja \
        -DUSE_CCACHE=on \
        -DDISABLE_TLS=off \
        -DUSE_DTRACE=off \
        ..
    ninja -j 10
    # run fast tests
    ctest -L fast
    # run all tests
    ctest --output-on-failure -v
    ```


### Linux

There are no special requirements for Linux.  A docker image can be pulled from
`foundationdb/build` that has all of FoundationDB's dependencies
pre-installed, and is what the CI uses to build and test PRs.

```
cmake -G Ninja <FDB_SOURCE_DIR>
ninja
cpack -G DEB
```

For RPM simply replace `DEB` with `RPM`.

### MacOS

The build under MacOS will work the same way as on Linux. To get boost and ninja you can use [Homebrew](https://brew.sh/).

```sh
cmake -G Ninja <PATH_TO_FOUNDATIONDB_SOURCE>
```

To generate a installable package,

```sh
ninja
$SRCDIR/packaging/osx/buildpkg.sh . $SRCDIR
```

### Windows

Under Windows, only Visual Studio with ClangCl is supported

1. Install Visual Studio 2019 (IDE or Build Tools), and enable llvm support
1. Install  [CMake 3.15](https://cmake.org/) or higher
1. Download [Boost 1.77.0](https://boostorg.jfrog.io/artifactory/main/release/1.77.0/source/boost_1_77_0.7z)
1. Unpack boost to C:\boost, or use `-DBOOST_ROOT=<PATH_TO_BOOST>` with `cmake` if unpacked elsewhere
1. Install [Python](https://www.python.org/downloads/) if is not already installed by Visual Studio
1. (Optional) Install [OpenJDK 11](https://developers.redhat.com/products/openjdk/download) to build Java bindings
1. (Optional) Install [OpenSSL 3.x](https://slproweb.com/products/Win32OpenSSL.html) to build with TLS support
1. (Optional) Install [WIX Toolset](https://wixtoolset.org/) to build Windows installer
1. `mkdir build && cd build`
1. `cmake -G "Visual Studio 16 2019" -A x64 -T ClangCl <PATH_TO_FOUNDATIONDB_SOURCE>`
1. `msbuild /p:Configuration=Release foundationdb.sln`
1. To increase build performance, use `/p:UseMultiToolTask=true` and `/p:CL_MPCount=<NUMBER_OF_PARALLEL_JOBS>` 
