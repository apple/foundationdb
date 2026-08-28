<img alt="FoundationDB logo" src="documentation/FDB_logo.png?raw=true" width="400">

![Build Status](https://codebuild.us-west-2.amazonaws.com/badges?uuid=eyJlbmNyeXB0ZWREYXRhIjoiVjVzb1RQNUZTaGxGNm9iUnk4OUZ1d09GdTMzZnVOT1YzaUU1RU1xR2o2TENRWFZjb3ZrTHJEcngrZVdnNE40bXJJVDErOGVwendIL3lFWFY3Y3oxQmdjPSIsIml2UGFyYW1ldGVyU3BlYyI6IlJUbWhnaUlJVXRORUNJTjQiLCJtYXRlcmlhbFNldFNlcmlhbCI6MX0%3D&branch=main)

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs ACID transactions for all operations. It is especially well-suited for read/write workloads, but also has excellent performance for write-intensive workloads. Users interact with the database using API language bindings.

To learn more about FoundationDB, visit [foundationdb.org](https://www.foundationdb.org/)

## Documentation

Documentation can be found online at <https://apple.github.io/foundationdb/>. The documentation covers details of API usage, background information on design philosophy, and extensive usage examples. Docs are built from the [source in this repo](documentation/sphinx/source).

## Forums

[The FoundationDB Forums](https://forums.foundationdb.org/) are the home for most of the discussion and communication about the FoundationDB project. We welcome your participation! We want FoundationDB to be a great project to be a part of, and as part of that, we have established a [Code of Conduct](CODE_OF_CONDUCT.md) to define what constitutes permissible modes of interaction.

## Contributing

Contributions to FoundationDB can include contributions to the codebase, sharing your experience and insights with the community on the Forums, or contributing to projects that make use of FoundationDB. Please see the [contributing guide](CONTRIBUTING.md) for more specifics.

## Getting Started

### Latest Stable Releases

The latest stable releases are versions that are recommended for production use, which have been extensively validated via simulation and real cluster tests and used in our production environment.

| Branch   |     Latest Production Release      |  Notes |
|:--------:|:-------------:|------:|
| 7.3      |  [7.3.77](https://github.com/apple/foundationdb/releases/tag/7.3.77) | Supported |
| 7.2      |   | Experimental |
| 7.1      |  [7.1.57](https://github.com/apple/foundationdb/releases/tag/7.1.57)   |   Bug fixes |
| 7.0      |           |   Experimental |
| 6.3      |  [6.3.25](https://github.com/apple/foundationdb/releases/tag/6.3.25) |   Unsupported |

- `Supported` branches are those we actively maintain and will publish new patch releases.
- `Bug fixes` are branches where we still accept bug fixes, but may not publish newer patch releases. The community can build the latest release binaries if needed and is encouraged to upgrade to the `Supported` branches.
- `Experimental` branches are those used for internal feature testing. They are not recommended for production use.
- `Unsupported` branches are those that will no longer receive any updates.

If you are running on old production releases, we recommend always upgrading to the next major release's latest version, and then continuing to the next major version, e.g., 6.2.X -> 6.3.25 -> 7.1.57 -> 7.3.69. These upgrade paths have been well tested in production (skipping a major release, not marked as `Experimental`, for an upgrade is only tested in simulation).

### Binary Downloads

Developers interested in using FoundationDB can get started by downloading and installing a binary package. Please see the [downloads page](https://github.com/apple/foundationdb/releases) for a list of available packages.


### Compiling from source

Developers on an OS for which there is no binary package, or who would like to start hacking on the code, can get started by compiling from source.

NOTE: FoundationDB has a lot of dependencies.  The Docker container
listed below tracks them and is what we use internally and is the
recommended method of building FDB.

#### Build Using the Official Docker Image

The official Docker image for building is [`foundationdb/build`](https://hub.docker.com/r/foundationdb/build), which includes all necessary dependencies. The Docker image definitions used by FoundationDB team members can be found in the [dedicated repository](https://github.com/FoundationDB/fdb-build-support).

To build FoundationDB with the clang toolchain,

``` bash
mkdir /some/build_output_dir
cd /some/build_output_dir
CC=clang CXX=clang++ LD=lld cmake -D USE_LD=LLD -D USE_LIBCXX=1 -G Ninja /some/fdb/source_dir
ninja
```

To use GCC, a non-default version is necessary. The following modifies environment
variables ($PATH, $LD_LIBRARY_PATH, etc) to pick up the right GCC version:

``` bash
source /opt/rh/gcc-toolset-13/enable
gcc --version  # should say 13
mkdir /some/build_output_dir
cd /some/build_output_dir
cmake -G Ninja /some/fdb/source_dir
ninja
```

Slightly more elaborate compile commands can be found in the shell aliases
defined in `/root/.bashrc` in the container image.

#### Build Locally

To build outside of the official Docker image, you'll need at least these dependencies:

1. [CMake](https://cmake.org/) version 3.24.2 or higher
1. [Mono](https://www.mono-project.com/download/stable/)
1. [ninja](https://ninja-build.org/)

This list is likely to be incomplete. Refer to the rockylinux9
Dockerfile in the `fdb-build-support` repo linked above for reference
material on specific packages and versions that are likely to be
required.

If compiling for local development, please set `-DUSE_WERROR=ON` in CMake. Our CI compiles with `-Werror` on, so this way you'll find out about compiler warnings that break the build earlier.

Once you have your dependencies, you can run `cmake` and then build:

1. Check out this repository.
1. Create a build directory (you can place it anywhere you like).
1. `cd <FDB_BUILD_DIR>`
1. `cmake -G Ninja <FDB_SOURCE_DIR>`
1. `ninja`

Building FoundationDB requires at least 8GB of memory. More memory is needed when building in parallel. If the computer freezes or crashes, consider disabling parallelized build using `ninja -j1`.


#### FreeBSD

1. Check out this repo on your server.

1. Install compile-time dependencies from ports.

1. (Optional) Use tmpfs & ccache for significantly faster repeat builds

1. (Optional) Install a [JDK](https://www.freshports.org/java/openjdk8/) for Java Bindings. FoundationDB currently builds with Java 8.

1. Navigate to the directory where you checked out the FoundationDB repository.

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
       -DUSE_DTRACE=off \
       ..
   ninja -j 10
   # run fast tests
   ctest -L fast
   # run all tests
   ctest --output-on-failure -v
   ```

### macOS

The build under macOS will work the same way as on Linux. [Homebrew](https://brew.sh/) can be used to install the `boost` library and the `ninja` build tool. Be careful: the current main branch uses Boost 1.86; install this version or let CMake download it. Also, if the Swift binding is not of interest, use `-DBUILD_SWIFT_BINDING=OFF`.

```sh
cmake -G Ninja <FDB_SOURCE_DIR> -B <BUILD_DIR>
cd <BUILD_DIR>
ninja
```

To generate an installable package,

```sh
<FDB_SOURCE_DIR>/packaging/osx/buildpkg.sh <BUILD_DIR> <FDB_SOURCE_DIR>
```

### Windows

Under Windows, only Visual Studio with ClangCl is supported.

1. Install Visual Studio 2019 (IDE or Build Tools), and enable LLVM support
1. Install [CMake 3.24.2](https://cmake.org/download/) or higher
1. Download [Boost 1.86.0](https://archives.boost.io/release/1.86.0/source/boost_1_86_0.tar.bz2)
1. Unpack boost to C:\boost, or use `-DBOOST_ROOT=<PATH_TO_BOOST>` with `cmake` if unpacked elsewhere
1. Install [Python](https://www.python.org/downloads/) if it is not already installed by Visual Studio
1. (Optional) Install [OpenJDK 11](https://developers.redhat.com/products/openjdk/download) to build Java bindings
1. (Optional) Install [OpenSSL 3.x](https://slproweb.com/products/Win32OpenSSL.html) to build with TLS support
1. (Optional) Install [WIX Toolset](https://wixtoolset.org/) to build the Windows installer
1. `mkdir build && cd build`
1. `cmake -G "Visual Studio 16 2019" -A x64 -T ClangCl <FDB_SOURCE_DIR>`
1. `msbuild /p:Configuration=Release foundationdb.sln`
1. To increase build performance, use `/p:UseMultiToolTask=true` and `/p:CL_MPCount=<NUMBER_OF_PARALLEL_JOBS>`

### Language Bindings

The language bindings that CMake supports will have a corresponding `README.md` file in the `bindings/lang` directory corresponding to each language.

Generally, CMake will build all language bindings for which it can find all necessary dependencies. After each successful CMake run, CMake will tell you which language bindings it is going to build.


### Generating `compile_commands.json`

CMake can generate a compilation database for code completion, navigation, and static analysis of the C++20 coroutine sources. Pass `-DCMAKE_EXPORT_COMPILE_COMMANDS=ON` when configuring a Ninja or Makefile build, then point your tooling at `compile_commands.json` in the build directory.

When Python support is enabled, this option also enables the `processed_compile_commands` target, which writes a database to the source directory. With Ninja, it includes Swift compilation commands as well.

If the build runs inside a container, the database contains container paths. Run the tooling in the same environment or map those paths to the host checkout.

### Code Formatting and Static Analysis

`clang-format` and `clang-tidy` run as part of CI on every pull request. See the [clang-format](https://apple.github.io/foundationdb/clang-format.html) and [clang-tidy](https://apple.github.io/foundationdb/clang-tidy.html) guides for how to run them locally before pushing.

### Using IDEs

CMake provides built-in support for several popular IDEs. FoundationDB's asynchronous code uses standard C++20 coroutines with the Flow runtime, so use an IDE or language server with C++20 support. See the [coroutine guide](design/coroutines.md) for the programming model.

The CMake option `OPEN_FOR_IDE` creates an editing-only project for a supported IDE. This project cannot be built, but supports editing and navigation.

For example, if you want to use Xcode to make changes to FoundationDB, you can create an Xcode project with the following command:

```sh
cmake -G Xcode -DOPEN_FOR_IDE=ON <FDB_SOURCE_DIRECTORY>
```

A second build directory with the `OPEN_FOR_IDE` flag off can be created for building and debugging purposes.
