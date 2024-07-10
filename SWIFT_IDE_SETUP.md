# FoundationDB/Swift IDE Setup Guide

Swift provides a language server implementation (https://github.com/apple/sourcekit-lsp) that should work with any LSP compatible editor.

In this guide we'll cover how to set up VSCode for local development on a Mac, however other setups should
work in the same way.

## Swift Versions

Note that Swift 5.9 (or higher) is required for the build at the time of writing this guide.
You can download Swift toolchains from [https://www.swift.org/download/](https://www.swift.org/download/), 
or by using the experimental https://github.com/swift-server/swiftly which simplifies this process.

## VSCode + Cross Compilation (MacOS to Linux)

## Host toolchain setup

Download and install latest Swift toolchain: https://www.swift.org/download/

Use the following version:

* **Snapshots / Trunk Development (5.9)**

Note the path of the installed toolchain suffixed with `/usr` as **`FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT`**, e.g.:

```bash
export FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT=/Library/Developer/Toolchains/swift-5.9-DEVELOPMENT-SNAPSHOT-2023-05-01-a.xctoolchain/usr
```


For linker and object file tools:

* Download LLVM toolchain:
  * arm64: https://github.com/llvm/llvm-project/releases/download/llvmorg-15.0.7/clang+llvm-15.0.7-arm64-apple-darwin22.0.tar.xz
    * from: https://github.com/llvm/llvm-project/releases/tag/llvmorg-15.0.7

Note the path of the installed LLVM toolchain + /usr as **`FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT`**, e.g.

```bash
cd ~/Downloads
wget https://github.com/llvm/llvm-project/releases/download/llvmorg-15.0.7/clang+llvm-15.0.7-arm64-apple-darwin22.0.tar.xz
tar xzf clang+llvm-15.0.7-arm64-apple-darwin22.0.tar.xz

export FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT=~/Downloads/clang+llvm-15.0.7-arm64-apple-darwin22.0
```


For actor compiler: Download and install mono:  [https://www.mono-project.com](https://www.mono-project.com/), e.g.

```bash
brew install mono
```

### Host container setup

The Foundation DB will be used on the host as the system SDK/root against which to build.
This is done by using docker and extracting the image

You may need to make sure that you have the latest foundationdb docker image pulled:

```bash
docker pull foundationdb/build:centos7-latest
```

```bash
cd
mkdir fdb-build && cd fdb-build

docker run -ti foundationdb/build:centos7-latest

# and in another terminal session (without terminating centos container):
# $ docker ps
#    CONTAINER ID   IMAGE          COMMAND       CREATED         STATUS         PORTS     NAMES
#    5fe40defb063   07d50e07570b   "/bin/bash"   9 seconds ago   Up 8 seconds             vigilant_gauss
# $ docker export 07d50e07570b -o container-root.tar

mkdir container-root && cd container-root
tar -xzf ../container-root.tar
```

This 

Note that **`FOUNDATIONDB_LINUX_CONTAINER_ROOT`** becomes `~/fdb-build/container-root`.

```bash
export FOUNDATIONDB_LINUX_CONTAINER_ROOT=~/fdb-build/container-root
```

### CMake

Now that you have setup your host, and obtained `FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT` , `FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT`, `FOUNDATIONDB_LINUX_CONTAINER_ROOT` you can run cmake.

You need to ensure you pass in these three flags, and `-C<foundation-db-source>/cmake/toolchain/macos-to-linux.cmake` after the flags.

For example:

```bash
cd ~/fdb-build
mkdir build && cd build

xcrun cmake -G Ninja -DCMAKE_MAKE_PROGRAM=$(xcrun --find ninja)             \
   -DFOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT=$FOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT  \
   -DFOUNDATIONDB_LLVM_TOOLCHAIN_ROOT=$FOUNDATIONDB_LLVM_TOOLCHAIN_ROOT    \
   -DFOUNDATIONDB_LINUX_CONTAINER_ROOT=$FOUNDATIONDB_LINUX_CONTAINER_ROOT  \
   -C$HOME/src/foundationdb/cmake/toolchain/macos-to-linux.cmake           \
   $HOME/src/foundationdb 

# Which would be this with all the parameters substituted:
#
# xcrun cmake -G Ninja -DCMAKE_MAKE_PROGRAM=/opt/homebrew/bin/ninja \
#   -DFOUNDATIONDB_SWIFT_TOOLCHAIN_ROOT=/Library/Developer/Toolchains/swift-5.9-DEVELOPMENT-SNAPSHOT-2023-05-01-a.xctoolchain/usr \
#   -DFOUNDATIONDB_LLVM_TOOLCHAIN_ROOT=$HOME/Downloads/clang1507 \
#   -DFOUNDATIONDB_LINUX_CONTAINER_ROOT=$HOME/fdb-build/container-root \
#   -C$HOME/src/foundationdb/cmake/toolchain/macos-to-linux.cmake \
#   $HOME/src/foundationdb 
```


If you get a warning about not being able to execute `lld` or other binaries from the toolchain because they're not trusted, open `Privacy & Security` and find a button "**Allow Anyway**" and click it once.
[Image: Screenshot 2023-04-17 at 12.47.01.png]

## Prebuild project for IDE

After configuration, make sure things get pre build to be usable in IDE:

```bash
xcrun cmake --build . -- prebuild_for_ide
```

Now you should see that your source directory has a `compile_commands.json` file.

## VSCode setup

Now you can open your source directory in VSCode.

Setup:

* Install the official **Swift plugin**
    * it's the one *maintained by* **Swift Server Work Group**
* For extension settings
    * Update `Swift: Path` to point to the Swift toolchain installed in the first step (e.g. `/Library/Developer/Toolchains/swift-DEVELOPMENT-SNAPSHOT-2023-05-01-a.xctoolchain/usr/bin`)
    * Do not omit the /usr/bin in the path (!)

[Image: Screenshot 2023-03-16 at 7.25.21 PM.png]
* Disable the default "C++" plugin
    * To make sure sourcekit-lsp is used for C++ files.

## Known issues

* jump-to-definition fails to open actor header files.
* jump-to-definition from C++ to Swift does not work.
* Code completion for semantic responses in Swift can be slow sometimes especially in files that import both FDBServer and FDBClient

