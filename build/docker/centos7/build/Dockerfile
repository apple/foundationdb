FROM centos:7

WORKDIR /tmp
COPY mono-project.com.rpmkey.pgp ./
RUN rpmkeys --import mono-project.com.rpmkey.pgp && \
    curl -Ls https://download.mono-project.com/repo/centos7-stable.repo -o /etc/yum.repos.d/mono-centos7-stable.repo && \
    yum repolist && \
    yum install -y \
        centos-release-scl-rh \
        epel-release \
        scl-utils \
        yum-utils && \
    yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo && \
    yum install -y \
        autoconf \
        automake \
        binutils-devel \
        curl \
        debbuild \
        devtoolset-8 \
        devtoolset-8-libasan-devel \
        devtoolset-8-libtsan-devel \
        devtoolset-8-libubsan-devel \
        devtoolset-8-systemtap-sdt-devel \
        docker-ce \
        dos2unix \
        dpkg \
        gettext-devel \
        git \
        golang \
        java-11-openjdk-devel \
        libcurl-devel \
        libuuid-devel \
        libxslt \
        lz4 \
        lz4-devel \
        lz4-static \
        mono-devel \
        redhat-lsb-core \
        rpm-build \
        tcl-devel \
        unzip \
        wget && \
    if [ "$(uname -p)" == "aarch64" ]; then \
        yum install -y \
            rh-python38 \
            rh-python38-python-devel \
            rh-ruby27; \
    else \
        yum install -y \
            rh-python36 \
            rh-python36-python-devel \
            rh-ruby26; \
    fi && \
    yum clean all && \
    rm -rf /var/cache/yum

# build/install git
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://github.com/git/git/archive/v2.30.0.tar.gz -o git.tar.gz && \
    echo "8db4edd1a0a74ebf4b78aed3f9e25c8f2a7db3c00b1aaee94d1e9834fae24e61  git.tar.gz" > git-sha.txt && \
    sha256sum -c git-sha.txt && \
    mkdir git && \
    tar --strip-components 1 --no-same-owner --directory git -xf git.tar.gz && \
    cd git && \
    make configure && \
    ./configure && \
    make && \
    make install && \
    cd ../ && \
    rm -rf /tmp/*

# build/install ninja
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://github.com/ninja-build/ninja/archive/v1.9.0.zip -o ninja.zip && \
    echo "8e2e654a418373f10c22e4cc9bdbe9baeca8527ace8d572e0b421e9d9b85b7ef  ninja.zip" > ninja-sha.txt && \
    sha256sum -c ninja-sha.txt && \
    unzip ninja.zip && \
    cd ninja-1.9.0 && \
    ./configure.py --bootstrap && \
    cp ninja /usr/bin && \
    cd .. && \
    rm -rf /tmp/*

# install cmake
RUN if [ "$(uname -p)" == "aarch64" ]; then \
        curl -Ls https://github.com/Kitware/CMake/releases/download/v3.19.6/cmake-3.19.6-Linux-aarch64.tar.gz -o cmake.tar.gz; \
        echo "69ec045c6993907a4f4a77349d0a0668f1bd3ce8bc5f6fbab6dc7a7e2ffc4f80  cmake.tar.gz" > cmake-sha.txt; \
    else \
        curl -Ls https://github.com/Kitware/CMake/releases/download/v3.13.4/cmake-3.13.4-Linux-x86_64.tar.gz -o cmake.tar.gz; \
        echo "563a39e0a7c7368f81bfa1c3aff8b590a0617cdfe51177ddc808f66cc0866c76  cmake.tar.gz" > cmake-sha.txt; \
    fi && \
    sha256sum -c cmake-sha.txt && \
    mkdir cmake && \
    tar --strip-components 1 --no-same-owner --directory cmake -xf cmake.tar.gz && \
    cp -r cmake/* /usr/local/ && \
    rm -rf /tmp/*

# build/install LLVM
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://github.com/llvm/llvm-project/releases/download/llvmorg-11.0.0/llvm-project-11.0.0.tar.xz -o llvm.tar.xz && \
    echo "b7b639fc675fa1c86dd6d0bc32267be9eb34451748d2efd03f674b773000e92b  llvm.tar.xz" > llvm-sha.txt && \
    sha256sum -c llvm-sha.txt && \
    mkdir llvm-project && \
    tar --strip-components 1 --no-same-owner --directory llvm-project -xf llvm.tar.xz && \
    mkdir -p llvm-project/build && \
    cd llvm-project/build && \
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -G Ninja \
        -DLLVM_INCLUDE_EXAMPLES=OFF \
        -DLLVM_INCLUDE_TESTS=OFF \
        -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;compiler-rt;libcxx;libcxxabi;libunwind;lld;lldb" \
        -DLLVM_STATIC_LINK_CXX_STDLIB=ON \
        ../llvm && \
    cmake --build . && \
    cmake --build . --target install && \
    cd ../.. && \
    rm -rf /tmp/*

# build/install openssl
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://www.openssl.org/source/openssl-1.1.1h.tar.gz -o openssl.tar.gz && \
    echo "5c9ca8774bd7b03e5784f26ae9e9e6d749c9da2438545077e6b3d755a06595d9  openssl.tar.gz" > openssl-sha.txt && \
    sha256sum -c openssl-sha.txt && \
    mkdir openssl && \
    tar --strip-components 1 --no-same-owner --directory openssl -xf openssl.tar.gz && \
    cd openssl && \
    ./config CFLAGS="-fPIC -O3" --prefix=/usr/local && \
    make -j`nproc` && \
    make -j1 install && \
    ln -sv /usr/local/lib64/lib*.so.1.1 /usr/lib64/ && \
    cd .. && \
    rm -rf /tmp/*

# install rocksdb to /opt
RUN curl -Ls https://github.com/facebook/rocksdb/archive/v6.10.1.tar.gz -o rocksdb.tar.gz && \
    echo "d573d2f15cdda883714f7e0bc87b814a8d4a53a82edde558f08f940e905541ee  rocksdb.tar.gz" > rocksdb-sha.txt && \
    sha256sum -c rocksdb-sha.txt && \
    tar --directory /opt -xf rocksdb.tar.gz && \
    rm -rf /tmp/*

# install boost 1.67 to /opt
RUN curl -Ls https://boostorg.jfrog.io/artifactory/main/release/1.67.0/source/boost_1_67_0.tar.bz2 -o boost_1_67_0.tar.bz2 && \
    echo "2684c972994ee57fc5632e03bf044746f6eb45d4920c343937a465fd67a5adba  boost_1_67_0.tar.bz2" > boost-sha-67.txt && \
    sha256sum -c boost-sha-67.txt && \
    tar --no-same-owner --directory /opt -xjf boost_1_67_0.tar.bz2 && \
    rm -rf /opt/boost_1_67_0/libs && \
    rm -rf /tmp/*

# install boost 1.72 to /opt
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.bz2 -o boost_1_72_0.tar.bz2 && \
    echo "59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722  boost_1_72_0.tar.bz2" > boost-sha-72.txt && \
    sha256sum -c boost-sha-72.txt && \
    tar --no-same-owner --directory /opt -xjf boost_1_72_0.tar.bz2 && \
    cd /opt/boost_1_72_0 &&\
    ./bootstrap.sh --with-libraries=context &&\
    ./b2 link=static cxxflags=-std=c++14 --prefix=/opt/boost_1_72_0 install &&\
    rm -rf /opt/boost_1_72_0/libs && \
    rm -rf /tmp/*

# jemalloc (needed for FDB after 6.3)
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2 -o jemalloc-5.2.1.tar.bz2 && \
    echo "34330e5ce276099e2e8950d9335db5a875689a4c6a56751ef3b1d8c537f887f6  jemalloc-5.2.1.tar.bz2" > jemalloc-sha.txt && \
    sha256sum -c jemalloc-sha.txt && \
    mkdir jemalloc && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory jemalloc -xjf jemalloc-5.2.1.tar.bz2 && \
    cd jemalloc && \
    ./configure --enable-static --disable-cxx && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*

# Install CCACHE
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://github.com/ccache/ccache/releases/download/v4.0/ccache-4.0.tar.gz -o ccache.tar.gz && \
    echo "ac97af86679028ebc8555c99318352588ff50f515fc3a7f8ed21a8ad367e3d45  ccache.tar.gz" > ccache-sha256.txt && \
    sha256sum -c ccache-sha256.txt && \
    mkdir ccache &&\
    tar --strip-components 1 --no-same-owner --directory ccache -xf ccache.tar.gz && \
    mkdir build && \
    cd build && \
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DZSTD_FROM_INTERNET=ON ../ccache && \
    cmake --build . --target install && \
    cd .. && \
    rm -rf /tmp/*

# build/install toml
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://github.com/ToruNiina/toml11/archive/v3.4.0.tar.gz -o toml.tar.gz && \
    echo "bc6d733efd9216af8c119d8ac64a805578c79cc82b813e4d1d880ca128bd154d  toml.tar.gz" > toml-sha256.txt && \
    sha256sum -c toml-sha256.txt && \
    mkdir toml && \
    tar --strip-components 1 --no-same-owner --directory toml -xf toml.tar.gz && \
    mkdir build && \
    cd build && \
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -Dtoml11_BUILD_TEST=OFF ../toml && \
    cmake --build . --target install && \
    cd .. && \
    rm -rf /tmp/*

# download old fdbserver binaries
ARG FDB_VERSION="6.2.29"
RUN mkdir -p /opt/foundationdb/old && \
    curl -Ls https://www.foundationdb.org/downloads/misc/fdbservers-${FDB_VERSION}.tar.gz | \
        tar --no-same-owner --directory /opt/foundationdb/old -xz && \
    chmod +x /opt/foundationdb/old/* && \
    ln -sf /opt/foundationdb/old/fdbserver-${FDB_VERSION} /opt/foundationdb/old/fdbserver

# build/install distcc
RUN source /opt/rh/devtoolset-8/enable && \
    if [ "$(uname -p)" == "aarch64" ]; then \
        source /opt/rh/rh-python38/enable; \
    else \
        source /opt/rh/rh-python36/enable; \
    fi && \
    curl -Ls https://github.com/distcc/distcc/archive/v3.3.5.tar.gz -o distcc.tar.gz && \
    echo "13a4b3ce49dfc853a3de550f6ccac583413946b3a2fa778ddf503a9edc8059b0  distcc.tar.gz" > distcc-sha256.txt && \
    sha256sum -c distcc-sha256.txt && \
    mkdir distcc && \
    tar --strip-components 1 --no-same-owner --directory distcc -xf distcc.tar.gz && \
    cd distcc && \
    ./autogen.sh && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*

# valgrind
RUN source /opt/rh/devtoolset-8/enable && \
    curl -Ls https://sourceware.org/pub/valgrind/valgrind-3.17.0.tar.bz2 -o valgrind-3.17.0.tar.bz2 && \
    echo "ad3aec668e813e40f238995f60796d9590eee64a16dff88421430630e69285a2  valgrind-3.17.0.tar.bz2" > valgrind-sha.txt && \
    sha256sum -c valgrind-sha.txt && \
    mkdir valgrind && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory valgrind -xjf valgrind-3.17.0.tar.bz2 && \
    cd valgrind && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*

RUN curl -Ls https://github.com/manticoresoftware/manticoresearch/raw/master/misc/junit/ctest2junit.xsl -o /opt/ctest2junit.xsl
