FROM centos:7 as build

ARG DEVTOOLSET_VERSION=11

WORKDIR /tmp
# skip installing elrepo-kernel on aarch64 -------
RUN if [ ! "$(uname -m)" == "aarch64" ]; then \
        rpm --import https://www.elrepo.org/RPM-GPG-KEY-elrepo.org && \
        yum -y install https://www.elrepo.org/elrepo-release-7.el7.elrepo.noarch.rpm && \
        yum -y --enablerepo=elrepo-kernel install kernel-lt{,-devel,-headers}; \
    fi && \
    rpm --import https://download.mono-project.com/repo/xamarin.gpg && \
    curl -Ls https://download.mono-project.com/repo/centos7-stable.repo | tee /etc/yum.repos.d/mono-centos7-stable.repo && \
    yum repolist && \
    yum install -y \
        centos-release-scl-rh \
        epel-release \
        scl-utils \
        yum-utils && \
    yum install -y \
        autoconf \
        automake \
        bin-utils \
        binutils-devel \
        curl \
        debbuild \
        devtoolset-${DEVTOOLSET_VERSION} \
        devtoolset-${DEVTOOLSET_VERSION}-libasan-devel \
        devtoolset-${DEVTOOLSET_VERSION}-libatomic-devel \
        devtoolset-${DEVTOOLSET_VERSION}-libtsan-devel \
        devtoolset-${DEVTOOLSET_VERSION}-libubsan-devel \
        devtoolset-${DEVTOOLSET_VERSION}-systemtap-sdt-devel \
        dos2unix \
        dpkg \
        gettext-devel \
        gperftools \
        gperftools-devel \
        gperftools-libs \
        ghostscript \
        glibc-static \
        gv \
        iptables \
        java-11-openjdk-devel \
        libbsd-devel \
        libcurl-devel \
        libedit \
        libedit-devel \
        libicu-devel \
        libtool \
        libuuid-devel \
        libxml2-devel \
        libxslt \
        mono-devel \
        pkg-config \
        python2 \
        redhat-lsb-core \
        rh-python38 \
        rh-python38-python-devel \
        rh-ruby27 \
        rpm-build \
        shadow-utils \
        sqlite \
        tcl-devel \
        unzip \
        vim-enhanced \
        wget \
        zlib-devel \
        && \
    rm -f /etc/yum.repos.d/mono-centos7-stable.repo && \
    yum clean all && \
    rm -rf /var/cache/yum

# TODO: Is this really necessary?!? -- from swift's dockerfile
RUN sed -i -e 's/\*__block/\*__libc_block/g' /usr/include/unistd.h


# install docker 20
ENV DOCKER_BUCKET="download.docker.com" \
    DOCKER_CHANNEL="stable" \
    # pragma: allowlist nextline secret
    DIND_COMMIT="3b5fac462d21ca164b3778647420016315289034" \
    DOCKER_BUILDX_VERSION="v0.9.1"

ENV DOCKER_VERSION="20.10.21"

VOLUME /var/lib/docker

RUN set -ex \
    && if [ "$(uname -m)" == "aarch64" ]; then \
    # pragma: allowlist nextline secret
        DOCKER_SHA256="b4ceb6151d4dd1bfc7557f5fe0317e29cfcac91f798c34fae7dee891a811f8ee"; \
        DOCKER_COMPOSE_VERSION="v2.16.0"; \
    else \
    # pragma: allowlist nextline secret
        DOCKER_SHA256="2582bed8772b283bda9d4565c0af76ee653c93d93dc6b8d0aad795d731a1bb81"; \
        DOCKER_COMPOSE_VERSION="1.26.0"; \
    fi \
    && curl -fSLs "https://${DOCKER_BUCKET}/linux/static/${DOCKER_CHANNEL}/$(uname -m)/docker-${DOCKER_VERSION}.tgz" -o docker.tgz \
    && echo "${DOCKER_SHA256} *docker.tgz" | sha256sum --quiet -c - \
    && tar --extract --file docker.tgz --strip-components 1  --directory /usr/local/bin/ \
    && rm -f docker.tgz \
    && docker -v \
    # set up subuid/subgid so that "--userns-remap=default" works out-of-the-box
    && groupadd dockremap \
    && useradd -g dockremap dockremap \
    && echo 'dockremap:165536:65536' >> /etc/subuid \
    && echo 'dockremap:165536:65536' >> /etc/subgid \
    && curl -Ls "https://raw.githubusercontent.com/docker/docker/${DIND_COMMIT}/hack/dind" -o /usr/local/bin/dind \
    && curl -Ls https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && curl -Ls https://github.com/docker/buildx/releases/download/${DOCKER_BUILDX_VERSION}/buildx-${DOCKER_BUILDX_VERSION}.$(uname -s)-$(uname -m) -o /usr/local/lib/docker/cli-plugins/docker-buildx \
    && chmod +x /usr/local/bin/dind /usr/local/bin/docker-compose /usr/local/lib/docker/cli-plugins/docker-buildx \
    && docker-compose version

# build/install lz4
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    source /opt/rh/rh-ruby27/enable && \
    curl -Ls https://github.com/lz4/lz4/archive/refs/tags/v1.9.3.tar.gz -o lz4.tar.gz && \
    echo "030644df4611007ff7dc962d981f390361e6c97a34e5cbc393ddfbe019ffe2c1  lz4.tar.gz" > lz4-sha.txt && \
    sha256sum --quiet -c lz4-sha.txt && \
    mkdir lz4 && \
    tar --strip-components 1 --no-same-owner --directory lz4 -xf lz4.tar.gz && \
    cd lz4 && \
    make && \
    make install && \
    cd ../ && \
    rm -rf /tmp/*

# build/install liburing
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    source /opt/rh/rh-ruby27/enable && \
    curl -Ls https://github.com/axboe/liburing/archive/refs/tags/liburing-2.1.tar.gz -o liburing.tar.gz && \
    echo "f1e0500cb3934b0b61c5020c3999a973c9c93b618faff1eba75aadc95bb03e07  liburing.tar.gz" > liburing-sha.txt && \
    sha256sum --quiet -c liburing-sha.txt && \
    mkdir liburing && \
    tar --strip-components 1 --no-same-owner --directory liburing -xf liburing.tar.gz && \
    cd liburing && \
    ./configure && \
    make -j$(nproc) -C src V=1 CPPFLAGS="-Werror" CFLAGS="-g -O2 -Wall -Wextra -Werror" CXXFLAGS="-g -O2 -Wall -Wextra -Werror" && \
    make install && \
    cd ../ && \
    rm -rf /tmp/*

# build/install git
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/git/git/archive/v2.30.0.tar.gz -o git.tar.gz && \
    echo "8db4edd1a0a74ebf4b78aed3f9e25c8f2a7db3c00b1aaee94d1e9834fae24e61  git.tar.gz" > git-sha.txt && \
    sha256sum --quiet -c git-sha.txt && \
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
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/ninja-build/ninja/archive/refs/tags/v1.10.2.zip -o ninja.zip && \
    echo "4e7b67da70a84084d5147a97fcfb867660eff55cc60a95006c389c4ca311b77d  ninja.zip" > ninja-sha.txt && \
    sha256sum --quiet -c ninja-sha.txt && \
    unzip ninja.zip && \
    cd ninja-1.10.2 && \
    ./configure.py --bootstrap && \
    cp ninja /usr/bin && \
    cd .. && \
    rm -rf /tmp/*

# install cmake
RUN if [ "$(uname -m)" == "aarch64" ]; then \
    # pragma: allowlist nextline secret
        CMAKE_SHA256="4a750db7becfa426a37f702fa87267e836dda6720f2b768e31828f4cb5e2e24b"; \
    else \
    # pragma: allowlist nextline secret
        CMAKE_SHA256="b1dfd11d50e2dfb3d18be86ca1a369da1c1131badc14b659491dd42be1fed704"; \
    fi && \
    curl -Ls https://github.com/Kitware/CMake/releases/download/v3.26.1/cmake-3.26.1-$(uname -s)-$(uname -m).tar.gz -o cmake.tar.gz && \
    echo "${CMAKE_SHA256}  cmake.tar.gz" > cmake-sha.txt && \
    sha256sum --quiet -c cmake-sha.txt && \
    mkdir cmake && \
    tar --strip-components 1 --no-same-owner --directory cmake -xf cmake.tar.gz && \
    cp -r cmake/* /usr/local/ && \
    rm -rf /tmp/*

# build/install LLVM
# compiler-rt, libcxx and libcxxabi can't be built with gcc<11
#     ref: https://libcxx.llvm.org/#platform-and-compiler-support)
# so build and install clang first, then build other components and with clang
# build clang a second time to pass component check
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    curl -Ls https://github.com/llvm/llvm-project/releases/download/llvmorg-15.0.6/llvm-project-15.0.6.src.tar.xz -o llvm.tar.xz && \
    echo "9d53ad04dc60cb7b30e810faf64c5ab8157dadef46c8766f67f286238256ff92  llvm.tar.xz" > llvm-sha.txt && \
    sha256sum --quiet -c llvm-sha.txt && \
    mkdir llvm-project && \
    tar --strip-components 1 --no-same-owner --directory llvm-project -xf llvm.tar.xz && \
    cd llvm-project && \
    mkdir -p build && \
    cd build && \
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -G Ninja \
        -Wno-dev \
        -DLLVM_INCLUDE_EXAMPLES=OFF \
        -DLLVM_INCLUDE_TESTS=OFF \
        -DLLVM_ENABLE_PROJECTS="clang;clang-tools-extra;lld;lldb" \
        -DLLVM_STATIC_LINK_CXX_STDLIB=ON \
        -DCLANG_DEFAULT_PIE_ON_LINUX=OFF \
        ../llvm && \
    cmake --build . && \
    cmake --build . --target install && \
    cd .. && \
    rm -rf build && \
    mkdir build && \
    cd build && \
    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -G Ninja \
        -Wno-dev \
        -DLLVM_INCLUDE_EXAMPLES=OFF \
        -DLLVM_INCLUDE_TESTS=OFF \
        -DLLVM_ENABLE_PROJECTS="clang;compiler-rt;libcxx;libcxxabi;libunwind" \
        -DLLVM_STATIC_LINK_CXX_STDLIB=ON \
        -DCLANG_DEFAULT_PIE_ON_LINUX=OFF \
        -DCMAKE_C_COMPILER=/usr/local/bin/clang \
        -DCMAKE_CXX_COMPILER=/usr/local/bin/clang++ \
        ../llvm && \
    cmake --build . && \
    cmake --build . --target install && \
    cd ../.. && \
    rm -rf /tmp/*

# build/install openssl
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://www.openssl.org/source/openssl-1.1.1m.tar.gz -o openssl.tar.gz && \
    echo "f89199be8b23ca45fc7cb9f1d8d3ee67312318286ad030f5316aca6462db6c96  openssl.tar.gz" > openssl-sha.txt && \
    sha256sum --quiet -c openssl-sha.txt && \
    mkdir openssl && \
    tar --strip-components 1 --no-same-owner --directory openssl -xf openssl.tar.gz && \
    cd openssl && \
    ./config CFLAGS="-fPIC -O3" --prefix=/usr/local && \
    make -j`nproc` && \
    make -j1 install && \
    ln -sv /usr/local/lib64/lib*.so.1.1 /usr/lib64/ && \
    cd .. && \
    rm -rf /tmp/*

# install golang 1.19
RUN if [ "$(uname -m)" == "aarch64" ]; then \
        GOLANG_ARCH="arm64"; \
    # pragma: allowlist nextline secret
        GOLANG_SHA256="99de2fe112a52ab748fb175edea64b313a0c8d51d6157dba683a6be163fd5eab"; \
    else \
        GOLANG_ARCH="amd64"; \
    # pragma: allowlist nextline secret
        GOLANG_SHA256="74b9640724fd4e6bb0ed2a1bc44ae813a03f1e72a4c76253e2d5c015494430ba"; \
    fi && \
    curl -Ls https://golang.org/dl/go1.19.3.linux-${GOLANG_ARCH}.tar.gz -o golang.tar.gz && \
    echo "${GOLANG_SHA256}  golang.tar.gz" > golang-sha.txt && \
    sha256sum --quiet -c golang-sha.txt && \
    tar --directory /usr/local -xf golang.tar.gz && \
    echo '[ -x /usr/local/go/bin/go ] && export GOROOT=/usr/local/go && export GOPATH=$HOME/go && export PATH=$GOPATH/bin:$GOROOT/bin:$PATH' >> /etc/profile.d/golang.sh && \
    source /etc/profile.d/golang.sh && \
    go install github.com/onsi/ginkgo/v2/ginkgo@v2.1.3 && \
    go install golang.org/x/tools/cmd/goimports@latest && \
    go install github.com/segmentio/golines@latest && \
    go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.9.2 && \
    go install sigs.k8s.io/kustomize/kustomize/v4@v4.5.2 && \
    go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.50.1 && \
    go install github.com/goreleaser/goreleaser@v1.6.3 && \
    go install sigs.k8s.io/kind@v0.17.0 && \
    rm -rf /tmp/*

# build/install googlebenchmark
# If you change this, then old versions of FDB will stop building in the resulting image.  If you need to support old and
# new builds, then copy the below, and download / compile a second version with the newer SHA.  This needs to be kept in
# lock-step with the CMake logic that automatically downloads external dependencies.  See flowbench/CMakeLists.cmake and
# flowbench/benchmark.cmake in the main FDB repo for more information.

# The perl one-liner tells the googlebenchmark build to download a specific SHA of googletest.  By default, they track
# (tracked?) master, which no longer exists, and risks version skew / incompatibility.
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    cd /tmp && \
    git clone https://github.com/google/benchmark.git googlebenchmark-f91b6b && \
    cd googlebenchmark-f91b6b/ && \
    git checkout f91b6b42b1b9854772a90ae9501464a161707d1e && \
    perl -p -i -e s/master/2fe3bd994b3189899d93f1d5a881e725e046fdc2/ cmake/GoogleTest.cmake.in

# We disable regexp support for Clang because the system version of that library only works with GCC.
RUN GOOGLE_BENCHMARK_INSTALL_DIR_CLANG="/opt/googlebenchmark-f91b6b" && \
    mkdir -p ${GOOGLE_BENCHMARK_INSTALL_DIR_CLANG} && \
    cd /tmp/googlebenchmark-f91b6b && \
    mkdir -p /tmp/googlebenchmark-f91b6b/build-clang && \
    cmake -DCMAKE_BUILD_TYPE=Release \
        -DBENCHMARK_DOWNLOAD_DEPENDENCIES=on \
        -DBENCHMARK_ENABLE_LTO=true \
        -DRUN_HAVE_STD_REGEX=0 \
        -DRUN_HAVE_POSIX_REGEX=0  \
        -DCMAKE_INSTALL_PREFIX=${GOOGLE_BENCHMARK_INSTALL_DIR_CLANG} \
        -DCMAKE_C_COMPILER=clang \
        -DCMAKE_CXX_COMPILER=clang++ \
        -DCMAKE_CXX_FLAGS="-stdlib=libc++" \
        -S /tmp/googlebenchmark-f91b6b \
        -B /tmp/googlebenchmark-f91b6b/build-clang && \
    cd /tmp/googlebenchmark-f91b6b/build-clang && \
    make -j 16 && \
    make install && \
    cd /tmp/googlebenchmark-f91b6b && \
    rm -rf /tmp/googlebenchmark-f91b6b/build-clang

RUN GOOGLE_BENCHMARK_INSTALL_DIR_GCC="/opt/googlebenchmark-f91b6b-g++" && \
    mkdir -p ${GOOGLE_BENCHMARK_INSTALL_DIR_GCC} && \
    cd /tmp/googlebenchmark-f91b6b && \
    mkdir -p /tmp/googlebenchmark-f91b6b/build-gcc && \
    cmake -DCMAKE_BUILD_TYPE=Release \
        -DBENCHMARK_DOWNLOAD_DEPENDENCIES=on \
        -DBENCHMARK_ENABLE_LTO=true \
        -DRUN_HAVE_STD_REGEX=0 \
        -DRUN_HAVE_POSIX_REGEX=0 \
        -DCMAKE_INSTALL_PREFIX=${GOOGLE_BENCHMARK_INSTALL_DIR_GCC} \
        -S /tmp/googlebenchmark-f91b6b \
        -B /tmp/googlebenchmark-f91b6b/build-gcc && \
    cd /tmp/googlebenchmark-f91b6b/build-gcc && \
    make -j 16 && \
    make install && \
    cd /tmp/googlebenchmark-f91b6b && \
    rm -rf /tmp/googlebenchmark-f91b6b/build-clang

RUN rm -rf /tmp/googlebenchmark*

# install doctest_proj to /opt
# We use this in a header-only fashion, so there's no reason to compile it here.
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    mkdir -p /opt/doctest_proj_2.4.8 && \
    pushd /opt/doctest_proj_2.4.8 && \
    git init && \
    git remote add origin https://github.com/onqtam/doctest.git && \
    git fetch --depth 1 origin 7b9885133108ae301ddd16e2651320f54cafeba7 && \
    git checkout FETCH_HEAD && \
    popd && \
    rm -rf /tmp/*

# download junit dependencies
RUN mkdir /opt/junit_dependencies && \
    cd    /opt/junit_dependencies && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-engine/5.7.1/junit-jupiter-engine-5.7.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-api/5.7.1/junit-jupiter-api-5.7.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/junit/jupiter/junit-jupiter-params/5.7.1/junit-jupiter-params-5.7.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-commons/1.7.1/junit-platform-commons-1.7.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-engine/1.7.1/junit-platform-engine-1.7.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-launcher/1.7.1/junit-platform-launcher-1.7.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/junit/platform/junit-platform-console/1.7.1/junit-platform-console-1.7.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/apiguardian/apiguardian-api/1.1.1/apiguardian-api-1.1.1.jar && \
    curl -LsO https://search.maven.org/remotecontent?filepath=org/opentest4j/opentest4j/1.2.0/opentest4j-1.2.0.jar && \
    echo "fc68f0d28633caccf3980fdf1e99628fba9c49424ee56dc685cd8b4d2a9fefde apiguardian-api-1.1.1.jar" > junit_dependencies.sha256 && \
    echo "ce7b985bc469e2625759a4ebc45533c70581a05a348278c1d6408e9b2e35e314 junit-jupiter-api-5.7.1.jar" >> junit_dependencies.sha256 && \
    echo "56616c9350b3624f76cffef6b24ce7bb222915bfd5688f96d3cf4cef34f077cb junit-jupiter-engine-5.7.1.jar" >> junit_dependencies.sha256 && \
    echo "8effdd7f8a4ba5558b568184dee08008b2443c86c673ef81de5861fbc7ef0613 junit-jupiter-params-5.7.1.jar" >> junit_dependencies.sha256 && \
    echo "7c546be86864718fbaceb79fa84ff1d3a516500fc428f1b21d061c2e0fbc5a4b junit-platform-commons-1.7.1.jar" >> junit_dependencies.sha256 && \
    echo "11ed48fcdfcea32f2fa98872db7ecba2d49d178f76493e7a149a2242363ad12e junit-platform-console-1.7.1.jar" >> junit_dependencies.sha256 && \
    echo "37df5a9cd6dbc1f754ba2b46f96b8874a83660e1796bf38c738f022dcf86c23f junit-platform-engine-1.7.1.jar" >> junit_dependencies.sha256 && \
    echo "3122ac6fb284bc50e3afe46419fc977f94d580e9d3d1ea58805d200b510a99ee junit-platform-launcher-1.7.1.jar" >> junit_dependencies.sha256 && \
    echo "58812de60898d976fb81ef3b62da05c6604c18fd4a249f5044282479fc286af2 opentest4j-1.2.0.jar" >> junit_dependencies.sha256 && \
    sha256sum --quiet -c junit_dependencies.sha256

# install gradle
RUN curl -Ls https://services.gradle.org/distributions/gradle-7.5.1-bin.zip -o gradle.zip && \
    echo "f6b8596b10cce501591e92f229816aa4046424f3b24d771751b06779d58c8ec4  gradle.zip" > gradle-sha.txt && \
    sha256sum --quiet -c gradle-sha.txt && \
    unzip -qq gradle.zip && \
    mv gradle-7.5.1 /opt/gradle && \
    echo '[ -x /opt/gradle/bin/gradle ] && export PATH=/opt/gradle/bin/:$PATH' >> /etc/profile.d/gradle.sh && \
    rm -rf /tmp/*

# install maven
RUN curl -Ls https://archive.apache.org/dist/maven/maven-3/3.8.3/binaries/apache-maven-3.8.3-bin.zip -o maven.zip && \
    echo "f28cd38f620d76423c4543d5b443cdbdd5cfac2c511626cb92be3d5d273a6959  maven.zip" > maven-sha.txt && \
    sha256sum --quiet -c maven-sha.txt && \
    unzip -qq maven.zip && \
    mv apache-maven-3.8.3 /opt/maven && \
    echo '[ -x /opt/maven/bin/mvn ] && export PATH=/opt/maven/bin/:$PATH' >> /etc/profile.d/maven.sh && \
    rm -rf /tmp/*

# install rocksdb to /opt
RUN curl -Ls https://github.com/facebook/rocksdb/archive/refs/tags/v7.10.2.tar.gz -o rocksdb.tar.gz && \
    echo "4619ae7308cd3d11cdd36f0bfad3fb03a1ad399ca333f192b77b6b95b08e2f78 rocksdb.tar.gz" > rocksdb-sha.txt && \
    sha256sum --quiet -c rocksdb-sha.txt && \
    tar --directory /opt -xf rocksdb.tar.gz && \
    rm -rf /tmp/*

# install Boost to /opt
# This used to pass --with-libraries=context,filesystem,iostreams,system,serialization
# which doesn't build serialization (or builds it in a form that downstream cmake builds
# cannot find).  Instead of debugging their build logic, we pass "all", which is simpler to
# maintain and does build serialization.
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    curl -Ls https://boostorg.jfrog.io/artifactory/main/release/1.82.0/source/boost_1_82_0.tar.bz2 -o boost_1_82_0.tar.bz2 && \
    echo "a6e1ab9b0860e6a2881dd7b21fe9f737a095e5f33a3a874afc6a345228597ee6  boost_1_82_0.tar.bz2" > boost-sha.txt && \
    sha256sum --quiet -c boost-sha.txt && \
    mkdir -p /opt/boost_1_82_0 && \
    tar --strip-components 1 --no-same-owner --directory /opt/boost_1_82_0 -xjf boost_1_82_0.tar.bz2 && \
    cd /opt/boost_1_82_0 && \
    ./bootstrap.sh --with-libraries=all && \
    ./b2 link=static cxxflags="-std=c++17 -fPIC" --prefix=/opt/boost_1_82_0 -j32 install &&\
    rm -rf /opt/boost_1_82_0/libs && \
    rm -rf /tmp/*


# install Boost to /opt, using clang to compile the library
# Boost::context depends on some C++11 features, e.g. std::call_once; however,
# gcc and clang are using different ABIs, thus a gcc-built Boost::context is
# not linkable to clang objects.
# 
# Bug with b2 4.9.4 and clang, need to force the target triplet: https://github.com/bfgroup/b2/issues/183 
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    curl -Ls https://boostorg.jfrog.io/artifactory/main/release/1.82.0/source/boost_1_82_0.tar.bz2 -o boost_1_82_0.tar.bz2 && \
    echo "a6e1ab9b0860e6a2881dd7b21fe9f737a095e5f33a3a874afc6a345228597ee6  boost_1_82_0.tar.bz2" > boost-sha.txt && \
    sha256sum --quiet -c boost-sha.txt && \
    mkdir -p /opt/boost_1_82_0_clang && \
    tar --strip-components 1 --no-same-owner --directory /opt/boost_1_82_0_clang -xjf boost_1_82_0.tar.bz2 && \
    cd /opt/boost_1_82_0_clang && \
    ./bootstrap.sh --with-toolset=clang --with-libraries=all && \
    ./b2 link=static cxxflags="-std=c++17 -stdlib=libc++ -nostdlib++ -fPIC --target=x86_64-unknown-linux-gnu" linkflags="-stdlib=libc++ -nostdlib++ -static-libgcc -lc++ -lc++abi" --prefix=/opt/boost_1_82_0_clang -j32 install && \
    rm -rf /opt/boost_1_82_0_clang/libs && \
    rm -rf /tmp/*

# jemalloc (needed for FDB after 6.3)
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2 -o jemalloc.tar.bz2 && \
    echo "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa  jemalloc.tar.bz2" > jemalloc-sha.txt && \
    sha256sum --quiet -c jemalloc-sha.txt && \
    mkdir jemalloc && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory jemalloc -xjf jemalloc.tar.bz2 && \
    cd jemalloc && \
    ./configure --enable-static --disable-cxx --enable-prof && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*

# install jemalloc to /opt/jemalloc_5.3.0 (The FDB build no longer finds the above
# one for some reason; also installing it system-wide makes it difficult to upgrade
# the dependency without accidentally upgrading it for older FDB branches.)
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2 -o jemalloc.tar.bz2 && \
    echo "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa  jemalloc.tar.bz2" > jemalloc-sha.txt && \
    sha256sum --quiet -c jemalloc-sha.txt && \
    mkdir -p /opt/jemalloc_5.3.0 && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory /opt/jemalloc_5.3.0 -xjf jemalloc.tar.bz2 && \
    cd /opt/jemalloc_5.3.0 && \
    ./configure --enable-static --disable-cxx --enable-prof && \
    make -j32 && \
    cd .. && \
    rm -rf /tmp/*

# install msgpack
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -LsO https://github.com/msgpack/msgpack-c/releases/download/cpp-3.3.0/msgpack-3.3.0.tar.gz && \
    echo "6e114d12a5ddb8cb11f669f83f32246e484a8addd0ce93f274996f1941c1f07b  msgpack-3.3.0.tar.gz" > msgpack-sha.txt && \
    sha256sum --quiet -c msgpack-sha.txt && \
    mkdir -p /opt/msgpack_3.3.0 && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory /opt/msgpack_3.3.0 -xf msgpack-3.3.0.tar.gz && \
    cd /opt/msgpack_3.3.0 && \
    cmake . && \
    make && \
    cd .. && \
    rm -rf /tmp/*

# Install CCACHE
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/ccache/ccache/releases/download/v4.0/ccache-4.0.tar.gz -o ccache.tar.gz && \
    echo "ac97af86679028ebc8555c99318352588ff50f515fc3a7f8ed21a8ad367e3d45  ccache.tar.gz" > ccache-sha256.txt && \
    sha256sum --quiet -c ccache-sha256.txt && \
    mkdir ccache &&\
    tar --strip-components 1 --no-same-owner --directory ccache -xf ccache.tar.gz && \
    mkdir build && \
    cd build && \
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -DZSTD_FROM_INTERNET=ON ../ccache && \
    cmake --build . --target install && \
    cd .. && \
    rm -rf /tmp/*

# build/install toml
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/ToruNiina/toml11/archive/v3.4.0.tar.gz -o toml.tar.gz && \
    echo "bc6d733efd9216af8c119d8ac64a805578c79cc82b813e4d1d880ca128bd154d  toml.tar.gz" > toml-sha256.txt && \
    sha256sum --quiet -c toml-sha256.txt && \
    mkdir toml && \
    tar --strip-components 1 --no-same-owner --directory toml -xf toml.tar.gz && \
    cd toml && \
    mkdir build && \
    cd build && \
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -Dtoml11_BUILD_TEST=OFF ../ && \
    cmake --build . --target install && \
    cd .. && \
    rm -rf /tmp/*

# build/install distcc
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    curl -Ls https://github.com/distcc/distcc/archive/v3.3.5.tar.gz -o distcc.tar.gz && \
    echo "13a4b3ce49dfc853a3de550f6ccac583413946b3a2fa778ddf503a9edc8059b0  distcc.tar.gz" > distcc-sha256.txt && \
    sha256sum --quiet -c distcc-sha256.txt && \
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
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://sourceware.org/pub/valgrind/valgrind-3.20.0.tar.bz2 -o valgrind.tar.bz2 && \
    echo "8536c031dbe078d342f121fa881a9ecd205cb5a78e639005ad570011bdb9f3c6  valgrind.tar.bz2" > valgrind-sha.txt && \
    sha256sum --quiet -c valgrind-sha.txt && \
    mkdir valgrind && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory valgrind -xjf valgrind.tar.bz2 && \
    cd valgrind && \
    ./configure --enable-only64bit --enable-lto && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*

# download old fdb binaries and client lib if not on aarch64
RUN if [ ! "$(uname -m)" == "aarch64" ]; then \
        for old_fdb_version in 7.1.23 7.1.19 7.0.0 6.3.23 6.3.22 6.3.18 6.3.17 6.3.16 6.3.15 6.3.13 6.3.12 6.3.9 6.2.30 6.2.29 6.2.28 6.2.27 6.2.26 6.2.25 6.2.24 6.2.23 6.2.22 6.2.21 6.2.20 6.2.19 6.2.18 6.2.17 6.2.16 6.2.15 6.2.10 6.1.13 6.1.12 6.1.11 6.1.10 6.0.18 6.0.17 6.0.16 6.0.15 6.0.14 5.2.8 5.2.7 5.1.7 5.1.6; do \
            mkdir -p /opt/foundationdb/old/${old_fdb_version}/bin; \
            curl -Ls https://github.com/apple/foundationdb/releases/download/${old_fdb_version}/fdbserver.x86_64 -o /opt/foundationdb/old/${old_fdb_version}/bin/fdbserver-${old_fdb_version}; \
            chmod +x /opt/foundationdb/old/${old_fdb_version}/bin/fdbserver-${old_fdb_version}; \
            curl -Ls https://github.com/apple/foundationdb/releases/download/${old_fdb_version}/fdbmonitor.x86_64 -o /opt/foundationdb/old/${old_fdb_version}/bin/fdbmonitor-${old_fdb_version}; \
            chmod +x /opt/foundationdb/old/${old_fdb_version}/bin/fdbmonitor-${old_fdb_version}; \
            curl -Ls https://github.com/apple/foundationdb/releases/download/${old_fdb_version}/fdbcli.x86_64 -o /opt/foundationdb/old/${old_fdb_version}/bin/fdbcli-${old_fdb_version}; \
            chmod +x /opt/foundationdb/old/${old_fdb_version}/bin/fdbcli-${old_fdb_version}; \
            mkdir -p /opt/foundationdb/old/${old_fdb_version}/lib; \
            curl -Ls https://github.com/apple/foundationdb/releases/download/${old_fdb_version}/libfdb_c.x86_64.so -o /opt/foundationdb/old/${old_fdb_version}/lib/libfdb_c-${old_fdb_version}.so; \
        done; \
    fi

RUN curl -Ls https://github.com/manticoresoftware/manticoresearch/raw/master/misc/junit/ctest2junit.xsl -o /opt/ctest2junit.xsl

# Download Rust binaries
ENV RUSTUP_VERSION=1.24.3 \
    RUST_VERSION=1.59.0
RUN if [ "$(uname -m)" == "aarch64" ]; then \
        RUST_ARCH="aarch64-unknown-linux-gnu"; \
    # pragma: allowlist nextline secret
        RUST_SHA256="32a1532f7cef072a667bac53f1a5542c99666c4071af0c9549795bbdb2069ec1"; \
    else \
        RUST_ARCH="x86_64-unknown-linux-gnu"; \
    # pragma: allowlist nextline secret
        RUST_SHA256="3dc5ef50861ee18657f9db2eeb7392f9c2a6c95c90ab41e45ab4ca71476b4338"; \
    fi && \
    curl -LsO "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${RUST_ARCH}/rustup-init" && \
    echo "${RUST_SHA256}  rustup-init" > rustup-sha.txt && \
    sha256sum --quiet -c rustup-sha.txt && \
    chmod +x rustup-init && \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain ${RUST_VERSION} --default-host ${RUST_ARCH} && \
    rm -rf /tmp/*

# Download swift binaries
ARG SWIFT_SIGNING_KEY=8A7495662C3CD4AE18D95637FAF6989E1BC16FEA
ARG SWIFT_PLATFORM=centos
ARG OS_MAJOR_VER=7
ARG SWIFT_WEBROOT=https://download.swift.org/development

ENV SWIFT_SIGNING_KEY=$SWIFT_SIGNING_KEY \
    SWIFT_PLATFORM=$SWIFT_PLATFORM \
    OS_MAJOR_VER=$OS_MAJOR_VER \
    OS_VER=$SWIFT_PLATFORM$OS_MAJOR_VER \
    SWIFT_WEBROOT="$SWIFT_WEBROOT/$SWIFT_PLATFORM$OS_MAJOR_VER"

RUN echo "${SWIFT_WEBROOT}/latest-build.yml"

# download old fdb binaries and client lib if not on aarch64
# aarch64 package is not available for CentOS7
# https://www.swift.org/download/
RUN if [ ! "$(uname -m)" == "aarch64" ]; then \
        set -e; \
        export $(curl -Ls ${SWIFT_WEBROOT}/latest-build.yml | grep 'download:' | sed 's/:[^:\/\/]/=/g') && \
        export $(curl -Ls ${SWIFT_WEBROOT}/latest-build.yml | grep 'download_signature:' | sed 's/:[^:\/\/]/=/g')  && \
        export DOWNLOAD_DIR=$(echo $download | sed "s/-${OS_VER}.tar.gz//g") && \
        echo $DOWNLOAD_DIR > .swift_tag && \
        export GNUPGHOME="$(mktemp -d)" && \
        curl -fLs ${SWIFT_WEBROOT}/${DOWNLOAD_DIR}/${download} -o latest_toolchain.tar.gz && \
        curl -fLs ${SWIFT_WEBROOT}/${DOWNLOAD_DIR}/${download_signature} -o latest_toolchain.tar.gz.sig && \
        curl -fLs https://swift.org/keys/all-keys.asc | gpg --import -  && \
        gpg --batch --verify latest_toolchain.tar.gz.sig latest_toolchain.tar.gz && \
        tar -xzf latest_toolchain.tar.gz --directory / --strip-components=1 && \
        chmod -R o+r /usr/lib/swift && \
        rm -rf "$GNUPGHOME" latest_toolchain.tar.gz.sig latest_toolchain.tar.gz; \
    fi

# Print Installed Swift Version
RUN if [ ! "$(uname -m)" == "aarch64" ]; then \
        swift --version; \
    fi

RUN echo "Setup sphinx" && \
    source /opt/rh/rh-python38/enable && \
    python3 -m pip install setuptools==65.3.0 sphinx-bootstrap-theme==0.8.1 docutils==0.19 sphinx==5.1.1 sphinx-autobuild Jinja2==3.1.2 urllib3==2.0.2

# =========================== END OF LAYER: build ==============================
FROM build as devel

ARG DEVTOOLSET_VERSION=11

# protobuf-compiler and protobuf-devel are used to generate reference data for some tests.

RUN yum-config-manager --add-repo=https://copr.fedorainfracloud.org/coprs/carlwgeorge/ripgrep/repo/epel-7/carlwgeorge-ripgrep-epel-7.repo && \
    yum repolist && \
    yum -y install \
        bash-completion \
        byobu \
        cgdb \
        emacs-nox \
        fish \
        htop \
        jq \
        libevent-devel \
        ncurses-devel \
        nload \
        protobuf-compiler \
        protobuf-devel \
        nano \
        ripgrep \
        sysstat \
        the_silver_searcher \
        tree \
        vim-enhanced \
        zsh && \
    yum clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    pip3 install \
        boto3 \
        lxml \
        psutil \
        python-dateutil \
        subprocess32 \
        toml && \
    mkdir fdb-joshua && \
    cd fdb-joshua && \
    git clone https://github.com/FoundationDB/fdb-joshua . && \
    pip3 install /tmp/fdb-joshua && \
    cd /tmp && \
    # - Download kubectl from AWS
    #   https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html
    if [ "$(uname -m)" == "aarch64" ]; then \
        curl -Ls https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/arm64/kubectl -o kubectl && \
        curl -Ls https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/arm64/kubectl.sha256 -o kubectl.txt; \
    else \
        curl -Ls https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl -o kubectl && \
        curl -Ls https://s3.us-west-2.amazonaws.com/amazon-eks/1.22.6/2022-03-09/bin/linux/amd64/kubectl.sha256 -o kubectl.txt; \
    fi && \
    sha256sum --quiet -c kubectl.txt && \
    chmod 755 kubectl && \
    mv kubectl /usr/local/bin/kubectl && \
    if [ "$(uname -m)" == "aarch64" ]; then \
        curl -Ls https://github.com/derailed/k9s/releases/download/v0.26.5/k9s_Linux_arm64.tar.gz -o k9s.tar.gz && \
        echo "62bc8fe98ad035a64bbeede91bdd57de357af3059bd80975a9d22b6b072d97f8  k9s.tar.gz" > k9s-sha.txt; \
    else \
        curl -Ls https://github.com/derailed/k9s/releases/download/v0.26.5/k9s_Linux_x86_64.tar.gz -o k9s.tar.gz && \
        echo "8ae556d05b4744f7b79cc71549e4096daa9c6b913be88ee43be42ef800137bc3  k9s.tar.gz" > k9s-sha.txt; \
    fi && \
    sha256sum --quiet -c k9s-sha.txt && \
    tar --no-same-owner --no-same-permissions --directory /usr/local/bin -xzf k9s.tar.gz k9s && \
    if [ "$(uname -m)" == "aarch64" ]; then \
    # pragma: allowlist nextline secret
        AWSCLI_SHA256="bcb195622f0956c8569736b95bff9cff7f5684e400e694a33661f2d498fbc799"; \
    else \
    # pragma: allowlist nextline secret
        AWSCLI_SHA256="daf9253f0071b5cfee9532bc5220bedd7a5d29d4e0f92b42b9e3e4c496341e88"; \
    fi && \
    curl -Ls https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m)-2.7.34.zip -o "awscliv2.zip" && \
    echo "${AWSCLI_SHA256}  awscliv2.zip" > awscliv2.txt && \
    sha256sum --quiet -c awscliv2.txt && \
    unzip -qq awscliv2.zip && \
    ./aws/install && \
    rm -rf /tmp/*

# install tig (git client)
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/jonas/tig/releases/download/tig-2.5.4/tig-2.5.4.tar.gz -o tig.tar.gz && \
    echo "c48284d30287a6365f8a4750eb0b122e78689a1aef8ce1d2961b6843ac246aa7  tig.tar.gz" > tig-sha.txt && \
    sha256sum --quiet -c tig-sha.txt && \
    mkdir tig && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory tig -xzf tig.tar.gz && \
    cd tig && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*

# install newer tmux
RUN source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    curl -Ls https://github.com/tmux/tmux/releases/download/3.1c/tmux-3.1c.tar.gz -o tmux.tar.gz && \
    echo "918f7220447bef33a1902d4faff05317afd9db4ae1c9971bef5c787ac6c88386  tmux.tar.gz" > tmux-sha.txt && \
    sha256sum --quiet -c tmux-sha.txt && \
    mkdir tmux && \
    tar --strip-components 1 --no-same-owner --no-same-permissions --directory tmux -xzf tmux.tar.gz && \
    cd tmux && \
    ./configure && \
    make && \
    make install && \
    cd .. && \
    rm -rf /tmp/*

ARG FDB_VERSION="6.3.18"
RUN if [ ! "$(uname -m)" == "aarch64" ]; then \
        mkdir -p /usr/lib/foundationdb/plugins && \
        curl -Ls https://fdb-joshua.s3.amazonaws.com/old_tls_library.tgz | \
            tar --strip-components=1 --no-same-owner --directory /usr/lib/foundationdb/plugins -xz && \
        ln -sf /usr/lib/foundationdb/plugins/FDBGnuTLS.so /usr/lib/foundationdb/plugins/fdb-libressl-plugin.so && \
        curl -Ls https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/libfdb_c.x86_64.so -o /usr/lib64/libfdb_c_${FDB_VERSION}.so && \
        ln -sf /usr/lib64/libfdb_c_${FDB_VERSION}.so /usr/lib64/libfdb_c.so; \
    fi

# Install header files for compling the operator
RUN if [ ! "$(uname -m)" == "aarch64" ]; then \
        rpm -ivh https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients-${FDB_VERSION}-1.el7.x86_64.rpm --excludepath=/etc/foundationdb --excludepath=/usr/bin --excludepath=/usr/lib --excludepath=/usr/lib64; \
    fi

# Installing helm https://helm.sh/docs/intro/install for e2e testing with the operator.
ARG HELM_VERSION="v3.12.0"
RUN pushd /tmp && \
    # Binaries and checksum are fetched from here: https://github.com/helm/helm/releases/tag/v3.12.0
    if [ "$(uname -m)" == "aarch64" ]; then \
        curl -sLo helm.tar.gz "https://get.helm.sh/helm-${HELM_VERSION}-linux-arm64.tar.gz" && echo "658839fed8f9be2169f5df68e55cb2f0aa731a50df454caf183186766800bbd0 helm.tar.gz" | sha256sum --quiet -c -; \
    else \
        curl -sLo helm.tar.gz "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" && echo "da36e117d6dbc57c8ec5bab2283222fbd108db86c83389eebe045ad1ef3e2c3b helm.tar.gz" | sha256sum --quiet -c -; \
    fi && \

    tar -zxvf /tmp/helm.tar.gz && \
    mv linux-*/helm /usr/local/bin/helm && \
    helm version --short && \
    popd && \
    rm -rf /tmp/*

WORKDIR /root
# Pull in rust developer tools -- we use "nightly" for development
# for extra warning messages and support for cargo fuzz
RUN source $HOME/.cargo/env \
    rustup default nightly \
    rustup component add rust-analysis \
    rustup component add rust-src \
    rustup component add rls \
    cargo install cargo-fuzz

# add vscode server
RUN if [ "$(uname -m)" == "aarch64" ]; then \
        VSCODE_ARCH="arm64"; \
    else \
        VSCODE_ARCH="x64"; \
    fi && \
    curl -Ls https://update.code.visualstudio.com/latest/server-linux-${VSCODE_ARCH}/stable -o /tmp/vscode-server-linux-${VSCODE_ARCH}.tar.gz && \
    mkdir -p .vscode-server/bin/latest && \
    tar --strip-components 1 --no-same-owner --directory .vscode-server/bin/latest -xf /tmp/vscode-server-linux-${VSCODE_ARCH}.tar.gz && \
    touch .vscode-server/bin/latest/0 && \
    rm -rf /tmp/*
RUN rm -f /root/anaconda-ks.cfg && \
    printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -Eeuo pipefail' \
    '' \
    'mkdir -p ~/.docker' \
    'cat > ~/.docker/config.json << EOF' \
    '{' \
    ' "proxies":' \
    ' {' \
    '   "default":' \
    '   {' \
    '     "httpProxy": "${HTTP_PROXY}",' \
    '     "httpsProxy": "${HTTPS_PROXY}",' \
    '     "noProxy": "${NO_PROXY}"' \
    '   }' \
    ' }' \
    '}' \
    'EOF' \
    > /usr/local/bin/docker_proxy.sh && \
    chmod 755 /usr/local/bin/docker_proxy.sh && \
    printf '%s\n' \
    "source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable" \
    'source /opt/rh/rh-python38/enable' \
    'source /opt/rh/rh-ruby27/enable' \
    '' \
    'function cmk_ci() {' \
    '    cmake -S ${HOME}/src/foundationdb -B ${HOME}/build_output -D USE_CCACHE=ON -D USE_WERROR=ON -D RocksDB_ROOT=/opt/rocksdb-7.10.2 -D RUN_JUNIT_TESTS=ON -D RUN_JAVA_INTEGRATION_TESTS=ON -G Ninja && \' \
    '    ninja -v -C ${HOME}/build_output -j 84 all packages strip_targets' \
    '}' \
    'function cmk() {' \
    '    cmake -S ${HOME}/src/foundationdb -B ${HOME}/build_output -D USE_CCACHE=ON -D USE_WERROR=ON -D RocksDB_ROOT=/opt/rocksdb-7.10.2 -D RUN_JUNIT_TESTS=ON -D RUN_JAVA_INTEGRATION_TESTS=ON -G Ninja && \' \
    '    ninja -C ${HOME}/build_output -j 84' \
    '}' \
    'function ccmk() {' \
    '    CC=clang CXX=clang++ cmake -S ${HOME}/src/foundationdb -B ${HOME}/build_output -D USE_CCACHE=ON -D USE_WERROR=ON -D RocksDB_ROOT=/opt/rocksdb-7.10.2 -D RUN_JUNIT_TESTS=ON -D RUN_JAVA_INTEGRATION_TESTS=ON -G Ninja && \' \
    '    ninja -C ${HOME}/build_output -j 84' \
    '}' \
    'function ct() {' \
    '    cd ${HOME}/build_output && ctest -j 32 --no-compress-output -T test --output-on-failure' \
    '}' \
    'function j() {' \
    '   python3 -m joshua.joshua "${@}"' \
    '}' \
    'function jsd() {' \
    '   j start --tarball $(find ${HOME}/build_output/packages -name correctness\*.tar.gz) "${@}"' \
    '}' \
    '' \
    'function fmt() {' \
    '   find ${HOME}/src/foundationdb -type f \( -name \*.c -o -name \*.cpp -o -name \*.h -o -name \*.hpp \) -a \( ! -name sqlite3.amalgamation.c \) -a \( ! -path \*.git\* \) -exec clang-format -style=file -i "{}" \;' \
    '}' \
    '' \
    'USER_BASHRC="$HOME/src/.bashrc.local"' \
    'if test -f "$USER_BASHRC"; then' \
    '   source $USER_BASHRC' \
    'fi' \
    '' \
    'function addr2line() {' \
    '    EXECUTABLE=""' \
    '    while getopts ":e:" OPTION' \
    '    do' \
    '            case "$OPTION" in' \
    '                    e)' \
    '                            EXECUTABLE="$OPTARG"' \
    '                            ;;' \
    '                    ?)' \
    '                            ;;' \
    '            esac' \
    '    done' \
    '    if [ -z $EXECUTABLE ]' \
    '    then' \
    '            echo "Missing executable"' \
    '            exit 1' \
    '    fi' \
    '    readelf -p .comment $EXECUTABLE | grep -q "clang"' \
    '    if [ $? -eq 0 ]' \
    '    then' \
    '            # This is a clang generated ELF' \
    '            llvm-addr2line "$@"' \
    '    else' \
    '            # This is gcc/other compiler generated ELF' \
    '            command addr2line "$@"' \
    '    fi' \
    '}' \
    '' \
    'source $HOME/.cargo/env' \
    '' \
    >> .bashrc
# =========================== END OF LAYER: devel ==============================
FROM build as distcc
ARG DEVTOOLSET_VERSION=11
RUN useradd distcc && \
    source /opt/rh/devtoolset-${DEVTOOLSET_VERSION}/enable && \
    source /opt/rh/rh-python38/enable && \
    update-distcc-symlinks
EXPOSE 3632
EXPOSE 3633
USER distcc
ENV ALLOW 0.0.0.0/0
ENTRYPOINT distccd \
           --daemon \
           --enable-tcp-insecure \
           --no-detach \
           --port 3632 \
           --log-stderr \
           --log-level info \
           --listen 0.0.0.0 \
           --allow ${ALLOW} \
           --jobs `nproc`
# =========================== END OF LAYER: distcc =============================
FROM devel as codebuild
COPY dockerd-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["dockerd-entrypoint.sh"]
# =========================== END OF LAYER: codebuild ==========================