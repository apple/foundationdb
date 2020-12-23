FROM centos:6

# Install dependencies for developer tools, bindings,\
# documentation, actorcompiler, and packaging tools\
RUN yum install -y yum-utils &&\
  yum-config-manager --enable rhel-server-rhscl-7-rpms &&\
  yum -y install centos-release-scl epel-release \
    http://opensource.wandisco.com/centos/6/git/x86_64/wandisco-git-release-6-1.noarch.rpm &&\
  yum -y install devtoolset-8-8.1-1.el6 java-1.8.0-openjdk-devel \
    devtoolset-8-gcc-8.3.1 devtoolset-8-gcc-c++-8.3.1 \
    devtoolset-8-libubsan-devel devtoolset-8-libasan-devel devtoolset-8-valgrind-devel \
    rh-python36-python-devel rh-ruby24 golang python27 rpm-build \
    mono-core debbuild python-pip dos2unix valgrind-devel ccache \
    distcc wget git lz4 lz4-devel lz4-static &&\
  pip install boto3==1.1.1

USER root

RUN adduser --comment '' fdb && chown fdb /opt

# wget of bintray without forcing UTF-8 encoding results in 403 Forbidden
RUN cd /opt/ &&\
    curl -L https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.bz2 -o boost_1_67_0.tar.bz2 &&\
    echo "2684c972994ee57fc5632e03bf044746f6eb45d4920c343937a465fd67a5adba  boost_1_67_0.tar.bz2" > boost-sha-67.txt &&\
    sha256sum -c boost-sha-67.txt &&\
    tar -xjf boost_1_67_0.tar.bz2 &&\
    rm -rf boost_1_67_0.tar.bz2 boost-sha-67.txt boost_1_67_0/libs &&\
    curl -L https://dl.bintray.com/boostorg/release/1.72.0/source/boost_1_72_0.tar.bz2 -o boost_1_72_0.tar.bz2 &&\
    echo "59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722  boost_1_72_0.tar.bz2" > boost-sha-72.txt &&\
    sha256sum -c boost-sha-72.txt &&\
    tar -xjf boost_1_72_0.tar.bz2 &&\
    rm -rf boost_1_72_0.tar.bz2 boost-sha-72.txt boost_1_72_0/libs

# install cmake
RUN curl -L https://github.com/Kitware/CMake/releases/download/v3.13.4/cmake-3.13.4-Linux-x86_64.tar.gz -o /tmp/cmake.tar.gz &&\
    echo "563a39e0a7c7368f81bfa1c3aff8b590a0617cdfe51177ddc808f66cc0866c76  /tmp/cmake.tar.gz" > /tmp/cmake-sha.txt &&\
    sha256sum -c /tmp/cmake-sha.txt &&\
    cd /tmp && tar xf cmake.tar.gz &&\
    cp -r cmake-3.13.4-Linux-x86_64/* /usr/local/ &&\
    rm -rf cmake.tar.gz cmake-3.13.4-Linux-x86_64 cmake-sha.txt

# install Ninja
RUN cd /tmp && curl -L https://github.com/ninja-build/ninja/archive/v1.9.0.zip -o ninja.zip &&\
    unzip ninja.zip && cd ninja-1.9.0 && scl enable devtoolset-8 -- ./configure.py --bootstrap && cp ninja /usr/bin &&\
    cd .. && rm -rf ninja-1.9.0 ninja.zip

# install openssl
RUN cd /tmp && curl -L https://www.openssl.org/source/openssl-1.1.1h.tar.gz -o openssl.tar.gz &&\
    echo "5c9ca8774bd7b03e5784f26ae9e9e6d749c9da2438545077e6b3d755a06595d9  openssl.tar.gz" > openssl-sha.txt &&\
    sha256sum -c openssl-sha.txt && tar -xzf openssl.tar.gz &&\
    cd openssl-1.1.1h && scl enable devtoolset-8 -- ./config CFLAGS="-fPIC -O3" --prefix=/usr/local &&\
    scl enable devtoolset-8 -- make -j`nproc` && scl enable devtoolset-8 -- make -j1 install &&\
    ln -sv /usr/local/lib64/lib*.so.1.1 /usr/lib64/ &&\
    cd /tmp/ && rm -rf /tmp/openssl-1.1.1h /tmp/openssl.tar.gz

RUN cd /opt/ && curl -L https://github.com/facebook/rocksdb/archive/v6.10.1.tar.gz -o rocksdb.tar.gz &&\
    echo "d573d2f15cdda883714f7e0bc87b814a8d4a53a82edde558f08f940e905541ee  rocksdb.tar.gz" > rocksdb-sha.txt &&\
    sha256sum -c rocksdb-sha.txt && tar xf rocksdb.tar.gz && rm -rf rocksdb.tar.gz rocksdb-sha.txt

# Localize time zone
ARG TIMEZONEINFO=America/Los_Angeles
RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/${TIMEZONEINFO} /etc/localtime

LABEL version=0.1.19
ENV DOCKER_IMAGEVER=0.1.19
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0
ENV CC=/opt/rh/devtoolset-8/root/usr/bin/gcc
ENV CXX=/opt/rh/devtoolset-8/root/usr/bin/g++
CMD scl enable devtoolset-8 rh-python36 rh-ruby24 -- bash
