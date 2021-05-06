ARG IMAGE_TAG=0.1.24
FROM centos:6

# Clean yum cache, disable default Base repo and enable Vault
RUN yum clean all &&\
  sed -i -e 's/gpgcheck=1/enabled=0/g' /etc/yum.repos.d/CentOS-Base.repo &&\
  sed -i -e 's/enabled=0/enabled=1/g' /etc/yum.repos.d/CentOS-Vault.repo &&\
  sed -i -n '/6.1/q;p' /etc/yum.repos.d/CentOS-Vault.repo &&\
  sed -i -e "s/6\.0/$(cut -d\  -f3 /etc/redhat-release)/g" /etc/yum.repos.d/CentOS-Vault.repo &&\
  yum install -y yum-utils &&\
  yum-config-manager --enable rhel-server-rhscl-7-rpms &&\
  yum -y install centos-release-scl-rh epel-release  \
    http://opensource.wandisco.com/centos/6/git/x86_64/wandisco-git-release-6-1.noarch.rpm &&\
  sed -i -e 's/#baseurl=/baseurl=/g' -e 's/mirror.centos.org/vault.centos.org/g' \
     -e 's/mirrorlist=/#mirrorlist=/g' /etc/yum.repos.d/CentOS-SCLo-scl-rh.repo &&\
  yum clean all

# Install dependencies for developer tools, bindings,\
# documentation, actorcompiler, and packaging tools\
RUN yum -y install devtoolset-8-8.1-1.el6 java-1.8.0-openjdk-devel \
      devtoolset-8-gcc-8.3.1 devtoolset-8-gcc-c++-8.3.1 \
      devtoolset-8-libubsan-devel devtoolset-8-libasan-devel devtoolset-8-valgrind-devel \
      rh-python36-python-devel rh-ruby24 golang python27 rpm-build \
      mono-core debbuild python-pip dos2unix valgrind-devel ccache \
      distcc wget libxslt git lz4 lz4-devel lz4-static &&\
  pip install boto3==1.1.1

USER root

RUN adduser --comment '' fdb && chown fdb /opt

# wget of bintray without forcing UTF-8 encoding results in 403 Forbidden
# Old versions of FDB need boost 1.67
RUN cd /opt/ &&\
    curl -L https://boostorg.jfrog.io/artifactory/main/release/1.67.0/source/boost_1_67_0.tar.bz2 -o boost_1_67_0.tar.bz2 &&\
    echo "2684c972994ee57fc5632e03bf044746f6eb45d4920c343937a465fd67a5adba  boost_1_67_0.tar.bz2" > boost-sha-67.txt &&\
    sha256sum -c boost-sha-67.txt &&\
    tar -xjf boost_1_67_0.tar.bz2 &&\
    rm -rf boost_1_67_0.tar.bz2 boost-sha-67.txt boost_1_67_0/libs

# install Boost 1.72
# wget of bintray without forcing UTF-8 encoding results in 403 Forbidden
RUN cd /tmp/ &&\
    curl -L https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.bz2 -o boost_1_72_0.tar.bz2 &&\
    echo "59c9b274bc451cf91a9ba1dd2c7fdcaf5d60b1b3aa83f2c9fa143417cc660722  boost_1_72_0.tar.bz2" > boost-sha-72.txt &&\
    sha256sum -c boost-sha-72.txt &&\
    tar -xjf boost_1_72_0.tar.bz2 &&\
    cd boost_1_72_0 &&\
    scl enable devtoolset-8 -- ./bootstrap.sh --with-libraries=context &&\
    scl enable devtoolset-8 -- ./b2 link=static cxxflags=-std=c++14 --prefix=/opt/boost_1_72_0 install &&\
    rm -rf boost_1_72_0.tar.bz2 boost-sha-72.txt boost_1_72_0

# jemalloc (needed for FDB after 6.3)
RUN cd /tmp/ &&\
    curl -L https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2 -o jemalloc-5.2.1.tar.bz2 &&\
    echo "34330e5ce276099e2e8950d9335db5a875689a4c6a56751ef3b1d8c537f887f6  jemalloc-5.2.1.tar.bz2" > jemalloc-sha.txt &&\
    sha256sum -c jemalloc-sha.txt &&\
    tar --no-same-owner --no-same-permissions -xjf jemalloc-5.2.1.tar.bz2 &&\
    cd jemalloc-5.2.1 &&\
    scl enable devtoolset-8 -- ./configure --enable-static --disable-cxx &&\
    scl enable devtoolset-8 -- make install

# install cmake
RUN curl -L https://github.com/Kitware/CMake/releases/download/v3.13.4/cmake-3.13.4-Linux-x86_64.tar.gz -o /tmp/cmake.tar.gz &&\
    echo "563a39e0a7c7368f81bfa1c3aff8b590a0617cdfe51177ddc808f66cc0866c76  /tmp/cmake.tar.gz" > /tmp/cmake-sha.txt &&\
    sha256sum -c /tmp/cmake-sha.txt &&\
    cd /tmp && tar xf cmake.tar.gz &&\
    cp -r cmake-3.13.4-Linux-x86_64/* /usr/local/ &&\
    rm -rf cmake.tar.gz cmake-3.13.4-Linux-x86_64 cmake-sha.txt

# install Ninja
RUN cd /tmp && curl -L https://github.com/ninja-build/ninja/archive/v1.9.0.zip -o ninja.zip &&\
    echo "8e2e654a418373f10c22e4cc9bdbe9baeca8527ace8d572e0b421e9d9b85b7ef  ninja.zip" > /tmp/ninja-sha.txt &&\
    sha256sum -c /tmp/ninja-sha.txt &&\
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

# Install toml11
 RUN cd /tmp && curl -L https://github.com/ToruNiina/toml11/archive/v3.4.0.tar.gz > toml.tar.gz &&\
     echo "bc6d733efd9216af8c119d8ac64a805578c79cc82b813e4d1d880ca128bd154d  toml.tar.gz" > toml-sha256.txt &&\
     sha256sum -c toml-sha256.txt &&\
     tar xf toml.tar.gz && rm -rf build && mkdir build && cd build && scl enable devtoolset-8 -- cmake -G Ninja -DCMAKE_BUILD_TYPE=Release -Dtoml11_BUILD_TEST=OFF ../toml11-3.4.0 &&\
     scl enable devtoolset-8 -- cmake --build . --target install && cd / && rm -rf tmp/build && rm -rf tmp/toml11-3.4.0

RUN cd /opt/ && curl -L https://github.com/facebook/rocksdb/archive/v6.10.1.tar.gz -o rocksdb.tar.gz &&\
    echo "d573d2f15cdda883714f7e0bc87b814a8d4a53a82edde558f08f940e905541ee  rocksdb.tar.gz" > rocksdb-sha.txt &&\
    sha256sum -c rocksdb-sha.txt && tar xf rocksdb.tar.gz && rm -rf rocksdb.tar.gz rocksdb-sha.txt

RUN cd /opt/ && curl -L https://github.com/manticoresoftware/manticoresearch/raw/master/misc/junit/ctest2junit.xsl -o ctest2junit.xsl

# Setting this environment variable switches from OpenSSL to BoringSSL
ENV OPENSSL_ROOT_DIR=/opt/boringssl

# install BoringSSL:  TODO: They don't seem to have releases(?)  I picked today's master SHA.
RUN cd /opt &&\
    git clone https://boringssl.googlesource.com/boringssl &&\
    cd boringssl &&\
    git checkout e796cc65025982ed1fb9ef41b3f74e8115092816 &&\
    mkdir build

# ninja doesn't respect CXXFLAGS, and the boringssl CMakeLists doesn't expose an option to define __STDC_FORMAT_MACROS
# also, enable -fPIC.
# this is moderately uglier than creating a patchfile, but easier to maintain.
RUN cd /opt/boringssl &&\
	for f in crypto/fipsmodule/rand/fork_detect_test.cc \
	         include/openssl/bn.h \
		 ssl/test/bssl_shim.cc ; do \
		perl -p -i -e 's/#include <inttypes.h>/#define __STDC_FORMAT_MACROS 1\n#include <inttypes.h>/g;' $f ; \
	done &&\
	perl -p -i -e 's/-Werror/-Werror -fPIC/' CMakeLists.txt &&\
	git diff

RUN cd /opt/boringssl/build &&\
    scl enable devtoolset-8 rh-python36 rh-ruby24 -- cmake -GNinja -DCMAKE_BUILD_TYPE=Release .. &&\
    scl enable devtoolset-8 rh-python36 rh-ruby24 -- ninja &&\
	./ssl/ssl_test &&\
	mkdir -p ../lib && cp crypto/libcrypto.a ssl/libssl.a ../lib

# Localize time zone
ARG TIMEZONEINFO=America/Los_Angeles
RUN rm -f /etc/localtime && ln -s /usr/share/zoneinfo/${TIMEZONEINFO} /etc/localtime

LABEL version=${IMAGE_TAG}
ENV DOCKER_IMAGEVER=${IMAGE_TAG}
ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0
ENV CC=/opt/rh/devtoolset-8/root/usr/bin/gcc
ENV CXX=/opt/rh/devtoolset-8/root/usr/bin/g++

ENV CCACHE_NOHASHDIR=true
ENV CCACHE_UMASK=0000
ENV CCACHE_SLOPPINESS="file_macro,time_macros,include_file_mtime,include_file_ctime,file_stat_matches"

CMD scl enable devtoolset-8 rh-python36 rh-ruby24 -- bash
