FROM centos:6
LABEL version=0.1.6
ENV DOCKER_IMAGEVER=0.1.6

# Install dependencies for developer tools, bindings,\
# documentation, actorcompiler, and packaging tools\
RUN yum install -y yum-utils &&\
	yum-config-manager --enable rhel-server-rhscl-7-rpms &&\
	yum -y install centos-release-scl epel-release &&\
	yum -y install devtoolset-8 java-1.8.0-openjdk-devel \
		rh-python36-python-devel devtoolset-8-valgrind-devel \
		mono-core rh-ruby24 golang python27 rpm-build debbuild \
		python-pip npm dos2unix valgrind-devel ccache &&\
	pip install boto3==1.1.1

USER root

RUN adduser --comment '' fdb && chown fdb /opt

# wget of bintray without forcing UTF-8 encoding results in 403 Forbidden
RUN cd /opt/ &&\
    curl -L https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.bz2 > boost_1_67_0.tar.bz2 &&\
		echo "2684c972994ee57fc5632e03bf044746f6eb45d4920c343937a465fd67a5adba  boost_1_67_0.tar.bz2" > boost-sha.txt &&\
		sha256sum -c boost-sha.txt &&\
    tar -xjf boost_1_67_0.tar.bz2 &&\
    rm -rf boost_1_67_0.tar.bz2 boost-sha.txt boost_1_67_0/libs

# install cmake
RUN curl -L https://github.com/Kitware/CMake/releases/download/v3.13.4/cmake-3.13.4-Linux-x86_64.tar.gz > /tmp/cmake.tar.gz &&\
    echo "563a39e0a7c7368f81bfa1c3aff8b590a0617cdfe51177ddc808f66cc0866c76  /tmp/cmake.tar.gz" > /tmp/cmake-sha.txt &&\
    sha256sum -c /tmp/cmake-sha.txt &&\
    cd /tmp && tar xf cmake.tar.gz &&\
		cp -r cmake-3.13.4-Linux-x86_64/* /usr/local/ &&\
		rm -rf cmake.tar.gz cmake-3.13.4-Linux-x86_64 cmake-sha.txt

# install LibreSSL
RUN curl -L https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-2.8.2.tar.gz > /tmp/libressl.tar.gz &&\
    cd /tmp && echo "b8cb31e59f1294557bfc80f2a662969bc064e83006ceef0574e2553a1c254fd5  libressl.tar.gz" > libressl-sha.txt &&\
    sha256sum -c libressl-sha.txt && tar xf libressl.tar.gz &&\
    cd libressl-2.8.2 && cd /tmp/libressl-2.8.2 && scl enable devtoolset-8 -- ./configure --prefix=/usr/local/stow/libressl CFLAGS="-fPIC -O3" --prefix=/usr/local &&\
    cd /tmp/libressl-2.8.2 && scl enable devtoolset-8 -- make -j`nproc` install &&\
    rm -rf /tmp/libressl-2.8.2 /tmp/libressl.tar.gz

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0
ENV CC=/opt/rh/devtoolset-8/root/usr/bin/gcc
ENV CXX=/opt/rh/devtoolset-8/root/usr/bin/g++
CMD scl enable devtoolset-8 python27 rh-python36 rh-ruby24 -- bash
