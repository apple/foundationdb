FROM centos:6
LABEL version=0.1

RUN yum install -y yum-utils
RUN yum-config-manager --enable rhel-server-rhscl-7-rpms
RUN yum -y install centos-release-scl
RUN yum install -y devtoolset-7

# install cmake
RUN curl -L https://github.com/Kitware/CMake/releases/download/v3.13.4/cmake-3.13.4-Linux-x86_64.tar.gz > /tmp/cmake.tar.gz &&\
    echo "563a39e0a7c7368f81bfa1c3aff8b590a0617cdfe51177ddc808f66cc0866c76  /tmp/cmake.tar.gz" > /tmp/cmake-sha.txt &&\
    sha256sum -c /tmp/cmake-sha.txt &&\
    cd /tmp && tar xf cmake.tar.gz && cp -r cmake-3.13.4-Linux-x86_64/* /usr/local/

# install boost
RUN curl -L https://dl.bintray.com/boostorg/release/1.67.0/source/boost_1_67_0.tar.bz2 > /tmp/boost.tar.bz2 &&\
    cd /tmp && echo "2684c972994ee57fc5632e03bf044746f6eb45d4920c343937a465fd67a5adba  boost.tar.bz2" > boost-sha.txt &&\
    sha256sum -c boost-sha.txt && tar xf boost.tar.bz2 && cp -r boost_1_67_0/boost /usr/local/include/

# install mono (for actorcompiler)
RUN yum install -y epel-release
RUN yum install -y mono-core

# install Java
RUN yum install -y java-1.8.0-openjdk-devel

# install python
RUN yum install -y rh-python36-python-devel

CMD scl enable devtoolset-7 -- scl enable rh-python36 -- bash
