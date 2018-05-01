FROM ubuntu:15.04

RUN sed -i -e 's/archive.ubuntu.com\|security.ubuntu.com/old-releases.ubuntu.com/g' -e 's/us\.old/old/g' /etc/apt/sources.list && apt-get clean

RUN apt-get update && apt-get --no-install-recommends install -y --force-yes bzip2 ca-certificates=20141019 adduser apt base-files base-passwd bash binutils build-essential cpp cpp-4.9 dpkg dos2unix fakeroot findutils g++=4:4.9.2-2ubuntu2 g++-4.9=4.9.2-10ubuntu13 gawk=1:4.1.1+dfsg-1 gcc-5-base gcc=4:4.9.2-2ubuntu2 gcc-4.9=4.9.2-10ubuntu13 gcc-4.9-base:amd64=4.9.2-10ubuntu13 gcc-5-base:amd64=5.1~rc1-0ubuntu1 gdb git golang golang-go golang-go-linux-amd64 golang-src grep gzip hostname java-common libasan1 liblsan0 libtsan0 libubsan0 libcilkrts5 libgcc-4.9-dev libstdc++-4.9-dev libgl1-mesa-dri libgl1-mesa-glx libmono-system-xml-linq4.0-cil libmono-system-data-datasetextensions4.0-cil libstdc++-4.9-pic locales login m4 make makedev mawk mono-dmcs npm openjdk-8-jdk passwd python-distlib python-gevent python-greenlet python-html5lib python-minimal python-pip python-pkg-resources python-requests python-setuptools python-six python-urllib3 python-yaml python2.7 python2.7-minimal rpm rpm2cpio ruby ruby2.1 rubygems-integration sed tar texinfo tzdata-java udev unzip util-linux valgrind vim wget golang-go.tools curl sphinx-common gnupg

RUN adduser --disabled-password --gecos '' fdb && chown -R fdb /opt && chmod -R 0777 /opt

USER fdb

RUN cd /opt/ && wget http://downloads.sourceforge.net/project/boost/boost/1.52.0/boost_1_52_0.tar.bz2 -qO - | tar -xj

USER root

RUN pip install boto3==1.1.1

RUN ln -s /usr/bin/nodejs /usr/bin/node

RUN cd /opt/ && wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-2.6.4.tar.gz &&\
    wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl-2.6.4.tar.gz.asc &&\
    wget https://ftp.openbsd.org/pub/OpenBSD/LibreSSL/libressl.asc &&\
    gpg --import libressl.asc && gpg --verify libressl-2.6.4.tar.gz.asc libressl-2.6.4.tar.gz &&\
    tar -xzf libressl-2.6.4.tar.gz && cd libressl-2.6.4 &&\
    ./configure CFLAGS="-fPIC -O3" && make -j4 && make install &&\
    cd /opt/ # && rm -r libressl-2.6.4/ libressl-2.6.4.tar.gz libressl-2.6.4.tar.gz.asc libressl.asc

RUN LANGUAGE=en_US.UTF-8 LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 locale-gen en_US.UTF-8

RUN dpkg-reconfigure locales

ARG TARGET_LIBC_VERSION=2.17
ENV TARGET_LIBC_VERSION=$TARGET_LIBC_VERSION

ARG CC=/usr/bin/gcc
ENV CC=$CC
