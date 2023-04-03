#!/bin/sh

# Fedora does not more provide the openssl static libraries so we need to build
# them from sources

set -e

if [ ! -f /usr/lib64/libcrypto.a -a ! -f /opt/openssl/lib/libcrypto.a ]; then
  # build cmake config for lz4
  sudo dnf install -y \
    wget \
    make \
    gcc \
    coreutils \
    perl-interpreter \
    sed \
    zlib-devel \
    diffutils \
    lksctp-tools-devel \
    util-linux \
    perl-podlators \
    procps-ng \
    'perl(Test::Harness)' \
    'perl(Test::More)' \
    'perl(Math::BigInt)' \
    'perl(Module::Load::Conditional)' \
    'perl(File::Temp)' \
    'perl(Time::HiRes)' \
    'perl(FindBin)' \
    'perl(lib)' \
    'perl(File::Compare)' \
    'perl(File::Copy)' \
    perl-Pod-Html

    OPENSSL_FN=openssl-1.1.1t
    OPENSSL_URL=https://github.com/openssl/openssl/releases/download/OpenSSL_1_1_1t/openssl-1.1.1t.tar.gz

    OLD_DIR=`pwd`
    BUILD_DIR=/tmp/openssl
    mkdir -p $BUILD_DIR
    rm -rf $BUILD_DIR/*

    cd $BUILD_DIR

    wget $OPENSSL_URL
    tar -xvf $OPENSSL_FN.tar.gz
    cd $OPENSSL_FN
    ./config --prefix=/opt/$OPENSSL_FN --openssldir=/etc/pki/tls
    make -j `nproc` -k
    make -j `nproc` install DESTDIR=inst

    sudo mkdir -p /opt
    sudo cp -rv inst/opt/$OPENSSL_FN /opt/
    [ -e /opt/openssl ] || sudo ln -s /opt/$OPENSSL_FN /opt/openssl
    cd $OLD_DIR
    rm -rf $BUILD_DIR
fi
