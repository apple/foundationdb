ARG REPOSITORY=foundationdb/build
ARG VERSION=centos6-latest
FROM ${REPOSITORY}:${VERSION}

# add vscode server
RUN yum repolist && \
    yum -y install \
        tmux \
        tree \
        emacs-nox \
        vim \
        bash-completion \
        jq \
        cgdb && \
    yum clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp
RUN source /opt/rh/devtoolset-8/enable && \
    source /opt/rh/rh-python36/enable && \
    pip3 install \
        python-dateutil \
        subprocess32 \
        psutil && \
    mkdir fdb-joshua && \
    cd fdb-joshua && \
    git clone --branch code_pipeline https://github.com/FoundationDB/fdb-joshua . && \
    pip3 install /tmp/fdb-joshua && \
    cd /tmp && \
    rm -rf /tmp/*

ARG OLD_FDB_BINARY_DIR=/app/deploy/global_data/oldBinaries/
ARG OLD_TLS_LIBRARY_DIR=/app/deploy/runtime/.tls_5_1/
ARG FDB_VERSION="6.2.29"
RUN mkdir -p ${OLD_FDB_BINARY_DIR} \
             ${OLD_TLS_LIBRARY_DIR} \
             /usr/lib/foundationdb/plugins && \
    curl -Ls https://www.foundationdb.org/downloads/misc/fdbservers-${FDB_VERSION}.tar.gz | tar -xz -C ${OLD_FDB_BINARY_DIR} && \
    rm -f ${OLD_FDB_BINARY_DIR}/*.sha256 && \
    chmod +x ${OLD_FDB_BINARY_DIR}/* && \
    curl -Ls https://www.foundationdb.org/downloads/misc/joshua_tls_library.tar.gz | tar -xz -C ${OLD_TLS_LIBRARY_DIR} --strip-components=1 && \
    curl -Ls https://www.foundationdb.org/downloads/${FDB_VERSION}/linux/libfdb_c_${FDB_VERSION}.so -o /usr/lib64/libfdb_c_${FDB_VERSION}.so && \
    ln -s /usr/lib64/libfdb_c_${FDB_VERSION}.so /usr/lib64/libfdb_c.so && \
    ln -s ${OLD_TLS_LIBRARY_DIR}/FDBGnuTLS.so /usr/lib/foundationdb/plugins/fdb-libressl-plugin.so && \
    ln -s ${OLD_TLS_LIBRARY_DIR}/FDBGnuTLS.so /usr/lib/foundationdb/plugins/FDBGnuTLS.so

WORKDIR /root
RUN echo -en "\n"\
    "source /opt/rh/devtoolset-8/enable\n"\
    "source /opt/rh/rh-python36/enable\n"\
    "source /opt/rh/rh-ruby24/enable\n"\
    "\n"\
    "function cmk() {\n"\
    "    cmake -S ${HOME}/src/foundationdb -B build_output -D USE_CCACHE=1 -D RocksDB_ROOT=/opt/rocksdb-6.10.1 -G Ninja && ninja -C build_output -j 84\n"\
    "}\n"\
    "function ct() {\n"\
    "    cd ${HOME}/build_output && ctest -j 32 --output-on-failure\n"\
    "}\n"\
    "function j() {\n"\
    "   python3 -m joshua.joshua --cluster-file /etc/foundationdb/cluster-file \"\${@}\"\n"\
    "}\n" >> .bashrc
