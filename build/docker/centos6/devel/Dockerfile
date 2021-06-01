ARG REPOSITORY=foundationdb/build
ARG VERSION=centos6-latest
FROM ${REPOSITORY}:${VERSION}

# add vscode server
RUN yum repolist && \
    yum -y install \
        bash-completion \
        byobu \
        cgdb \
        emacs-nox \
        jq \
        the_silver_searcher \
        tmux \
        tree \
        vim \
        zsh && \
    yum clean all && \
    rm -rf /var/cache/yum

WORKDIR /tmp
RUN source /opt/rh/devtoolset-8/enable && \
    source /opt/rh/rh-python36/enable && \
    pip3 install \
        lxml \
        psutil \
        python-dateutil \
        subprocess32 && \
    mkdir fdb-joshua && \
    cd fdb-joshua && \
    git clone https://github.com/FoundationDB/fdb-joshua . && \
    pip3 install /tmp/fdb-joshua && \
    cd /tmp && \
    curl -Ls https://amazon-eks.s3.us-west-2.amazonaws.com/1.18.9/2020-11-02/bin/linux/amd64/kubectl -o kubectl && \
    echo "3dbe69e6deb35fbd6fec95b13d20ac1527544867ae56e3dae17e8c4d638b25b9  kubectl" > kubectl.txt && \
    sha256sum -c kubectl.txt && \
    mv kubectl /usr/local/bin/kubectl && \
    chmod 755 /usr/local/bin/kubectl && \
    curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.0.30.zip -o "awscliv2.zip" && \
    echo "7ee475f22c1b35cc9e53affbf96a9ffce91706e154a9441d0d39cbf8366b718e  awscliv2.zip" > awscliv2.txt && \
    sha256sum -c awscliv2.txt && \
    unzip -qq awscliv2.zip && \
    ./aws/install && \
    rm -rf /tmp/*

ARG FDB_VERSION="6.2.29"
RUN mkdir -p /usr/lib/foundationdb/plugins && \
    curl -Ls https://www.foundationdb.org/downloads/misc/joshua_tls_library.tar.gz | \
        tar --strip-components=1 --no-same-owner --directory /usr/lib/foundationdb/plugins -xz && \
    ln -sf /usr/lib/foundationdb/plugins/FDBGnuTLS.so /usr/lib/foundationdb/plugins/fdb-libressl-plugin.so && \
    curl -Ls https://www.foundationdb.org/downloads/${FDB_VERSION}/linux/libfdb_c_${FDB_VERSION}.so -o /usr/lib64/libfdb_c_${FDB_VERSION}.so && \
    ln -sf /usr/lib64/libfdb_c_${FDB_VERSION}.so /usr/lib64/libfdb_c.so

WORKDIR /root
RUN rm -f /root/anaconda-ks.cfg && \
    printf '%s\n' \
    'source /opt/rh/devtoolset-8/enable' \
    'source /opt/rh/rh-python36/enable' \
    'source /opt/rh/rh-ruby26/enable' \
    '' \
    'function cmk_ci() {' \
    '    cmake -S ${HOME}/src/foundationdb -B ${HOME}/build_output -D USE_CCACHE=ON -D USE_WERROR=ON -D RocksDB_ROOT=/opt/rocksdb-6.10.1 -D RUN_JUNIT_TESTS=ON -D RUN_JAVA_INTEGRATION_TESTS=ON -G Ninja && \' \
    '    ninja -v -C ${HOME}/build_output -j 84 all packages strip_targets' \
    '}' \
    'function cmk() {' \
    '    cmake -S ${HOME}/src/foundationdb -B ${HOME}/build_output -D USE_CCACHE=ON -D USE_WERROR=ON -D RocksDB_ROOT=/opt/rocksdb-6.10.1 -D RUN_JUNIT_TESTS=ON -D RUN_JAVA_INTEGRATION_TESTS=ON -G Ninja && \' \
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
    'USER_BASHRC="$HOME/src/.bashrc.local"' \
    'if test -f "$USER_BASHRC"; then' \
    '   source $USER_BASHRC' \
    'fi' \
    '' \
    >> .bashrc
