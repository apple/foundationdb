ARG REPOSITORY=foundationdb/build
ARG VERSION=centos7-latest
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

WORKDIR /root
RUN curl -Ls https://update.code.visualstudio.com/latest/server-linux-x64/stable -o /tmp/vscode-server-linux-x64.tar.gz && \
    mkdir -p .vscode-server/bin/latest && \
    tar --strip-components 1 --no-same-owner --directory .vscode-server/bin/latest -xf /tmp/vscode-server-linux-x64.tar.gz && \
    touch .vscode-server/bin/latest/0 && \
    rm /tmp/*
RUN echo -en "\n"\
    "source /opt/rh/devtoolset-8/enable\n"\
    "source /opt/rh/rh-python36/enable\n"\
    "source /opt/rh/rh-ruby26/enable\n"\
    "\n"\
    "function cmk() {\n"\
    "    cmake -S ${HOME}/src/foundationdb -B build_output -D USE_CCACHE=1 -G Ninja && ninja -C build_output -j 84 \n"\
    "}\n"\
    "function ct() {\n"\
    "    cd ${HOME}/build_output && ctest -j 32 --output-on-failure\n"\
    "}\n" >> .bashrc
