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

WORKDIR /root
RUN echo -en "\n"\
    "source /opt/rh/devtoolset-8/enable\n"\
    "source /opt/rh/rh-python36/enable\n"\
    "source /opt/rh/rh-ruby24/enable\n"\
    "\n"\
    "function cmk() {\n"\
    "    cmake -S ${HOME}/src/foundationdb -B build_output -D USE_CCACHE=1 -G Ninja && ninja -C build_output -j 84 \n"\
    "}\n"\
    "function ct() {\n"\
    "    cd ${HOME}/build_output && ctest -j 32 --output-on-failure\n"\
    "}\n" >> .bashrc
