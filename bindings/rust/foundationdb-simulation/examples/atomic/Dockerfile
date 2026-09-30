# This Dockerfile is used to build the `atomic` example.
# WARNING:
# The build image is fairly large, so you need to have extra storage space on your CI
FROM foundationdb/build:centos7-20240228040135-fc272dd89b
ARG FDB_VERSION="7.3.27"
ARG FDB_RS_FEATURE_VERSION="fdb-7_3"

# Required to correctly build the example
ENV RUSTC_LINKER=clang

# Add source code and bump Rust version to stable
RUN mkdir /root/fdb-rs
COPY . /root/fdb-rs
RUN /root/.cargo/bin/rustup update stable
RUN /root/.cargo/bin/rustup default stable

# Install libfdb
ADD https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients-${FDB_VERSION}-1.el7.x86_64.rpm /tmp/foundationdb-clients-${FDB_VERSION}-1.el7.x86_64.rpm
RUN rpm -i /tmp/foundationdb-clients-${FDB_VERSION}-1.el7.x86_64.rpm

# Install fdbserver
ADD https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-server-${FDB_VERSION}-1.el7.x86_64.rpm /tmp/foundationdb-server-${FDB_VERSION}-1.el7.x86_64.rpm
RUN rpm -i /tmp/foundationdb-server-${FDB_VERSION}-1.el7.x86_64.rpm

# Build the .so
# The artifact will be at /root/fdb-rs/target/release/examples/libatomic.so
RUN cd /root/fdb-rs && \
    source /opt/rh/devtoolset-11/enable && \
    source /root/.cargo/env && \
    RUSTFLAGS="-C linker=clang" cargo build -p foundationdb-simulation --release --example atomic --features ${FDB_RS_FEATURE_VERSION},fdb-docker

# In case you are running a seg fault, uncomment this to have the fdbserver debug
# ADD https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/fdbserver.debug.x86_64.gz /tmp/fdbserver.debug.x86_64.gz
# RUN gzip -d /tmp/fdbserver.debug.x86_64.gz

WORKDIR /root/fdb-rs
CMD ["/root/fdb-rs/scripts/simulation_entrypoint.sh", "20"]