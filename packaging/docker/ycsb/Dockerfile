FROM openjdk:17-slim AS RUN

WORKDIR /tmp
RUN apt-get update && \
    apt-get install -y curl zip && \
    curl -Ls https://amazon-eks.s3.amazonaws.com/1.19.6/2021-01-05/bin/linux/amd64/kubectl -o kubectl && \
    echo "08ff68159bbcb844455167abb1d0de75bbfe5ae1b051f81ab060a1988027868a  kubectl" > kubectl.txt && \
    sha256sum -c kubectl.txt && \
    mv kubectl /usr/local/bin/kubectl && \
    chmod 755 /usr/local/bin/kubectl && \
    curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.2.43.zip -o "awscliv2.zip" && \
    echo "9a8b3c4e7f72bbcc55e341dce3af42479f2730c225d6d265ee6f9162cfdebdfd  awscliv2.zip" > awscliv2.txt && \
    sha256sum -c awscliv2.txt && \
    unzip -qq awscliv2.zip && \
    ./aws/install && \
    rm -rf /tmp/*

ADD YCSB /YCSB
WORKDIR /YCSB

ENV FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY=/var/dynamic-conf/lib/multiversion/
ENV FDB_NETWORK_OPTION_TRACE_ENABLE=/var/log/fdb-trace-logs
ENV LD_LIBRARY_PATH=/var/dynamic-conf/lib/
ENV BUCKET=backup-112664522426-us-west-2

# TODO: Log4J complains that it's eating the HTracer logs.  Even without it, we get per-operation
# time series graphs of throughput, median, 90, 99, 99.9 and 99.99 (in usec).
COPY ycsb/run_ycsb.sh /usr/local/bin/run_ycsb.sh
RUN mkdir -p /var/log/fdb-trace-logs && \
    chmod +x /usr/local/bin/run_ycsb.sh

CMD ["run_ycsb.sh"]
