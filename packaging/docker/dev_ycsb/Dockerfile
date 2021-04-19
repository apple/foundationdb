ARG REPOSITORY=foundationdb/build
ARG VERSION=centos7-latest
FROM $REPOSITORY:$VERSION

#########################################################################################################################################
# This install YCSB AND the FDB client
# libraries necessary to run it. The
# following are the different files downloaded:
#
#  1. YCSB
#  2. libfdb_c_${FDB_VERSION}.so -- the C binding. Sent to /var/lib/fdb
#  3. fdb-java-${FDB_VERSION}.jar -- the Java library. Sent to ${YCSB_HOME}/foundationdb-binding/lib
#  4. jaxb-api-2.3.1.jar -- a library dependency necessary for making HDR histograms. Sent to ${YCSB_HOME}/foundationdb-binding/lib
#
# Note that these files are only complete for FDB 6.2.x. If you are wanting to run FDB 6.3.x versions, then you'll need to add
# libfdb_java_${FDB_VERSION}.so to /var/lib/fdb as well
#########################################################################################################################################

ENV YCSB_VERSION=ycsb-foundationdb-binding-0.17.0 \
    PATH=${PATH}:/usr/bin

RUN cd /opt \
    && eval curl "-Ls https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-foundationdb-binding-0.17.0.tar.gz" \
    | tar -xzvf - 

RUN rm -Rf /opt/${YCSB_VERSION}/lib/fdb-java-5.2.5.jar

WORKDIR /var/fdb/tmp

COPY . /var/fdb/tmp/packages

WORKDIR /var/fdb/tmp/packages/lib

RUN mv libfdb_c.so /usr/lib/libfdb_c.so && \
	mv libfdb_java.so /usr/lib/libfdb_java.so

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/

WORKDIR /var/fdb/tmp/packages

RUN mv fdb-java-7.0.0-PRERELEASE.jar /opt/${YCSB_VERSION}/lib/fdb-java-7.0.0-PRERELEASE.jar

WORKDIR "/opt/${YCSB_VERSION}"

CMD ["tail", "-f", "/dev/null"]
