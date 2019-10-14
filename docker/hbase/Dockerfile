FROM openjdk:8-alpine

ARG HBASE_VERSION=1.4.9

ENV PATH $PATH:/hbase/bin

ENV JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk

ENV HBASE_MANAGES_ZK=false

WORKDIR /

RUN apk update && \
    apk add --no-cache procps wget tar bash && \
    rm -rf /var/lib/apt/lists/*

RUN url_archive="http://archive.apache.org/dist/hbase/$HBASE_VERSION/hbase-$HBASE_VERSION-bin.tar.gz" && \
    wget -q -t 10 --max-redirect 1 --retry-connrefused -O "hbase-$HBASE_VERSION-bin.tar.gz" "$url_archive" && \
    mkdir "hbase-$HBASE_VERSION" && \
    tar zxf "hbase-$HBASE_VERSION-bin.tar.gz" -C "hbase-$HBASE_VERSION" --strip 1 && \
    ln -sv "hbase-$HBASE_VERSION" hbase && \
    rm -fv "hbase-$HBASE_VERSION-bin.tar.gz" && \
    mkdir -pv /data && \
    mkdir -pv /hbase/logs

VOLUME /data

COPY entrypoint.sh /
COPY hbase-site.template.xml /hbase/conf/

EXPOSE 2181 8080 8085 9090 9095 16000 16010 16201 16302

ENTRYPOINT ["/entrypoint.sh"]
