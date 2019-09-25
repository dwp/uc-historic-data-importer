FROM dwp-centos-with-java-htme:latest

ARG APP_VERSION
ENV NAME=uc-historic-data-importer
ENV JAR=${NAME}-${APP_VERSION}.jar
COPY build/libs/$JAR ./
RUN ls -la *.jar

#ENTRYPOINT ["sh", "-c", "./hbase-to-mongo-export-latest.jar \"$@\"", "--"]
