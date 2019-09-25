FROM dwp-centos-with-java:latest

ARG APP_VERSION
ENV APP_NAME=uc-historic-data-importer
ENV APP_JAR=${APP_NAME}-${APP_VERSION}.jar
ENV APP_HOME=/opt/${APP_NAME}
ENV USER=uhdi
RUN mkdir ${APP_HOME}
WORKDIR ${APP_HOME}
COPY build/libs/${APP_JAR} ./${APP_NAME}.jar
RUN useradd ${USER} && \
        chown -R ${USER}.${USER} . && \
        chmod +x ./${APP_NAME}.jar && ls -la *.jar && pwd
USER ${USER}
ENTRYPOINT ["sh", "-c", "./uc-historic-data-importer.jar \"$@\"", "--"]
