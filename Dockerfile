FROM openjdk:8
ARG APP_VERSION
ENV APP_NAME=uc-historic-data-importer
ENV APP_JAR=${APP_NAME}-${APP_VERSION}.jar
ENV APP_HOME=/opt/${APP_NAME}
ENV USER=uhdi
RUN mkdir ${APP_HOME}
WORKDIR ${APP_HOME}
COPY build/libs/*.jar ./${APP_NAME}.jar
COPY uc-historic-data-importer-keystore.jks ./
COPY uc-historic-data-importer-truststore.jks ./
COPY resources/application-docker.properties application.properties
RUN useradd ${USER} && \
        chown -R ${USER}.${USER} . && \
        chmod +x ./${APP_NAME}.jar && ls -l && pwd

USER ${USER}
ENTRYPOINT ["sh", "-c", "java -Dcorrelation_id=${CORRELATION_ID} -Dtopic_name=${TOPIC_NAME} -Denvironment=${ENVIRONMENT} -Dapplication=${APPLICATION} -Dapp_version=${APP_VERSION} -Dcomponent=${COMPONENT} -jar -Dcorrelation_id=${CORRELATION_ID} -Dtopic_name=${TOPIC_NAME} -Denvironment=${ENVIRONMENT} -Dapplication=${APPLICATION} -Dapp_version=${APP_VERSION} -Dcomponent=${COMPONENT}./uc-historic-data-importer.jar \"$@\"", "--"]
