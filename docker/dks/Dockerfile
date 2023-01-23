FROM dwp-java:latest

EXPOSE 8443
EXPOSE 8080

ENV USER dks
ENV INSTALL_DIR /opt/data-key-service
RUN useradd ${USER}

RUN mkdir -p ${INSTALL_DIR}/logs
WORKDIR ${INSTALL_DIR}

RUN curl --silent --insecure "https://api.github.com/repos/dwp/data-key-service/releases/latest" \
             | jq -r '.assets[0].browser_download_url' >> dks_version
RUN cat dks_version
RUN wget --input-file dks_version --quiet --no-check-certificate --output-document dks-latest.jar
RUN ls -la *.jar

RUN chmod a+rwx -R /opt/data-key-service
COPY application-secure.properties ./
COPY dks*.jks  ./
RUN ls -la ./*

RUN chown -R ${USER}.${USER} ${INSTALL_DIR}
RUN chmod +x *.jar && ls -l
USER ${USER}

ENTRYPOINT ["sh", "-c", "java -Dlogging.config=logback.xml -Ddks.log.directory=${INSTALL_DIR}/logs -jar dks-latest.jar \"$@\"", "--"]
