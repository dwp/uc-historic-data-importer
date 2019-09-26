FROM dwp-centos-with-java:latest

EXPOSE 8443

ENV USER dks
ENV INSTALL_DIR /opt/dks
RUN useradd ${USER}

RUN mkdir -p ${INSTALL_DIR}
WORKDIR ${INSTALL_DIR}

RUN wget $(curl --silent --insecure "https://api.github.com/repos/dwp/data-key-service/releases/latest" \
    | jq -r '.assets[0].browser_download_url') --no-check-certificate --output-document dks-latest.jar

COPY application.properties ./
COPY dks*.jks  ./

RUN chown -R ${USER}.${USER} ${INSTALL_DIR}
RUN chmod +x *.jar && ls -l
USER ${USER}
ENTRYPOINT ["sh", "-c", "./dks-latest.jar \"$@\"", "--"]
