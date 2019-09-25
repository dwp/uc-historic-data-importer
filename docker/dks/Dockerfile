FROM dwp-centos-with-java:latest

EXPOSE 8443

RUN mkdir -p /opt/dks
WORKDIR /opt/dks

RUN wget $(curl --silent --insecure "https://api.github.com/repos/dwp/data-key-service/releases/latest" \
    | jq -r '.assets[0].browser_download_url') --no-check-certificate --output-document dks-latest.jar

RUN ls -la *.jar

COPY application.properties ./
COPY dks*.jks  ./


ENTRYPOINT ["sh", "-c", "./dks-latest.jar \"$@\"", "--"]
