# uc-historic-data-importer

Import UC mongo backup into hbase.


A Makefile wraps some of the gradle and docker-compose commands to give a
more unified basic set of operations. These can be checked by running:

```
$ make help
```

## Build

Ensure a JVM is installed and run the gradle wrapper.

    make build

## Distribute

If a standard zip file is required, just use the assembleDist command.

    make dist

This produces a zip and a tarball of the latest version.

## Run full local stack

A full local stack can be run using the provided Dockerfile and Docker
Compose configuration. The Dockerfile uses a multi-stage build so no
pre-compilation is required.

    make up

The environment can be stopped without losing any data:

    make down

Or completely removed including all data volumes:

    make destroy


## Run in an IDE

First bring up the containerized versions of hbase, aws and dks:

    make up-ancillary

Create a run configuration with the environment variable `SPRING_CONFIG_LOCATION`
pointing to `resources/application-ide.properties` and a main class of
`app.UcHistoricDataImporterApplication`, run this.


## Getting logs

The services are listed in the `docker-compose.yaml` file and logs can be
retrieved for all services, or for a subset.

    docker-compose logs aws-s3

The logs can be followed so new lines are automatically shown.

    docker-compose logs -f aws-s3

## UC laptops

This is a one time activity.

### Java

Install a JDK underneath your home directory, one way to do this is with
[sdkman](https://sdkman.io).

Once sdkman is installed and initialised you can install a jdk with e.g.:

    sdk install java 8.0.222-zulu


### Gradle wrapper

Update the project's gradle wrapper properties file to include a gradle
repository that can be accessed from a UC laptop. From the project root
directory:

    cd setup
    ./wrapper.sh ../gradle/wrapper/gradle-wrapper.properties

A backup of the original file will created at
`./gradle/wrapper/gradle-wrapper.properties.backup.1`

### Gradle.org certificates

The gradle.org certificate chain must be inserted into your local java
truststore:

    cd setup # if not already there.
    ./certificates.sh path-to-truststore
    # e.g.
    ./certificates.sh $JAVA_HOME/jre/lib/security/cacerts

..again a backup will be created at (in the example above)

`$JAVA_HOME/jre/lib/security/cacerts.backup.1`.
