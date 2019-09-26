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
