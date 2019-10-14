SHELL:=bash

APP_VERSION=$(shell cat ./gradle.properties | cut -f2 -d'=')
S3_READY_REGEX=^Ready\.$

default: help

.PHONY: help
help:
	@{ \
		grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | \
		sort | \
		awk \
		'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'; \
	}

%.jks:
	./truststores.sh

build-base-images: ## Build base images to avoid rebuilding frequently
	@{ \
		pushd docker; \
		cp ../settings.gradle.kts ../gradle.properties . ; \
		docker build --tag dwp-kotlin-slim-gradle:latest --file Dockerfile_java_gradle_base . ; \
		rm settings.gradle.kts gradle.properties ; \
		popd; \
	}

.PHONY: java-image
java-image: ## Build java image.
	cd docker/java && docker build --tag dwp-java:latest .

.PHONY: python-image
python-image: ## Build python image.
	cd docker/python && docker build --tag dwp-python:latest .

.PHONY: dks-image
dks-image: ## Build the dks image.
	docker-compose build dks

.PHONY: dks-insecure-image
dks-insecure-image: ## Build the dks image.
	docker-compose build dks-insecure


.PHONY: s3-init-image
s3-init-image: ## Build the image that creates the s3 bucket.
	docker-compose build s3-init

.PHONY: integration-test-image
integration-test-image: ## Build the image for integration tests.
	docker-compose build integration-test

.PHONY: add-containers-to-hosts
add-containers-to-hosts:
	./add-containers-to-hosts.sh

.PHONY: ancillary-images
ancillary-images: build-base-images java-image python-image dks-image dks-insecure-image s3-init-image  ## Build base images

build-jar: ## Build the jar.
	./gradlew clean build

dist: ## Assemble distribution files in build/dist.
	./gradlew assembleDist

.PHONY: build-all
build-all: build-jar build-image ## Build the jar file and the images.

.PHONY: build-image
build-image: ancillary-images build-jar ## Build all ecosystem of images
	docker-compose build --no-cache --build-arg APP_VERSION=$(APP_VERSION) uc-historic-data-importer

.PHONY: up-ancillary
up-ancillary: ## Bring up supporting containers (hbase, aws, dks)
	docker-compose up -d zookeeper hbase s3 dks dks-insecure
	@{ \
		while ! docker logs s3 2> /dev/null | grep -q $(S3_READY_REGEX); do \
		echo Waiting for s3.; \
		sleep 2; \
		done; \
	}
	docker-compose up s3-init

.PHONY: up
up: build-image up-ancillary ## Run the ecosystem of containers.
	docker-compose up uc-historic-data-importer

.PHONY: up-all
up-all: build-image up

.PHONY: integration
integration: destroy up-all integration-test-image
	docker-compose run --rm integration-test

.PHONY: destroy
destroy: ## Bring everything down and clean up.
	docker-compose down
	docker network prune -f
	docker volume prune -f
