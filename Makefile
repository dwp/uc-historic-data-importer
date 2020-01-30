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

.PHONY: bootstrap
bootstrap: ## Bootstrap local environment for first use
	make git-hooks

.PHONY: git-hooks
git-hooks: ## Set up hooks in .git/hooks
	@{ \
		HOOK_DIR=.git/hooks; \
		for hook in $(shell ls .githooks); do \
			if [ ! -h $${HOOK_DIR}/$${hook} -a -x $${HOOK_DIR}/$${hook} ]; then \
				mv $${HOOK_DIR}/$${hook} $${HOOK_DIR}/$${hook}.local; \
				echo "moved existing $${hook} to $${hook}.local"; \
			fi; \
			ln -s -f ../../.githooks/$${hook} $${HOOK_DIR}/$${hook}; \
		done \
	}

.PHONY: gradle-image
gradle-image: ## Build gradle image.
	cp settings.gradle.kts gradle.properties docker/gradle
	cd docker/gradle && docker build --tag dwp-gradle:latest .

.PHONY: java-image
java-image: ## Build java image.
	cd docker/java && docker build --tag dwp-java:latest .

.PHONY: python-image
python-image: ## Build python image.
	cd docker/python && docker build --tag dwp-python:latest .

.PHONY: dks-standalone-https-image
dks-standalone-https-image: ## Build the dks-standalone-https image.
	docker-compose build dks-standalone-https

.PHONY: dks-standalone-http-image
dks-standalone-http-image: ## Build the dks-standalone-http image.
	docker-compose build dks-standalone-http

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
ancillary-images: gradle-image java-image python-image dks-standalone-http-image dks-standalone-https-image s3-init-image  ## Build base images

build-jar: ## Build the jar.
	./gradlew clean build

dist: ## Assemble distribution files in build/dist.
	./gradlew assembleDist

.PHONY: s3-init-paging
s3-init-paging: ## Populate s3 with > 500 objects and run the importer to see paging.
	CREATE_PAGINATED_DATA=yes docker-compose up s3-init


.PHONY: build-all
build-all: build-jar build-image ## Build the jar file and the images.

.PHONY: build-image
build-image: ancillary-images build-jar ## Build all ecosystem of images
	docker-compose build --no-cache --build-arg APP_VERSION=$(APP_VERSION) uc-historic-data-importer

.PHONY: up-ancillary
up-ancillary: ## Bring up supporting containers (hbase, aws, dks)
	docker-compose up -d zookeeper hbase s3 dks-standalone-https dks-standalone-http
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
integration: up-all integration-test-image
	docker-compose run --rm integration-test

.PHONY: destroy
destroy: ## Bring everything down and clean up.
	docker-compose down
	docker network prune -f
	docker volume prune -f

.PHONY: dks-logs-https
dks-logs-https: ## Cat the logs of dks-standalone-https
	docker exec dks-standalone-https cat /opt/data-key-service/logs/dks.out

.PHONY: dks-logs-http
dks-logs-http: ## Cat the logs of dks-standalone-http
	docker exec dks-standalone-http cat /opt/data-key-service/logs/dks.out
