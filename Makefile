SHELL:=bash
app_version=$(shell cat ./gradle.properties | cut -f2 -d'=')
s3_ready_regex=^Ready\.$
app_jar=./build/libs/uc-historic-data-importer-$(app_version).jar
certificates=%.jks

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


.PHONY: java-image
java-image: ## Build java image.
	cd docker/java && docker build --tag dwp-java:latest .

.PHONY: python-image
python-image: ## Build python image.
	cd docker/python && docker build --tag dwp-python:latest .

.PHONY: dks-image
dks-image: $(certificates) ## Build the dks image.
	docker-compose build dks


.PHONY: s3-init-image
s3-init-image: ## Build the image that creates the s3 bucket.
	docker-compose build s3-init


.PHONY: ancillary-images #  Build the supporting images.
ancillary-images: java-image python-image dks-image s3-init-image  ## Build base images

build-jar: ## Build the jar file
	./gradlew clean build

dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: build-all
build-all: build-jar build-images ## Build the jar file and then all docker images

.PHONY: build-image
build-image: $(certificates) ancillary-images build-jar ## Build all ecosystem of images
	@{ \
		echo $(app_version); \
		docker-compose build \
			--build-arg APP_VERSION=$(app_version) \
			uc-historic-data-importer; \
	}

.PHONY: up
up: ## Run the ecosystem of containers
	@{ \
		docker-compose up -d hbase s3 dks; \
		while ! docker logs s3 | grep -q $(s3_ready_regex); do \
			echo Waiting for s3.; \
			sleep 2; \
		done; \
		docker-compose up s3-init; \
		docker-compose up uc-historic-data-importer; \
	}

.PHONY: up-all
up-all: build-image up

.PHONY: destroy
destroy: ## Bring down the hbase and other services then delete all volumes
	docker-compose down
	docker network prune -f
	docker volume prune -f
