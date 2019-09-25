SHELL:=bash
app_version=$(shell cat ./gradle.properties | cut -f2 -d'=')

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
java-image: ## basic image with java installed
	@{ \
		cd docker/java; \
		docker build --tag dwp-java:latest .; \
	}

.PHONY: python-image
python-image:
	@{ \
		cd docker/python; \
		docker build --tag dwp-python:latest .; \
	}

.PHONY: dks-image
dks-image: %.jks ## basic image with java installed
	@{ \
		cd docker/dks; \
		docker build --tag dks:latest .; \
	}

.PHONY: ancillary-images
ancillary-images: java-image python-image dks-image  ## Build base images to avoid rebuilding frequently

build-jar: ## Build the jar file
	./gradlew clean build

dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: build-all
build-all: build-jar build-images ## Build the jar file and then all docker images


# .PHONY: ancillary-images
# ancillary-images: java-image python-image dks-image ## Build base images to avoid rebuilding frequently

.PHONY: build-images
build-images: ancillary-images ## Build all ecosystem of images
	@{ \
		 docker-compose build \
			--build-arg APP_VERSION=$(app_version) \
			uc-historic-data-importer; \
	}

.PHONY: up
up: ## Run the ecosystem of containers
	@{ \
		docker-compose up -d hbase aws-s3; \
		docker-compose up uc-historic-data-importer; \
	}

.PHONY: up-all
up-all: build-images up

.PHONY: destroy
destroy: ## Bring down the hbase and other services then delete all volumes
	docker-compose down
	docker network prune -f
	docker volume prune -f
