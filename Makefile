SHELL:=bash

app_version=$(shell cat ./gradle.properties | cut -f2 -d'=')

default: help

.PHONY: help
help:
	@{ \
		grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		sort | \
		awk \
		'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'; \
	}
build-jar: ## Build the jar file
	./gradlew clean build

dist: ## Assemble distribution files in build/dist
	./gradlew assembleDist

.PHONY: build-all
build-all: build-jar build-images ## Build the jar file and then all docker images

.PHONY: build-base-images
build-base-images: ## Build base images to avoid rebuilding frequently
	@{ \
		pushd docker; \
		docker build --tag dwp-centos-with-java:latest \
			--file Dockerfile_centos_java . ; \
		docker build --tag dwp-python-preinstall:latest \
			--file Dockerfile_python_preinstall . ; \
		popd; \
	}

.PHONY: build-images
build-images: build-base-images ## Build all ecosystem of images
	@{ \
		 docker-compose build \
			--build-arg APP_VERSION=$(app_version) \
			uc-historic-data-importer; \
	}

.PHONY: up
up: ## Run the ecosystem of containers
	@{ \
		docker-compose up -d uc-historic-data-importer; \
	}

.PHONY: up-all
up-all: build-images up

.PHONY: destroy
destroy: ## Bring down the hbase and other services then delete all volumes
	docker-compose down
	docker network prune -f
	docker volume prune -f
