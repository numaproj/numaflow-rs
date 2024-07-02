TAG ?= stable
PUSH ?= false
REPO = quay.io/numaio/numaflow-rs/map-tickgen-serde:${TAG}
DOCKER_FILE_PATH = examples/map-tickgen-serde/Dockerfile

.PHONY: update
update:
	cargo update

.PHONY: image
image: update
	cd ../../ && docker build \
	-f ${DOCKER_FILE_PATH} \
	-t ${REPO} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${REPO}; fi
