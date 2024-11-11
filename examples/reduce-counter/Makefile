TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/reduce-counter:${TAG}
DOCKER_FILE_PATH = examples/reduce-counter/Dockerfile

.PHONY: update
update:
	cargo check
	cargo update

.PHONY: image
image: update
	cd ../../ && docker build \
	-f ${DOCKER_FILE_PATH}  \
	-t ${IMAGE_REGISTRY} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${IMAGE_REGISTRY}; fi
