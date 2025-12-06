TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/redis-sink:${TAG}
DOCKER_FILE_PATH = examples/redis-sink/Dockerfile

.PHONY: update
update:
	cargo check
	cargo update

.PHONY: image
image: update
	cd ../../ && docker build \
	-f ${DOCKER_FILE_PATH}  \
	-t ${IMAGE_REGISTRY} . --load
	@if [ "$(PUSH)" = "true" ]; then docker push ${IMAGE_REGISTRY}; fi
