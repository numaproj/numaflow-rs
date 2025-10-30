TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/all-sinks:${TAG}
DOCKER_FILE_PATH = examples/all-sinks/Dockerfile

.PHONY: update
update:
	cargo check
	cargo update

.PHONY: image
image: update
	cd ../../ && \
	docker build -f ${DOCKER_FILE_PATH}  -t ${IMAGE_REGISTRY} --target all-sinks --platform linux/arm64 . --load
	@if [ "$(PUSH)" = "true" ]; then docker push ${IMAGE_REGISTRY}; fi
