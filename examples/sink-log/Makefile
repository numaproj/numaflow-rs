TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/sink-log:${TAG}
DOCKER_FILE_PATH = examples/sink-log/Dockerfile

.PHONY: update
update:
	cargo check
	cargo update

.PHONY: image
image: update
	cd ../../ && \
	docker build -f ${DOCKER_FILE_PATH}  -t ${IMAGE_REGISTRY} --target sink-log --platform linux/arm64 . --load
	@if [ "$(PUSH)" = "true" ]; then docker push ${IMAGE_REGISTRY}; fi
