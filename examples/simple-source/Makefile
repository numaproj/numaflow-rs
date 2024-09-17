TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/simple-source:${TAG}
DOCKER_FILE_PATH = examples/simple-source/Dockerfile

.PHONY: update
update:
	cargo check
	cargo update

.PHONY: image
image: update
	cd ../../ && docker buildx build \
	-f ${DOCKER_FILE_PATH}  \
	-t ${IMAGE_REGISTRY} .  --platform linux/amd64,linux/arm64 --push

.PHONY: clean
clean:
	-rm -rf target
