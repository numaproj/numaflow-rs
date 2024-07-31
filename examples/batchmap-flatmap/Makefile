TAG ?= stable
PUSH ?= true
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/batchmap-flatmap:${TAG}
DOCKER_FILE_PATH = examples/batchmap-flatmap/Dockerfile

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

.PHONY: clean
clean:
	-rm -rf target
