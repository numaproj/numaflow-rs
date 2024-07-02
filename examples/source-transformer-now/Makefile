TAG ?= stable
PUSH ?= false
REPO = quay.io/numaio/numaflow-rs/source-transformer-now:${TAG}
DOCKER_FILE_PATH = examples/source-transformer-now/Dockerfile

.PHONY: update
update:
	cargo check
	cargo update

.PHONY: image
image: update
	cd ../../ && docker build \
	-f ${DOCKER_FILE_PATH}  \
	-t ${REPO} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${REPO}; fi

.PHONY: clean
clean:
	-rm -rf target
