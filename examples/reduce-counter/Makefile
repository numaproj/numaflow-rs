TAG ?= stable
PUSH ?= false
REPO = quay.io/numaio/numaflow-rs/reduce-counter:${TAG}
DOCKER_FILE_PATH = examples/reduce-counter/Dockerfile

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
