TAG ?= stable
PUSH ?= false
REPO = quay.io/numaio/numaflow-rs/sideinput-example:${TAG}
DOCKER_FILE_PATH = examples/sideinput/Dockerfile

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