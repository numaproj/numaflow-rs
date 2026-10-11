TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/sink-tracing:${TAG}
DOCKER_FILE_PATH = examples/sink-tracing/Dockerfile

# Note: unlike the other examples we deliberately do NOT run `cargo update`
# before `docker build`. This example's OTel dependencies resolve to crates
# with MSRV > 1.85 in their latest patches; the host running `cargo update`
# would poison `Cargo.lock` for the sibling examples. Letting the in-container
# `cargo build` resolve the lockfile from scratch keeps things isolated.
.PHONY: image
image:
	cd ../../ && docker build \
	-f ${DOCKER_FILE_PATH}  \
	-t ${IMAGE_REGISTRY} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${IMAGE_REGISTRY}; fi

.PHONY: clean
clean:
	-rm -rf target
