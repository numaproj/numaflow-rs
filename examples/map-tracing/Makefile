TAG ?= stable
PUSH ?= false
IMAGE_REGISTRY = quay.io/numaio/numaflow-rs/map-tracing:${TAG}
DOCKER_FILE_PATH = examples/map-tracing/Dockerfile

# Note: unlike the other examples we deliberately do NOT run `cargo update`
# before `docker build`. This example's OTel dependencies resolve to crates
# with MSRV > 1.85 in their latest patches, which the workspace's rust:1.85
# Dockerfile cannot compile. Letting the in-container `cargo build` resolve
# the lockfile from scratch picks compatible older patches.
.PHONY: image
image:
	cd ../../ && docker build \
	-f ${DOCKER_FILE_PATH}  \
	-t ${IMAGE_REGISTRY} .
	@if [ "$(PUSH)" = "true" ]; then docker push ${IMAGE_REGISTRY}; fi

.PHONY: clean
clean:
	-rm -rf target
