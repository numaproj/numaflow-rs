TAG ?= stable
PUSH ?= false

.PHONY: build
build:
	cargo build --release --bin stream-sorter

.PHONY: image
image: build
	docker build -t quay.io/numaio/numaflow-rs/stream-sorter:${TAG} -f examples/stream-sorter/Dockerfile .
	@if [ "$(PUSH)" = "true" ]; then docker push quay.io/numaio/numaflow-rs/stream-sorter:${TAG}; fi

.PHONY: clean
clean:
	cargo clean
