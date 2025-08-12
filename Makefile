.PHONY: lint fmt test-fmt clippy test codegen build clean-proto codegen-clean

.PHONY: build
build:
	cargo build --workspace

fmt:
	cargo fmt --all

.PHONY: lint
lint: test-fmt clippy

.PHONY: test-fmt
test-fmt:
	cargo fmt --all --check

.PHONY: clippy
clippy:
	cargo clippy --workspace -- -D warnings -A clippy::module_inception

# run cargo test on the repository root
.PHONY: test
test:
	cargo test --workspace

.PHONY: codegen
codegen:
	# Change timestamps so that tonic_build code generation will always be triggered.
	cd numaflow && touch proto/* && PROTO_CODE_GEN=1 cargo build

.PHONY: clean-proto
clean-proto:
	# Clean generated proto files
	cd numaflow && rm -rf src/generated

.PHONY: codegen-clean
codegen-clean: clean-proto codegen
