.PHONY: lint fmt test-fmt clippy test codegen

fmt:
	cargo fmt --all

.PHONY: lint
lint: test-fmt clippy

.PHONY: test-fmt
test-fmt:
	cargo fmt --all --check

.PHONY: clippy
clippy:
	cargo clippy --workspace -- -D warnings

# run cargo test on the repository root
.PHONY: test
test:
	cargo test --workspace

.PHONY: codegen
codegen:
	# Change timestamps so that tonic_build code generation will always be triggered.
	cd numaflow && mkdir -p src/servers && touch proto/* && PROTO_CODE_GEN=1 cargo build
