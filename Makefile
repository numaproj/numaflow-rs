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
	@echo "Running tests"
	cargo test --workspace
	@echo "Running panic tests sequentially..."
	cargo test --workspace --features test-panic sink::tests::sink_panic -- --test-threads=1
	cargo test --workspace --features test-panic sourcetransform::tests::source_transformer_panic -- --test-threads=1
	cargo test --workspace --features test-panic map::tests::map_server_panic -- --test-threads=1
	cargo test --workspace --features test-panic mapstream::tests::map_stream_server_panic -- --test-threads=1
	cargo test --workspace --features test-panic batchmap::tests::batchmap_panic -- --test-threads=1
	cargo test --workspace --features test-panic reduce::tests::panic_tests::panic_in_reduce -- --test-threads=1
	cargo test --workspace --features test-panic reduce::tests::panic_tests::panic_with_multiple_keys -- --test-threads=1
	cargo test --workspace --features test-panic session_reduce::tests::panic_tests::test_panic_in_session_reduce -- --test-threads=1
	cargo test --workspace --features test-panic shared::panic_tests::test_panic_hook_functionality -- --test-threads=1

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
