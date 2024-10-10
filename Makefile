# Description: Makefile for Rust projects

# perform a cargo fmt on all directories containing a Cargo.toml file
.PHONY: lint
# find all directories containing Cargo.toml files
DIRS := $(shell find . -type f -name Cargo.toml -not -path "./target/*" -exec dirname {} \; | sort -u)
$(info Included directories: $(DIRS))
fmt:
	@for dir in $(DIRS); do \
		echo "Formatting code in $$dir"; \
		cargo fmt --all --manifest-path "$$dir/Cargo.toml"; \
	done

# Check if all files are formatted and run clippy on all directories containing a Cargo.toml file
.PHONY: lint
lint: test-fmt clippy

.PHONY: test-fmt
test-fmt:
	@for dir in $(DIRS); do \
		echo "Checking if code is formatted in directory: $$dir"; \
		cargo fmt --all --check --manifest-path "$$dir/Cargo.toml" || { echo "Code is not formatted in $$dir"; exit 1; }; \
	done

.PHONY: clippy
clippy:
	@for dir in $(DIRS); do \
		echo "Running clippy in directory: $$dir"; \
		cargo clippy --workspace --manifest-path "$$dir/Cargo.toml" -- -D warnings || { echo "Clippy warnings/errors found in $$dir"; exit 1; }; \
	done

# run cargo test on the repository root
.PHONY: test
test:
	cargo test --workspace

.PHONY: codegen
codegen:
	# Change timestamps so that tonic_build code generation will always be triggered.
	touch proto/*
	PROTO_CODE_GEN=1 cargo build
