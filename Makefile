# Description: Makefile for Rust projects

# perform a cargo fmt on all directories containing a Cargo.toml file
.PHONY: lint
# find all directories containing Cargo.toml files
DIRS := $(shell find . -type f -name Cargo.toml -exec dirname {} \; | sort -u)
$(info Included directories: $(DIRS))
lint:
	@for dir in $(DIRS); do \
		echo "Formatting code in $$dir"; \
		cargo fmt --all --manifest-path "$$dir/Cargo.toml"; \
	done

# run cargo test on the repository root
.PHONY: test
test:
	cargo test --workspace
