.PHONY: build
build:
	cargo build --release --target x86_64-unknown-linux-gnu
	mkdir -p ./dist
	cp ./target/x86_64-unknown-linux-gnu/release/side-input ./dist/

.PHONY: image
image: build
	docker buildx build -t "quay.io/numaio/numaflow-rust/sideinput-example:v0.5.0" --platform linux/amd64,linux/arm64 --target sideinput . --push

clean:
	cargo clean
	-rm -rf ./dist
