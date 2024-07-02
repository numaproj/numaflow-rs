.PHONY: update
update:
	cargo update

.PHONY: image
image: update
	cd ../../ && docker build -f examples/map-cat/Dockerfile -t map-cat .