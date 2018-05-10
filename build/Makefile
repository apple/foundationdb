.PHONY: docker
docker:
	docker build -t fdbuild .
	docker run --rm -it -v '$(dir $(CURDIR)):/foundationdb' fdbuild make -C /foundationdb
