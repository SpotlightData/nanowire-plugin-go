VERSION := $(shell cat VERSION)


test:
	go test -v -race -cover

version:
	godocdown > readme.md
	git tag $(VERSION)
	git push --all
	git push --tags
