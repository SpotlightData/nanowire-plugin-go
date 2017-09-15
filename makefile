VERSION := $(shell cat VERSION)


test:
	go test -v -race -cover

docs: readme.md
	godocdown > readme.md
	git add readme.md
	git commit -m"godocdown auto commit"

version: docs
	git tag $(VERSION)
	git push --all
	git push --tags
