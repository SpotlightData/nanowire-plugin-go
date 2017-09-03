test:
	go test -v -race -cover

version:
	git tag $(VERSION)
	git push --all
	git push --tags
