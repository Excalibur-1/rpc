run:
	go run main.go
lint:
	golangci-lint run
sonar:
	golangci-lint run --out-format checkstyle > golangci-lint.xml & go test -v ./... -json > report.out -coverprofile=coverage.out & sonar-scanner -X
test:
	go test -v ./...
fmt:
	gofmt -s -w -l ./
git-commit:
	git commit -a
go-build:
	go build
all: go-build fmt git-commit