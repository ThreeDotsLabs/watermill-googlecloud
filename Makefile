include: .env

up:
	docker-compose up -d

test:
	go test ./...

test_v:
	go test -v ./...

test_short:
	go test ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -timeout=45m ./...

test_reconnect:
	go test -tags=reconnect ./...

wait:
	go run github.com/ThreeDotsLabs/wait-for@latest localhost:8085

build:
	go build ./...

fmt:
	go fmt ./...
	goimports -l -w .

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

