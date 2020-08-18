export PUBSUB_EMULATOR_HOST=localhost:8085

up:
	docker-compose up -d

test: up
	go test -parallel 20 ./...

test_v: up
	go test -parallel 20 -v ./...

test_short: up
	go test -parallel 20 ./... -short

test_race: up
	go test ./... -short -race

test_stress: up
	go test -tags=stress -parallel 30 -timeout=45m ./...

test_reconnect: up
	go test -tags=reconnect ./...

fmt:
	go fmt ./...
	goimports -l -w .

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

