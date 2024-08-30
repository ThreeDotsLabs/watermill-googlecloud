### Testing Locally

The tests expect a running instance of PubSub. You can run the emulator specified in the `docker-compose.yml`, then set the correct env variable by running the following:

```
make up
PUBSUB_EMULATOR_HOST=localhost:8085 make test
```

Running one test:

```
make up
PUBSUB_EMULATOR_HOST=localhost:8085 go test -v ./... -run TestPublishSubscribe/TestContinueAfterSubscribeClose
```
