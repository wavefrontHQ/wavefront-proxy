# "DataDog Agent -> WFProxy -> DataDog" Tests

## Build Proxy

On Proxy repo home run:

```
MVN_ARGS="-DskipTests" make build-jar docker
```

## Run test

On `tests/ddaget/` run:

```
WF_SERVER=nimba \
WF_TOKEN=XXXXX \
DD_API_KEY=XXXX \
make
```

## Test if is working

1. Go to you WF server, and serach for a metric `docker.cpu.usage`, you shoul get some series with a `dd_agent_version=7` tag, and other with a `dd_agent_version=6` tag.
2. Do the same on your Datadog acount (the `dd_agent_version` will not be available)
