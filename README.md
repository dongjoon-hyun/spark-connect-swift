# Apache Spark Connect Client for Swift

## Requirement
- Apache Spark 4.0.0 RC2
- Swift 6.0
- gRPC Swift 2.0.0
- gRPC Swift Protobuf 1.0.0
- gRPC Swift NIO Transport 1.0.0

## Download the latest Apache Spark 4.0.0 RC2 binary distribution and run `Spark Connect Server`.

```
$ curl -LO https://dist.apache.org/repos/dist/dev/spark/v4.0.0-rc2-bin/spark-4.0.0-bin-hadoop3.tgz
$ tar xvfz spark-4.0.0-bin-hadoop3.tgz
$ cd spark-4.0.0-bin-hadoop3
$ sbin/start-connect-server.sh
```

## Run tests

```
$ cd spark-connect-swift
$ swift test
Building for debugging...
[3/3] Write swift-version--58304C5D6DBC2206.txt
Build complete! (7.97s)
Test Suite 'All tests' started at 2025-02-28 19:05:21.157.
Test Suite 'All tests' passed at 2025-02-28 19:05:21.159.
	 Executed 0 tests, with 0 failures (0 unexpected) in 0.000 (0.002) seconds
􀟈  Test run started.
􀄵  Testing Library Version: 102 (arm64e-apple-macos13.0)
􀟈  Suite BuilderTests started.
􀟈  Suite SparkSessionTests started.
􀟈  Suite DataFrameTests started.
􀟈  Test version() started.
􀟈  Test table() started.
􀟈  Test sparkContext() started.
􀟈  Test range() started.
􀟈  Test schema() started.
􀟈  Test stop() started.
􀟈  Test show() started.
􀟈  Test count() started.
􀟈  Test builderDefault() started.
􁁛  Test builderDefault() passed after 0.325 seconds.
􀟈  Test remote() started.
􁁛  Test version() passed after 0.326 seconds.
􁁛  Test stop() passed after 0.326 seconds.
􁁛  Test remote() passed after 0.001 seconds.
􀟈  Test appName() started.
􁁛  Test sparkContext() passed after 0.326 seconds.
􁁛  Test show() passed after 0.326 seconds.
􁁛  Test appName() passed after 0.011 seconds.
􁁛  Suite BuilderTests passed after 0.337 seconds.
􁁛  Test schema() passed after 0.986 seconds.
􁁛  Test count() passed after 1.696 seconds.
􁁛  Test range() passed after 1.735 seconds.
􁁛  Suite SparkSessionTests passed after 1.736 seconds.
􁁛  Test table() passed after 2.495 seconds.
􁁛  Suite DataFrameTests passed after 2.495 seconds.
􁁛  Test run with 11 tests passed after 2.496 seconds.
```

## How to use
