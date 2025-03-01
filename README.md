# Apache Spark Connect Client for Swift

This is an experimental Swift library to show how to connect to a remote Apache Spark Connect Server and run SQL statements to manipulate remote data.

So far, this library project is tracking the upstream changes like the official [Apache Spark](https://spark.apache.org) 4.0 release and [Apache Arrow](https://arrow.apache.org) project's Swift-support. 

## Requirement
- [Apache Spark 4.0.0 RC2 (March 2025)](https://dist.apache.org/repos/dist/dev/spark/v4.0.0-rc2-bin/)
- [Swift 6.0 (2024)](https://swift.org)
- [gRPC Swift 2.0 (Jaunary 2025)](https://github.com/grpc/grpc-swift/releases/tag/2.0.0)
- [gRPC Swift Protobuf 1.0 (January 2025)](https://github.com/grpc/grpc-swift-protobuf/releases/tag/1.0.0)
- [gRPC Swift NIO Transport 1.0](https://github.com/grpc/grpc-swift-nio-transport/releases/tag/1.0.0)
- [Apache Arrow Swift](https://github.com/apache/arrow/tree/main/swift)

## Run `Apache Spark 4.0 Connect Server`

    $ curl -LO https://dist.apache.org/repos/dist/dev/spark/v4.0.0-rc2-bin/spark-4.0.0-bin-hadoop3.tgz
    $ tar xvfz spark-4.0.0-bin-hadoop3.tgz
    $ cd spark-4.0.0-bin-hadoop3
    $ sbin/start-connect-server.sh

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

## How to use in your apps

Create a Swift project.
```
$ mkdir SparkConnectSwiftApp
$ cd SparkConnectSwiftApp
$ swift package init --name SparkConnectSwiftApp --type executable
```

Add `SparkConnect` package to the dependency like the following
```
$ cat Package.swift
import PackageDescription

let package = Package(
    name: "SparkConnectSwiftApp",
    platforms: [
      .macOS(.v15)
    ],
    dependencies: [
      .package(url: "git@github.com:dongjoon-hyun/spark-connect-swift.git", from: "0.1.0")
    ],
    targets: [
        .executableTarget(
            name: "SparkConnectSwiftApp",
            dependencies: [ .product(name: "SparkConnect", package: "spark-connect-swift") ]
        )
    ]
)
```

Use `SparkSession` of `SparkConnect` module in Swift.

```
$ cat Sources/main.swift

import SparkConnect

let spark = try await SparkConnect.SparkSession.builder.getOrCreate()
print("Connected to Apache Spark \(try await spark.version) Server")

let statements = [
  "DROP TABLE IF EXISTS t",
  "CREATE TABLE IF NOT EXISTS t(a INT)",
  "INSERT INTO t VALUES (1), (2), (3)",
]

for s in statements {
  print("EXECUTE: \(s)")
  try await spark.sql(s).count()
}
print("SELECT * FROM t")
print(try await spark.sql("SELECT * FROM t").count())

await spark.stop()
```

You can find this example in the following repository.
- https://github.com/dongjoon-hyun/spark-connect-swift-app
