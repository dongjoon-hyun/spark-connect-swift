# Apache Spark Connect Client for Swift

[![Swift Version Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fdongjoon-hyun%2Fspark-connect-swift%2Fbadge%3Ftype%3Dswift-versions)](https://swiftpackageindex.com/dongjoon-hyun/spark-connect-swift)
[![Platform Compatibility](https://img.shields.io/endpoint?url=https%3A%2F%2Fswiftpackageindex.com%2Fapi%2Fpackages%2Fdongjoon-hyun%2Fspark-connect-swift%2Fbadge%3Ftype%3Dplatforms)](https://swiftpackageindex.com/dongjoon-hyun/spark-connect-swift)
[![GitHub Actions Build](https://github.com/dongjoon-hyun/spark-connect-swift/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/dongjoon-hyun/spark-connect-swift/blob/main/.github/workflows/build_and_test.yml)

This is an experimental Swift library to show how to connect to a remote Apache Spark Connect Server and run SQL statements to manipulate remote data.

So far, this library project is tracking the upstream changes like the [Apache Spark](https://spark.apache.org) 4.0.0 RC2 release and [Apache Arrow](https://arrow.apache.org) project's Swift-support.

## Requirement
- [Apache Spark 4.0.0 RC2 (March 2025)](https://dist.apache.org/repos/dist/dev/spark/v4.0.0-rc2-bin/)
- [Swift 6.0 (2024)](https://swift.org)
- [gRPC Swift 2.0 (Jaunary 2025)](https://github.com/grpc/grpc-swift/releases/tag/2.0.0)
- [gRPC Swift Protobuf 1.0 (January 2025)](https://github.com/grpc/grpc-swift-protobuf/releases/tag/1.0.0)
- [gRPC Swift NIO Transport 1.0 (January 2025)](https://github.com/grpc/grpc-swift-nio-transport/releases/tag/1.0.0)
- [Apache Arrow Swift](https://github.com/apache/arrow/tree/main/swift)

## Run `Apache Spark 4.0.0 RC2 Connect Server`

    $ curl -LO https://dist.apache.org/repos/dist/dev/spark/v4.0.0-rc2-bin/spark-4.0.0-bin-hadoop3.tgz
    $ tar xvfz spark-4.0.0-bin-hadoop3.tgz
    $ cd spark-4.0.0-bin-hadoop3
    $ sbin/start-connect-server.sh

## Run tests

```
$ cd spark-connect-swift
$ swift test
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
    .package(url: "https://github.com/dongjoon-hyun/spark-connect-swift.git", branch: "main")
  ],
  targets: [
    .executableTarget(
      name: "SparkConnectSwiftApp",
      dependencies: [.product(name: "SparkConnect", package: "spark-connect-swift")]
    )
  ]
)
```

Use `SparkSession` of `SparkConnect` module in Swift.

```
$ cat Sources/main.swift

import SparkConnect

let spark = try await SparkSession.builder.getOrCreate()
print("Connected to Apache Spark \(await spark.version) Server")

let statements = [
  "DROP TABLE IF EXISTS t",
  "CREATE TABLE IF NOT EXISTS t(a INT)",
  "INSERT INTO t VALUES (1), (2), (3)",
]

for s in statements {
  print("EXECUTE: \(s)")
  _ = try await spark.sql(s).count()
}
print("SELECT * FROM t")
try await spark.sql("SELECT * FROM t").show()

await spark.stop()
```

You can find this example in the following repository.
- https://github.com/dongjoon-hyun/spark-connect-swift-app
