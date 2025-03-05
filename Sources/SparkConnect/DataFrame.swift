import Atomics
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import GRPCProtobuf
import SwiftyTextTable
import Synchronization

// A DataFrame which supports only SQL queries
public actor DataFrame: Sendable {
  var spark: SparkSession
  var plan: Plan
  var schema: DataType? = nil

  init(spark: SparkSession, plan: Plan) async throws {
    self.spark = spark
    self.plan = plan
  }

  init(spark: SparkSession, sqlText: String) async throws {
    self.spark = spark
    self.plan = sqlText.toSparkConnectPlan
  }

  private func setSchema(_ schema: DataType) {
    self.schema = schema
  }

  func rdd() throws {
    // SQLSTATE: 0A000
    // [UNSUPPORTED_CONNECT_FEATURE.RDD]
    // Feature is not supported in Spark Connect: Resilient Distributed Datasets (RDDs).
    throw SparkConnectError.UnsupportedOperationException
  }

  func schema() async throws -> DataType {
    var dataType: Spark_Connect_DataType? = nil

    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(
        spark.client.getAnalyzePlanRequest(spark.sessionID, plan))
      dataType = response.schema.schema
    }
    return dataType!
  }

  public func count() async throws -> Int64 {
    let counter = Atomic(Int64(0))

    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      try await service.executePlan(spark.client.getExecutePlanRequest(spark.sessionID, plan)) {
        response in
        for try await m in response.messages {
          counter.add(m.arrowBatch.rowCount, ordering: .relaxed)
        }
      }
    }
    return counter.load(ordering: .relaxed)
  }

  public func collect() async throws {
    throw SparkConnectError.UnsupportedOperationException
  }

  // TODO: Show the real data
  public func show() async throws {
    let counter = Atomic(Int64(0))

    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      try await service.executePlan(spark.client.getExecutePlanRequest(spark.sessionID, plan)) {
        response in
        for try await m in response.messages {
          if m.hasSchema {
            await self.setSchema(m.schema)
          }
          if !m.arrowBatch.data.isEmpty {
            counter.add(m.arrowBatch.rowCount, ordering: .relaxed)
          }
        }
      }
    }
    if let schema = self.schema {
      var columns: [TextTableColumn] = []
      for f in schema.struct.fields {
        columns.append(TextTableColumn(header: f.name))
      }
      var table = TextTable(columns: columns)
      for _ in 1...(counter.load(ordering: .relaxed)) {
        table.addRow(values: [""])
      }
      print(table.render())
    }
  }
}
