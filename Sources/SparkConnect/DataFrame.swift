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
import Atomics
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import GRPCProtobuf
import NIOCore
import SwiftyTextTable
import Synchronization

// A DataFrame which supports only SQL queries
public actor DataFrame: Sendable {
  var spark: SparkSession
  var plan: Plan
  var schema: DataType? = nil
  var batches: [RecordBatch] = [RecordBatch]()

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

  private func addBathes(_ batches: [RecordBatch]) {
    self.batches.append(contentsOf: batches)
  }

  public func rdd() throws {
    // SQLSTATE: 0A000
    // [UNSUPPORTED_CONNECT_FEATURE.RDD]
    // Feature is not supported in Spark Connect: Resilient Distributed Datasets (RDDs).
    throw SparkConnectError.UnsupportedOperationException
  }

  // We cannot expose the internal type `Spark_Connect_DataType`.
  public func schema() async throws -> String {
    var dataType: String? = nil

    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: spark.client.host, port: spark.client.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = Spark_Connect_SparkConnectService.Client(wrapping: client)
      let response = try await service.analyzePlan(
        spark.client.getAnalyzePlanRequest(spark.sessionID, plan))
      dataType = try response.schema.schema.jsonString()
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

  public func show() async throws {
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
            // The original schema should arrive before ArrowBatches
            await self.setSchema(m.schema)
          }
          let ipcStreamBytes = m.arrowBatch.data
          if !ipcStreamBytes.isEmpty {
            let IPC_CONTINUATION_TOKEN = Int32(-1)
            // Schema
            assert(ipcStreamBytes[0..<4].int32 == IPC_CONTINUATION_TOKEN)
            let schemaSize = Int64(ipcStreamBytes[4..<8].int32)
            let schema = Data(ipcStreamBytes[8..<(8 + schemaSize)])

            // Arrow IPC Data
            assert(
              ipcStreamBytes[(8 + schemaSize)..<(8 + schemaSize + 4)].int32
                == IPC_CONTINUATION_TOKEN)
            var pos: Int64 = 8 + schemaSize + 4
            let dataHeaderSize = Int64(ipcStreamBytes[pos..<(pos + 4)].int32)
            pos += 4
            let dataHeader = Data(ipcStreamBytes[pos..<(pos + dataHeaderSize)])
            pos += dataHeaderSize
            let dataBodySize = Int64(ipcStreamBytes.count) - pos - 8
            let dataBody = Data(ipcStreamBytes[pos..<(pos + dataBodySize)])

            // Read ArrowBatches
            let reader = ArrowReader()
            let arrowResult = ArrowReader.makeArrowReaderResult()
            _ = reader.fromMessage(schema, dataBody: Data(), result: arrowResult)
            _ = reader.fromMessage(dataHeader, dataBody: dataBody, result: arrowResult)
            await self.addBathes(arrowResult.batches)
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
      for batch in self.batches {
        for i in 0..<batch.length {
          var values: [String] = []
          for column in batch.columns {
            let str = column.array as! AsString
            if column.data.isNull(i) {
              values.append("NULL")
            } else {
              values.append(str.asString(i))
            }
          }
          table.addRow(values: values)
        }
      }
      print(table.render())
    }
  }
}
