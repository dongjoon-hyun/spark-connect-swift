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
import Synchronization

public actor SparkSession {

  public static let builder: Builder = Builder()

  let client: Client
  public let conf: RuntimeConf

  init(_ connection: String, _ userID: String? = nil) {
    let processInfo = ProcessInfo.processInfo
    let userName = processInfo.environment["SPARK_USER"] ?? processInfo.userName
    self.client = Client(remote: connection, user: userID ?? userName)
    self.conf = RuntimeConf(self.client)
  }

  // This is supposed to be overwritten by the Spark Connect Servier's Spark version
  public var version: String = ""

  func setVersion(_ version: String) {
    self.version = version
  }

  var sessionID: String = UUID().uuidString

  func getSessionID() -> String {
    sessionID
  }

  var serverSideSessionID: String = ""

  var sparkContext: SparkContext {
    get throws {
      // SQLSTATE: 0A000
      // [UNSUPPORTED_CONNECT_FEATURE.SESSION_SPARK_CONTEXT]
      // Feature is not supported in Spark Connect: Access to the SparkContext.
      throw SparkConnectError.UnsupportedOperationException
    }
  }

  public func stop() async {
    await client.stop()
  }

  public func range(_ end: Int64) async throws -> DataFrame {
    return try await range(0, end)
  }

  public func range(_ start: Int64, _ end: Int64, _ step: Int64 = 1) async throws -> DataFrame {
    return try await DataFrame(spark: self, plan: client.getPlanRange(start, end, step))
  }

  public func sql(_ sqlText: String) async throws -> DataFrame {
    return try await DataFrame(spark: self, sqlText: sqlText)
  }

  // Although this is `UNSUPPORTED_CONNECT_FEATURE.SESSION_SPARK_CONTEXT`,
  // it's defined as the return type of `SparkSession.sparkContext` method.
  struct SparkContext {
  }

  public actor Builder {
    var sparkConf: [String: String] = [:]

    public func config(_ key: String, _ value: String) -> Builder {
      sparkConf[key] = value
      return self
    }

    func clear() -> Builder {
      sparkConf.removeAll()
      return self
    }

    public func remote(_ url: String) -> Builder {
      return config("spark.remote", url)
    }

    public func appName(_ name: String) -> Builder {
      return config("spark.app.name", name)
    }

    func enableHiveSupport() -> Builder {
      return config("spark.sql.catalogImplementation", "hive")
    }

    func create() async throws -> SparkSession {
      let session = SparkSession(sparkConf["spark.remote"] ?? "sc://localhost:15002")
      let response = try await session.client.connect(session.sessionID)
      await session.setVersion(response.sparkVersion.version)
      let isSuccess = try await session.client.setConf(map: sparkConf)
      assert(isSuccess)
      return session
    }

    public func getOrCreate() async throws -> SparkSession {
      return try await create()
    }
  }
}
