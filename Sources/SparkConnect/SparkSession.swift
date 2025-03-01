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

  let client: Client = Client()

  // This is supposed to be overwritten by the Spark Connect Servier's Spark version
  public var version: String = ""

  func setVersion(_ version: String) {
    self.version = version
  }

  var userContext: UserContext? = nil

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

  public func stop() {
    client.stop()
  }

  func range(_ end: Int64) async throws -> DataFrame {
    return try await range(0, end)
  }

  func range(_ start: Int64, _ end: Int64, _ step: Int64 = 1) async throws -> DataFrame {
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

    func config(_ key: String, _ value: String) -> Builder {
      sparkConf[key] = value
      return self
    }

    func remote(_ url: String) -> Builder {
      return config("spark.remote", url)
    }

    func appName(_ name: String) -> Builder {
      return config("spark.app.name", name)
    }

    func enableHiveSupport() -> Builder {
      return config("spark.sql.catalogImplementation", "hive")
    }

    func create() async throws -> SparkSession {
      let session = SparkSession()
      let response = try await session.client.connect(session.sessionID)
      await session.setVersion(response.sparkVersion.version)
      try await session.client.setConf(sessionID: session.sessionID, map: sparkConf)
      return session
    }

    public func getOrCreate() async throws -> SparkSession {
      return try await create()
    }
  }
}
