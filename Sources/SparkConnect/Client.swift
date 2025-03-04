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
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import GRPCProtobuf
import Synchronization

typealias AnalyzePlanRequest = Spark_Connect_AnalyzePlanRequest
typealias AnalyzePlanResponse = Spark_Connect_AnalyzePlanResponse
typealias ConfigRequest = Spark_Connect_ConfigRequest
typealias DataType = Spark_Connect_DataType
typealias ExecutePlanRequest = Spark_Connect_ExecutePlanRequest
typealias Plan = Spark_Connect_Plan
typealias Range = Spark_Connect_Range
typealias Relation = Spark_Connect_Relation
typealias SparkConnectService = Spark_Connect_SparkConnectService
typealias UserContext = Spark_Connect_UserContext

// Conceptually the remote spark session that communicates with the server
struct Client {
  let clientType: String = "swift"
  let url: URL
  let host: String
  let port: Int
  let userContext: UserContext
  let sessionID: String? = nil

  init(remote: String, user: String) {
    self.url = URL(string: remote)!
    self.host = url.host() ?? "localhost"
    self.port = self.url.port ?? 15002
    self.userContext = user.toUserContext
  }

  func stop() {
  }

  private var service: SparkConnectService.Client<HTTP2ClientTransport.Posix>? = nil

  func connect(_ sessionID: String) async throws -> AnalyzePlanResponse {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = SparkConnectService.Client(wrapping: client)
      let version = AnalyzePlanRequest.SparkVersion()
      var request = AnalyzePlanRequest()
      request.clientType = clientType
      request.userContext = userContext
      request.sessionID = sessionID
      request.analyze = .sparkVersion(version)
      let response = try await service.analyzePlan(request)
      return response
    }
  }

  func getConfigRequestSet(map: [String: String]) -> ConfigRequest {
    var request = ConfigRequest()
    request.operation = ConfigRequest.Operation()
    var set = ConfigRequest.Set()
    set.pairs = map.toSparkConnectKeyValue
    request.operation.opType = .set(set)
    return request
  }

  func setConf(sessionID: String, map: [String: String]) async throws -> Bool {
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: self.host, port: self.port),
        transportSecurity: .plaintext
      )
    ) { client in
      let service = SparkConnectService.Client(wrapping: client)
      var request = getConfigRequestSet(map: map)
      request.clientType = clientType
      request.userContext = userContext
      request.sessionID = sessionID
      let _ = try await service.config(request)
      return true
    }
  }

  func getConfigRequestGet(keys: [String]) async throws -> ConfigRequest {
    var request = ConfigRequest()
    request.operation = ConfigRequest.Operation()
    var get = ConfigRequest.Get()
    get.keys = keys
    request.operation.opType = .get(get)
    return request
  }

  func getPlanRange(_ start: Int64, _ end: Int64, _ step: Int64) -> Plan {
    var range = Range()
    range.start = start
    range.end = end
    range.step = step
    var relation = Relation()
    relation.range = range
    var plan = Plan()
    plan.opType = .root(relation)
    return plan
  }

  func getExecutePlanRequest(_ sessionID: String, _ plan: Plan) async
    -> ExecutePlanRequest
  {
    var request = ExecutePlanRequest()
    request.clientType = clientType
    request.userContext = userContext
    request.sessionID = sessionID
    request.operationID = UUID().uuidString
    request.plan = plan
    return request
  }

  func getAnalyzePlanRequest(_ sessionID: String, _ plan: Plan) async
    -> AnalyzePlanRequest
  {
    var request = AnalyzePlanRequest()
    request.clientType = clientType
    request.userContext = userContext
    request.sessionID = sessionID
    var schema = AnalyzePlanRequest.Schema()
    schema.plan = plan
    request.analyze = .schema(schema)
    return request
  }
}
