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
import Testing

import SparkConnect

/// A test suite for various SQL statements.
struct SQLTests {
  let fm = FileManager.default
  let path = Bundle.module.path(forResource: "queries", ofType: "")!
  let encoder = JSONEncoder()

  let regexID = /#\d+L?/
  let regexPlanId = /plan_id=\d+/
  let regexLocation = /file:[a-zA-Z0-9\.\-\/\\]+/
  let regexOwner = /(runner|185)/

  private func cleanUp(_ str: String) -> String {
    return removeOwner(removeID(removeLocation(str)))
  }

  private func removeID(_ str: String) -> String {
    return str.replacing(regexPlanId, with: "plan_id=").replacing(regexID, with: "#")
  }

  private func removeLocation(_ str: String) -> String {
    return str.replacing(regexLocation, with: "*")
  }

  private func removeOwner(_ str: String) -> String {
    return str.replacing(regexOwner, with: "*")
  }

  @Test
  func testRemoveID() {
    #expect(removeID("123") == "123")
    #expect(removeID("123L") == "123L")
    #expect(removeID("#123") == "#")
    #expect(removeID("#123L") == "#")
    #expect(removeID("plan_id=123") == "plan_id=")
  }

  @Test
  func removeLocation() {
    #expect(removeLocation("file:/abc") == "*")
  }

  @Test
  func removeOwner() {
    #expect(removeOwner("runner") == "*")
    #expect(removeOwner("185") == "*")
  }

#if !os(Linux)
  @Test
  func runAll() async throws {
    let spark = try await SparkSession.builder.getOrCreate()
    for name in try! fm.contentsOfDirectory(atPath: path).sorted() {
      guard name.hasSuffix(".sql") else { continue }
      print(name)

      let sql = try String(contentsOf: URL(fileURLWithPath: "\(path)/\(name)"), encoding: .utf8)
      let jsonData = try encoder.encode(try await spark.sql(sql).collect())
      let answer = cleanUp(String(data: jsonData, encoding: .utf8)!)
      let expected = cleanUp(try String(contentsOf: URL(fileURLWithPath: "\(path)/\(name).json"), encoding: .utf8))
      #expect(answer == expected.trimmingCharacters(in: .whitespacesAndNewlines))
    }
    await spark.stop()
  }
#endif
}
