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

extension String {
  var toSparkConnectPlan: Spark_Connect_Plan {
    var sql = Spark_Connect_SQL()
    sql.query = self
    var relation = Spark_Connect_Relation()
    relation.sql = sql
    var plan = Spark_Connect_Plan()
    plan.opType = Spark_Connect_Plan.OneOf_OpType.root(relation)
    return plan
  }

  var toUserContext: Spark_Connect_UserContext {
    var context = Spark_Connect_UserContext()
    context.userID = self
    context.userName = self
    return context
  }
}

extension [String: String] {
  var toSparkConnectKeyValue: [Spark_Connect_KeyValue] {
    var array = [Spark_Connect_KeyValue]()
    for keyValue in self {
      var kv = Spark_Connect_KeyValue()
      kv.key = keyValue.key
      kv.value = keyValue.value
      array.append(kv)
    }
    return array
  }
}
