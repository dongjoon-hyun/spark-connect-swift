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

/// Utility functions like `org.apache.spark.sql.connect.common.ProtoUtils`.
public enum ProtoUtils {

  private static let SPARK_JOB_TAGS_SEP = ","  // SparkContext.SPARK_JOB_TAGS_SEP

  /// Validate if a tag for ExecutePlanRequest.tags is valid. Throw IllegalArgumentException if not.
  /// - Parameter tag: A tag string.
  public static func throwIfInvalidTag(_ tag: String) throws {
    // Same format rules apply to Spark Connect execution tags as to SparkContext job tags,
    // because the Spark Connect job tag is also used as part of SparkContext job tag.
    // See SparkContext.throwIfInvalidTag and ExecuteHolderSessionTag
    if tag.isEmpty {
      throw SparkConnectError.InvalidArgument
    }
    if tag.contains(SPARK_JOB_TAGS_SEP) {
      throw SparkConnectError.InvalidArgument
    }
  }
}
