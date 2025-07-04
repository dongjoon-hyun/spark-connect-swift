// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Foundation

/// @nodoc
public protocol ArrowArrayHolder {
  var type: ArrowType { get }
  var length: UInt { get }
  var nullCount: UInt { get }
  var array: AnyArray { get }
  var data: ArrowData { get }
  var getBufferData: () -> [Data] { get }
  var getBufferDataSizes: () -> [Int] { get }
  var getArrowColumn: (ArrowField, [ArrowArrayHolder]) throws -> ArrowColumn { get }
}

/// @nodoc
public class ArrowArrayHolderImpl: ArrowArrayHolder {
  public let data: ArrowData
  public let type: ArrowType
  public let length: UInt
  public let nullCount: UInt
  public let array: AnyArray
  public let getBufferData: () -> [Data]
  public let getBufferDataSizes: () -> [Int]
  public let getArrowColumn: (ArrowField, [ArrowArrayHolder]) throws -> ArrowColumn
  public init<T>(_ arrowArray: ArrowArray<T>) {
    self.array = arrowArray
    self.data = arrowArray.arrowData
    self.length = arrowArray.length
    self.type = arrowArray.arrowData.type
    self.nullCount = arrowArray.nullCount
    self.getBufferData = { () -> [Data] in
      var bufferData = [Data]()
      for buffer in arrowArray.arrowData.buffers {
        bufferData.append(Data())
        buffer.append(to: &bufferData[bufferData.count - 1])
      }

      return bufferData
    }

    self.getBufferDataSizes = { () -> [Int] in
      var bufferDataSizes = [Int]()
      for buffer in arrowArray.arrowData.buffers {
        bufferDataSizes.append(Int(buffer.capacity))
      }

      return bufferDataSizes
    }

    self.getArrowColumn = {
      (field: ArrowField, arrayHolders: [ArrowArrayHolder]) throws -> ArrowColumn in
      var arrays = [ArrowArray<T>]()
      for arrayHolder in arrayHolders {
        if let array = arrayHolder.array as? ArrowArray<T> {
          arrays.append(array)
        }
      }

      return ArrowColumn(field, chunked: ChunkedArrayHolder(try ChunkedArray<T>(arrays)))
    }
  }

  public static func loadArray(  // swiftlint:disable:this cyclomatic_complexity
    _ arrowType: ArrowType, with: ArrowData
  ) throws -> ArrowArrayHolder {
    switch arrowType.id {
    case .int8:
      return try ArrowArrayHolderImpl(FixedArray<Int8>(with))
    case .int16:
      return try ArrowArrayHolderImpl(FixedArray<Int16>(with))
    case .int32:
      return try ArrowArrayHolderImpl(FixedArray<Int32>(with))
    case .int64:
      return try ArrowArrayHolderImpl(FixedArray<Int64>(with))
    case .uint8:
      return try ArrowArrayHolderImpl(FixedArray<UInt8>(with))
    case .uint16:
      return try ArrowArrayHolderImpl(FixedArray<UInt16>(with))
    case .uint32:
      return try ArrowArrayHolderImpl(FixedArray<UInt32>(with))
    case .uint64:
      return try ArrowArrayHolderImpl(FixedArray<UInt64>(with))
    case .double:
      return try ArrowArrayHolderImpl(FixedArray<Double>(with))
    case .float:
      return try ArrowArrayHolderImpl(FixedArray<Float>(with))
    case .decimal128:
      return try ArrowArrayHolderImpl(FixedArray<Decimal>(with))
    case .date32:
      return try ArrowArrayHolderImpl(Date32Array(with))
    case .date64:
      return try ArrowArrayHolderImpl(Date64Array(with))
    case .time32:
      return try ArrowArrayHolderImpl(Time32Array(with))
    case .time64:
      return try ArrowArrayHolderImpl(Time64Array(with))
    case .timestamp:
      return try ArrowArrayHolderImpl(TimestampArray(with))
    case .string:
      return try ArrowArrayHolderImpl(StringArray(with))
    case .boolean:
      return try ArrowArrayHolderImpl(BoolArray(with))
    case .binary:
      return try ArrowArrayHolderImpl(BinaryArray(with))
    case .strct:
      return try ArrowArrayHolderImpl(StructArray(with))
    default:
      throw ArrowError.invalid("Array not found for type: \(arrowType)")
    }
  }
}

/// @nodoc
public class ArrowArray<T>: AsString, AnyArray {
  public typealias ItemType = T
  public let arrowData: ArrowData
  public var nullCount: UInt { return self.arrowData.nullCount }
  public var length: UInt { return self.arrowData.length }

  public required init(_ arrowData: ArrowData) throws {
    self.arrowData = arrowData
  }

  public func isNull(_ at: UInt) throws -> Bool {
    if at >= self.length {
      throw ArrowError.outOfBounds(index: Int64(at))
    }

    return self.arrowData.isNull(at)
  }

  public subscript(_ index: UInt) -> T? {
    fatalError("subscript() has not been implemented")
  }

  public func asString(_ index: UInt) -> String {
    if self[index] == nil {
      return ""
    }

    return "\(self[index]!)"
  }

  public func asAny(_ index: UInt) -> Any? {
    if self[index] == nil {
      return nil
    }

    return self[index]!
  }
}

/// @nodoc
public class FixedArray<T>: ArrowArray<T> {
  public override subscript(_ index: UInt) -> T? {
    if self.arrowData.isNull(index) {
      return nil
    }

    let byteOffset = self.arrowData.stride * Int(index)
    return self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(as: T.self)
  }
}

/// @nodoc
public class StringArray: ArrowArray<String> {
  public override subscript(_ index: UInt) -> String? {
    let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
    if self.arrowData.isNull(index) {
      return nil
    }

    let offsets = self.arrowData.buffers[1]
    let values = self.arrowData.buffers[2]

    var startIndex: Int32 = 0
    if index > 0 {
      startIndex = offsets.rawPointer.advanced(by: offsetIndex).load(as: Int32.self)
    }

    let endIndex = offsets.rawPointer.advanced(by: offsetIndex + MemoryLayout<Int32>.stride)
      .load(as: Int32.self)
    let arrayLength = Int(endIndex - startIndex)
    let rawPointer = values.rawPointer.advanced(by: Int(startIndex))
      .bindMemory(to: UInt8.self, capacity: arrayLength)
    let buffer = UnsafeBufferPointer<UInt8>(start: rawPointer, count: arrayLength)
    let byteArray = Array(buffer)
    return String(data: Data(byteArray), encoding: .utf8)
  }
}

/// @nodoc
public class BoolArray: ArrowArray<Bool> {
  public override subscript(_ index: UInt) -> Bool? {
    if self.arrowData.isNull(index) {
      return nil
    }

    let valueBuffer = self.arrowData.buffers[1]
    return BitUtility.isSet(index, buffer: valueBuffer)
  }
}

/// @nodoc
public class Date32Array: ArrowArray<Date> {
  public override subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }

    let byteOffset = self.arrowData.stride * Int(index)
    let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(
      as: UInt32.self)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds * 86400))
  }
}

/// @nodoc
public class Date64Array: ArrowArray<Date> {
  public override subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }

    let byteOffset = self.arrowData.stride * Int(index)
    let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(
      as: UInt64.self)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
  }
}

/// @nodoc
public class Time32Array: FixedArray<Time32> {}
/// @nodoc
public class Time64Array: FixedArray<Time64> {}

/// @nodoc
public class Decimal128Array: FixedArray<Decimal> {
  public override subscript(_ index: UInt) -> Decimal? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let scale: Int32 =
      switch self.arrowData.type.id {
      case .decimal128(_, let scale):
        scale
      default:
        18
      }
    let byteOffset = self.arrowData.stride * Int(index)
    let value = self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(
      as: UInt64.self)
    return Decimal(value) / pow(10, Int(scale))
  }
}

public class TimestampArray: FixedArray<Timestamp> {

  public struct FormattingOptions: Equatable {
    public var dateFormat: String = "yyyy-MM-dd HH:mm:ss.SSS"
    public var locale: Locale = .current
    public var includeTimezone: Bool = true
    public var fallbackToRaw: Bool = true

    public init(
      dateFormat: String = "yyyy-MM-dd HH:mm:ss.SSS",
      locale: Locale = .current,
      includeTimezone: Bool = true,
      fallbackToRaw: Bool = true
    ) {
      self.dateFormat = dateFormat
      self.locale = locale
      self.includeTimezone = includeTimezone
      self.fallbackToRaw = fallbackToRaw
    }

    public static func == (lhs: FormattingOptions, rhs: FormattingOptions) -> Bool {
      return lhs.dateFormat == rhs.dateFormat && lhs.locale.identifier == rhs.locale.identifier
        && lhs.includeTimezone == rhs.includeTimezone && lhs.fallbackToRaw == rhs.fallbackToRaw
    }
  }

  private var cachedFormatter: DateFormatter?
  private var cachedOptions: FormattingOptions?

  public func formattedDate(at index: UInt, options: FormattingOptions = FormattingOptions())
    -> String?
  {
    guard let timestamp = self[index] else { return nil }

    guard let timestampType = self.arrowData.type as? ArrowTypeTimestamp else {
      return options.fallbackToRaw ? "\(timestamp)" : nil
    }

    let date = dateFromTimestamp(timestamp, unit: timestampType.unit)

    if cachedFormatter == nil || cachedOptions != options {
      let formatter = DateFormatter()
      formatter.dateFormat = options.dateFormat
      formatter.locale = options.locale
      if options.includeTimezone, let timezone = timestampType.timezone {
        formatter.timeZone = TimeZone(identifier: timezone)
      }
      cachedFormatter = formatter
      cachedOptions = options
    }

    return cachedFormatter?.string(from: date)
  }

  private func dateFromTimestamp(_ timestamp: Int64, unit: ArrowTimestampUnit) -> Date {
    let timeInterval: TimeInterval

    switch unit {
    case .seconds:
      timeInterval = TimeInterval(timestamp)
    case .milliseconds:
      timeInterval = TimeInterval(timestamp) / 1_000
    case .microseconds:
      timeInterval = TimeInterval(timestamp) / 1_000_000
    case .nanoseconds:
      timeInterval = TimeInterval(timestamp) / 1_000_000_000
    }

    return Date(timeIntervalSince1970: timeInterval)
  }

  public override func asString(_ index: UInt) -> String {
    if let formatted = formattedDate(at: index) {
      return formatted
    }

    return super.asString(index)
  }
}

/// @nodoc
public class BinaryArray: ArrowArray<Data> {
  public struct Options {
    public var printAsHex = false
    public var printEncoding: String.Encoding = .utf8
  }

  public var options = Options()

  public override subscript(_ index: UInt) -> Data? {
    let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
    if self.arrowData.isNull(index) {
      return nil
    }

    let offsets = self.arrowData.buffers[1]
    let values = self.arrowData.buffers[2]
    var startIndex: Int32 = 0
    if index > 0 {
      startIndex = offsets.rawPointer.advanced(by: offsetIndex).load(as: Int32.self)
    }

    let endIndex = offsets.rawPointer.advanced(by: offsetIndex + MemoryLayout<Int32>.stride)
      .load(as: Int32.self)
    let arrayLength = Int(endIndex - startIndex)
    let rawPointer = values.rawPointer.advanced(by: Int(startIndex))
      .bindMemory(to: UInt8.self, capacity: arrayLength)
    let buffer = UnsafeBufferPointer<UInt8>(start: rawPointer, count: arrayLength)
    let byteArray = Array(buffer)
    return Data(byteArray)
  }

  public override func asString(_ index: UInt) -> String {
    if self[index] == nil { return "" }
    let data = self[index]!
    if options.printAsHex {
      return data.hexEncodedString()
    } else {
      return String(data: data, encoding: .utf8)!
    }
  }
}

/// @nodoc
public class StructArray: ArrowArray<[Any?]> {
  public private(set) var arrowFields: [ArrowArrayHolder]?
  public required init(_ arrowData: ArrowData) throws {
    try super.init(arrowData)
    var fields = [ArrowArrayHolder]()
    for child in arrowData.children {
      fields.append(try ArrowArrayHolderImpl.loadArray(child.type, with: child))
    }

    self.arrowFields = fields
  }

  public override subscript(_ index: UInt) -> [Any?]? {
    if self.arrowData.isNull(index) {
      return nil
    }

    if let fields = arrowFields {
      var result = [Any?]()
      for field in fields {
        result.append(field.array.asAny(index))
      }

      return result
    }

    return nil
  }

  public override func asString(_ index: UInt) -> String {
    if self.arrowData.isNull(index) {
      return ""
    }

    var output = "{"
    if let fields = arrowFields {
      for fieldIndex in 0..<fields.count {
        let asStr = fields[fieldIndex].array as? AsString
        if fieldIndex == 0 {
          output.append("\(asStr!.asString(index))")
        } else {
          output.append(",\(asStr!.asString(index))")
        }
      }
    }

    output += "}"
    return output
  }
}
