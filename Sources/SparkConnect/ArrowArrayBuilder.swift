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
public protocol ArrowArrayHolderBuilder {
  func toHolder() throws -> ArrowArrayHolder
  func appendAny(_ val: Any?)
}

public class ArrowArrayBuilder<T: ArrowBufferBuilder, U: ArrowArray<T.ItemType>>:
  ArrowArrayHolderBuilder
{
  let type: ArrowType
  let bufferBuilder: T
  public var length: UInt { return self.bufferBuilder.length }
  public var capacity: UInt { return self.bufferBuilder.capacity }
  public var nullCount: UInt { return self.bufferBuilder.nullCount }
  public var offset: UInt { return self.bufferBuilder.offset }

  fileprivate init(_ type: ArrowType) throws {
    self.type = type
    self.bufferBuilder = try T()
  }

  public func append(_ vals: T.ItemType?...) {
    for val in vals {
      self.bufferBuilder.append(val)
    }
  }

  public func append(_ vals: [T.ItemType?]) {
    for val in vals {
      self.bufferBuilder.append(val)
    }
  }

  public func append(_ val: T.ItemType?) {
    self.bufferBuilder.append(val)
  }

  public func appendAny(_ val: Any?) {
    self.bufferBuilder.append(val as? T.ItemType)
  }

  public func finish() throws -> ArrowArray<T.ItemType> {
    let buffers = self.bufferBuilder.finish()
    let arrowData = try ArrowData(self.type, buffers: buffers, nullCount: self.nullCount)
    let array = try U(arrowData)
    return array
  }

  public func getStride() -> Int {
    return self.type.getStride()
  }

  public func toHolder() throws -> ArrowArrayHolder {
    return try ArrowArrayHolderImpl(self.finish())
  }
}

public class NumberArrayBuilder<T>: ArrowArrayBuilder<FixedBufferBuilder<T>, FixedArray<T>> {
  fileprivate convenience init() throws {
    try self.init(ArrowType(ArrowType.infoForNumericType(T.self)))
  }
}

public class StringArrayBuilder: ArrowArrayBuilder<VariableBufferBuilder<String>, StringArray> {
  fileprivate convenience init() throws {
    try self.init(ArrowType(ArrowType.ArrowString))
  }
}

public class BinaryArrayBuilder: ArrowArrayBuilder<VariableBufferBuilder<Data>, BinaryArray> {
  fileprivate convenience init() throws {
    try self.init(ArrowType(ArrowType.ArrowBinary))
  }
}

public class BoolArrayBuilder: ArrowArrayBuilder<BoolBufferBuilder, BoolArray> {
  fileprivate convenience init() throws {
    try self.init(ArrowType(ArrowType.ArrowBool))
  }
}

public class Date32ArrayBuilder: ArrowArrayBuilder<Date32BufferBuilder, Date32Array> {
  fileprivate convenience init() throws {
    try self.init(ArrowType(ArrowType.ArrowDate32))
  }
}

public class Date64ArrayBuilder: ArrowArrayBuilder<Date64BufferBuilder, Date64Array> {
  fileprivate convenience init() throws {
    try self.init(ArrowType(ArrowType.ArrowDate64))
  }
}

public class Time32ArrayBuilder: ArrowArrayBuilder<FixedBufferBuilder<Time32>, Time32Array> {
  fileprivate convenience init(_ unit: ArrowTime32Unit) throws {
    try self.init(ArrowTypeTime32(unit))
  }
}

public class Time64ArrayBuilder: ArrowArrayBuilder<FixedBufferBuilder<Time64>, Time64Array> {
  fileprivate convenience init(_ unit: ArrowTime64Unit) throws {
    try self.init(ArrowTypeTime64(unit))
  }
}

public class Decimal128ArrayBuilder: ArrowArrayBuilder<FixedBufferBuilder<Decimal>, Decimal128Array>
{
  fileprivate convenience init(precision: Int32, scale: Int32) throws {
    try self.init(ArrowTypeDecimal128(precision: precision, scale: scale))
  }
}

public class TimestampArrayBuilder: ArrowArrayBuilder<FixedBufferBuilder<Int64>, TimestampArray> {
  fileprivate convenience init(_ unit: ArrowTimestampUnit, timezone: String? = nil) throws {
    try self.init(ArrowTypeTimestamp(unit, timezone: timezone))
  }
}

public class StructArrayBuilder: ArrowArrayBuilder<StructBufferBuilder, StructArray> {
  let builders: [any ArrowArrayHolderBuilder]
  let fields: [ArrowField]
  public init(_ fields: [ArrowField], builders: [any ArrowArrayHolderBuilder]) throws {
    self.fields = fields
    self.builders = builders
    try super.init(ArrowNestedType(ArrowType.ArrowStruct, fields: fields))
    self.bufferBuilder.initializeTypeInfo(fields)
  }

  public init(_ fields: [ArrowField]) throws {
    self.fields = fields
    var builders = [any ArrowArrayHolderBuilder]()
    for field in fields {
      builders.append(try ArrowArrayBuilders.loadBuilder(arrowType: field.type))
    }

    self.builders = builders
    try super.init(ArrowNestedType(ArrowType.ArrowStruct, fields: fields))
  }

  public override func append(_ values: [Any?]?) {
    self.bufferBuilder.append(values)
    if let anyValues = values {
      for index in 0..<builders.count {
        self.builders[index].appendAny(anyValues[index])
      }
    } else {
      for index in 0..<builders.count {
        self.builders[index].appendAny(nil)
      }
    }
  }

  public override func finish() throws -> StructArray {
    let buffers = self.bufferBuilder.finish()
    var childData = [ArrowData]()
    for builder in self.builders {
      childData.append(try builder.toHolder().array.arrowData)
    }

    let arrowData = try ArrowData(
      self.type, buffers: buffers,
      children: childData, nullCount: self.nullCount,
      length: self.length)
    let structArray = try StructArray(arrowData)
    return structArray
  }
}

public class ArrowArrayBuilders {
  public static func loadBuilder(  // swiftlint:disable:this cyclomatic_complexity
    _ builderType: Any.Type
  ) throws -> ArrowArrayHolderBuilder {
    if builderType == Int8.self || builderType == Int8?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int8>
    } else if builderType == Int16.self || builderType == Int16?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int16>
    } else if builderType == Int32.self || builderType == Int32?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int32>
    } else if builderType == Int64.self || builderType == Int64?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Int64>
    } else if builderType == Float.self || builderType == Float?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Float>
    } else if builderType == UInt8.self || builderType == UInt8?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<UInt8>
    } else if builderType == UInt16.self || builderType == UInt16?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<UInt16>
    } else if builderType == UInt32.self || builderType == UInt32?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<UInt32>
    } else if builderType == UInt64.self || builderType == UInt64?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<UInt64>
    } else if builderType == Double.self || builderType == Double?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder() as NumberArrayBuilder<Double>
    } else if builderType == String.self || builderType == String?.self {
      return try ArrowArrayBuilders.loadStringArrayBuilder()
    } else if builderType == Bool.self || builderType == Bool?.self {
      return try ArrowArrayBuilders.loadBoolArrayBuilder()
    } else if builderType == Date.self || builderType == Date?.self {
      return try ArrowArrayBuilders.loadDate64ArrayBuilder()
    } else if builderType == Decimal.self || builderType == Decimal?.self {
      return try ArrowArrayBuilders.loadDecimal128ArrayBuilder(38, 18)
    } else {
      throw ArrowError.invalid("Invalid type for builder: \(builderType)")
    }
  }

  public static func isValidBuilderType<T>(_ type: T.Type) -> Bool {
    return type == Int8?.self || type == Int16?.self || type == Int32?.self || type == Int64?.self
      || type == UInt8?.self || type == UInt16?.self || type == UInt32?.self || type == UInt64?.self
      || type == String?.self || type == Double?.self || type == Float?.self || type == Date?.self
      || type == Bool?.self || type == Bool.self || type == Int8.self || type == Int16.self
      || type == Int32.self || type == Int64.self || type == UInt8.self || type == UInt16.self
      || type == UInt32.self || type == UInt64.self || type == String.self || type == Double.self
      || type == Float.self || type == Date.self || type == Decimal.self || type == Decimal?.self
  }

  public static func loadStructArrayBuilderForType<T>(_ obj: T) throws -> StructArrayBuilder {
    let mirror = Mirror(reflecting: obj)
    var builders = [ArrowArrayHolderBuilder]()
    var fields = [ArrowField]()
    for (property, value) in mirror.children {
      guard let propertyName = property else {
        continue
      }

      let builderType = type(of: value)
      let arrowType = ArrowType(ArrowType.infoForType(builderType))
      fields.append(ArrowField(propertyName, type: arrowType, isNullable: true))
      builders.append(try loadBuilder(arrowType: arrowType))
    }

    return try StructArrayBuilder(fields, builders: builders)
  }

  public static func loadBuilder(  // swiftlint:disable:this cyclomatic_complexity
    arrowType: ArrowType
  ) throws -> ArrowArrayHolderBuilder {
    switch arrowType.id {
    case .uint8:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt8>
    case .uint16:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt16>
    case .uint32:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt32>
    case .uint64:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt64>
    case .int8:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int8>
    case .int16:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int16>
    case .int32:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int32>
    case .int64:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int64>
    case .double:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Double>
    case .float:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Float>
    case .string:
      return try StringArrayBuilder()
    case .boolean:
      return try BoolArrayBuilder()
    case .binary:
      return try BinaryArrayBuilder()
    case .date32:
      return try Date32ArrayBuilder()
    case .date64:
      return try Date64ArrayBuilder()
    case .time32:
      guard let timeType = arrowType as? ArrowTypeTime32 else {
        throw ArrowError.invalid("Expected arrow type for \(arrowType.id) not found")
      }
      return try Time32ArrayBuilder(timeType.unit)
    case .time64:
      guard let timeType = arrowType as? ArrowTypeTime64 else {
        throw ArrowError.invalid("Expected arrow type for \(arrowType.id) not found")
      }
      return try Time64ArrayBuilder(timeType.unit)
    case .decimal128:
      guard let decimalType = arrowType as? ArrowTypeDecimal128 else {
        throw ArrowError.invalid("Expected ArrowTypeDecimal128 for decimal128 type")
      }
      return try Decimal128ArrayBuilder(precision: decimalType.precision, scale: decimalType.scale)
    case .timestamp:
      guard let timestampType = arrowType as? ArrowTypeTimestamp else {
        throw ArrowError.invalid("Expected arrow type for \(arrowType.id) not found")
      }
      return try TimestampArrayBuilder(timestampType.unit)
    default:
      throw ArrowError.unknownType("Builder not found for arrow type: \(arrowType.id)")
    }
  }

  public static func loadNumberArrayBuilder<T>() throws -> NumberArrayBuilder<T> {
    let type = T.self
    if type == Int8.self {
      return try NumberArrayBuilder<T>()
    } else if type == Int16.self {
      return try NumberArrayBuilder<T>()
    } else if type == Int32.self {
      return try NumberArrayBuilder<T>()
    } else if type == Int64.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt8.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt16.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt32.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt64.self {
      return try NumberArrayBuilder<T>()
    } else if type == Float.self {
      return try NumberArrayBuilder<T>()
    } else if type == Double.self {
      return try NumberArrayBuilder<T>()
    } else if type == Decimal.self {
      return try NumberArrayBuilder<T>()
    } else {
      throw ArrowError.unknownType("Type is invalid for NumberArrayBuilder")
    }
  }

  public static func loadStringArrayBuilder() throws -> StringArrayBuilder {
    return try StringArrayBuilder()
  }

  public static func loadBoolArrayBuilder() throws -> BoolArrayBuilder {
    return try BoolArrayBuilder()
  }

  public static func loadDate32ArrayBuilder() throws -> Date32ArrayBuilder {
    return try Date32ArrayBuilder()
  }

  public static func loadDate64ArrayBuilder() throws -> Date64ArrayBuilder {
    return try Date64ArrayBuilder()
  }

  public static func loadBinaryArrayBuilder() throws -> BinaryArrayBuilder {
    return try BinaryArrayBuilder()
  }

  public static func loadTime32ArrayBuilder(_ unit: ArrowTime32Unit) throws -> Time32ArrayBuilder {
    return try Time32ArrayBuilder(unit)
  }

  public static func loadTime64ArrayBuilder(_ unit: ArrowTime64Unit) throws -> Time64ArrayBuilder {
    return try Time64ArrayBuilder(unit)
  }

  public static func loadTimestampArrayBuilder(_ unit: ArrowTimestampUnit, timezone: String? = nil)
    throws -> TimestampArrayBuilder
  {
    return try TimestampArrayBuilder(unit, timezone: timezone)
  }

  public static func loadDecimal128ArrayBuilder(
    _ precision: Int32 = 38,
    _ scale: Int32 = 18
  ) throws -> Decimal128ArrayBuilder {
    return try Decimal128ArrayBuilder(precision: precision, scale: scale)
  }
}
