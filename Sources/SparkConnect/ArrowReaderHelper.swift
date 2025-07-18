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

import FlatBuffers
import Foundation

/// @nodoc
private func makeBinaryHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowType = ArrowType(ArrowType.ArrowBinary)
    let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try BinaryArray(arrowData)))
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

private func makeStringHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowType = ArrowType(ArrowType.ArrowString)
    let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try StringArray(arrowData)))
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

private func makeDecimalHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowData = try ArrowData(field.type, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try Decimal128Array(arrowData)))
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

private func makeDateHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    if field.type.id == .date32 {
      let arrowData = try ArrowData(field.type, buffers: buffers, nullCount: nullCount)
      return .success(ArrowArrayHolderImpl(try Date32Array(arrowData)))
    }

    let arrowData = try ArrowData(field.type, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try Date64Array(arrowData)))
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

private func makeTimeHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    if field.type.id == .time32 {
      if let arrowType = field.type as? ArrowTypeTime32 {
        let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
        return .success(ArrowArrayHolderImpl(try FixedArray<Time32>(arrowData)))
      } else {
        return .failure(.invalid("Incorrect field type for time: \(field.type)"))
      }
    }

    if let arrowType = field.type as? ArrowTypeTime64 {
      let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
      return .success(ArrowArrayHolderImpl(try FixedArray<Time64>(arrowData)))
    } else {
      return .failure(.invalid("Incorrect field type for time: \(field.type)"))
    }
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

private func makeTimestampHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    if let arrowType = field.type as? ArrowTypeTimestamp {
      let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
      return .success(ArrowArrayHolderImpl(try TimestampArray(arrowData)))
    } else {
      return .failure(.invalid("Incorrect field type for timestamp: \(field.type)"))
    }
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

private func makeBoolHolder(
  _ buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowType = ArrowType(ArrowType.ArrowBool)
    let arrowData = try ArrowData(arrowType, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try BoolArray(arrowData)))
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

private func makeFixedHolder<T>(
  _: T.Type, field: ArrowField, buffers: [ArrowBuffer],
  nullCount: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowData = try ArrowData(field.type, buffers: buffers, nullCount: nullCount)
    return .success(ArrowArrayHolderImpl(try FixedArray<T>(arrowData)))
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

func makeStructHolder(
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData],
  rbLength: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  do {
    let arrowData = try ArrowData(
      field.type,
      buffers: buffers, children: children,
      nullCount: nullCount, length: rbLength)
    return .success(ArrowArrayHolderImpl(try StructArray(arrowData)))
  } catch let error as ArrowError {
    return .failure(error)
  } catch {
    return .failure(.unknownError("\(error)"))
  }
}

func makeArrayHolder(
  _ field: org_apache_arrow_flatbuf_Field,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData]?,
  rbLength: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  let arrowField = fromProto(field: field)
  return makeArrayHolder(
    arrowField, buffers: buffers, nullCount: nullCount, children: children, rbLength: rbLength)
}

func makeArrayHolder(  // swiftlint:disable:this cyclomatic_complexity
  _ field: ArrowField,
  buffers: [ArrowBuffer],
  nullCount: UInt,
  children: [ArrowData]?,
  rbLength: UInt
) -> Result<ArrowArrayHolder, ArrowError> {
  let typeId = field.type.id
  switch typeId {
  case .int8:
    return makeFixedHolder(Int8.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint8:
    return makeFixedHolder(UInt8.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int16:
    return makeFixedHolder(Int16.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint16:
    return makeFixedHolder(UInt16.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int32:
    return makeFixedHolder(Int32.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint32:
    return makeFixedHolder(UInt32.self, field: field, buffers: buffers, nullCount: nullCount)
  case .int64:
    return makeFixedHolder(Int64.self, field: field, buffers: buffers, nullCount: nullCount)
  case .uint64:
    return makeFixedHolder(UInt64.self, field: field, buffers: buffers, nullCount: nullCount)
  case .decimal128:
    return makeDecimalHolder(field, buffers: buffers, nullCount: nullCount)
  case .boolean:
    return makeBoolHolder(buffers, nullCount: nullCount)
  case .float:
    return makeFixedHolder(Float.self, field: field, buffers: buffers, nullCount: nullCount)
  case .double:
    return makeFixedHolder(Double.self, field: field, buffers: buffers, nullCount: nullCount)
  case .string:
    return makeStringHolder(buffers, nullCount: nullCount)
  case .binary:
    return makeBinaryHolder(buffers, nullCount: nullCount)
  case .date32, .date64:
    return makeDateHolder(field, buffers: buffers, nullCount: nullCount)
  case .time32, .time64:
    return makeTimeHolder(field, buffers: buffers, nullCount: nullCount)
  case .timestamp:
    return makeTimestampHolder(field, buffers: buffers, nullCount: nullCount)
  case .strct:
    return makeStructHolder(
      field, buffers: buffers, nullCount: nullCount, children: children!, rbLength: rbLength)
  default:
    return .failure(.unknownType("Type \(typeId) currently not supported"))
  }
}

func makeBuffer(
  _ buffer: org_apache_arrow_flatbuf_Buffer, fileData: Data,
  length: UInt, messageOffset: Int64
) -> ArrowBuffer {
  let startOffset = messageOffset + buffer.offset
  let endOffset = startOffset + buffer.length
  let bufferData = [UInt8](fileData[startOffset..<endOffset])
  return ArrowBuffer.createBuffer(bufferData, length: length)
}

func isFixedPrimitive(_ type: org_apache_arrow_flatbuf_Type_) -> Bool {
  switch type {
  case .int, .bool, .floatingpoint, .date, .time, .timestamp, .decimal:
    return true
  default:
    return false
  }
}

func findArrowType(  // swiftlint:disable:this cyclomatic_complexity function_body_length
  _ field: org_apache_arrow_flatbuf_Field
) -> ArrowType {
  let type = field.typeType
  switch type {
  case .int:
    let intType = field.type(type: org_apache_arrow_flatbuf_Int.self)!
    let bitWidth = intType.bitWidth
    if bitWidth == 8 {
      return ArrowType(intType.isSigned ? ArrowType.ArrowInt8 : ArrowType.ArrowUInt8)
    }
    if bitWidth == 16 {
      return ArrowType(intType.isSigned ? ArrowType.ArrowInt16 : ArrowType.ArrowUInt16)
    }
    if bitWidth == 32 {
      return ArrowType(intType.isSigned ? ArrowType.ArrowInt32 : ArrowType.ArrowUInt32)
    }
    if bitWidth == 64 {
      return ArrowType(intType.isSigned ? ArrowType.ArrowInt64 : ArrowType.ArrowUInt64)
    }
    return ArrowType(ArrowType.ArrowUnknown)
  case .bool:
    return ArrowType(ArrowType.ArrowBool)
  case .floatingpoint:
    let floatType = field.type(type: org_apache_arrow_flatbuf_FloatingPoint.self)!
    switch floatType.precision {
    case .single:
      return ArrowType(ArrowType.ArrowFloat)
    case .double:
      return ArrowType(ArrowType.ArrowDouble)
    default:
      return ArrowType(ArrowType.ArrowUnknown)
    }
  case .decimal:
    let dataType = field.type(type: org_apache_arrow_flatbuf_Decimal.self)!
    if dataType.bitWidth == 128 {
      return ArrowType(ArrowType.ArrowDecimal128)
    }
    return ArrowType(ArrowType.ArrowUnknown)
  case .utf8:
    return ArrowType(ArrowType.ArrowString)
  case .binary:
    return ArrowType(ArrowType.ArrowBinary)
  case .date:
    let dateType = field.type(type: org_apache_arrow_flatbuf_Date.self)!
    if dateType.unit == .day {
      return ArrowType(ArrowType.ArrowDate32)
    }

    return ArrowType(ArrowType.ArrowDate64)
  case .time:
    let timeType = field.type(type: org_apache_arrow_flatbuf_Time.self)!
    if timeType.unit == .second || timeType.unit == .millisecond {
      return ArrowTypeTime32(timeType.unit == .second ? .seconds : .milliseconds)
    }

    return ArrowTypeTime64(timeType.unit == .microsecond ? .microseconds : .nanoseconds)
  case .timestamp:
    let timestampType = field.type(type: org_apache_arrow_flatbuf_Timestamp.self)!
    let arrowUnit: ArrowTimestampUnit
    switch timestampType.unit {
    case .second:
      arrowUnit = .seconds
    case .millisecond:
      arrowUnit = .milliseconds
    case .microsecond:
      arrowUnit = .microseconds
    case .nanosecond:
      arrowUnit = .nanoseconds
    }

    let timezone = timestampType.timezone
    return ArrowTypeTimestamp(arrowUnit, timezone: timezone)
  case .struct_:
    _ = field.type(type: org_apache_arrow_flatbuf_Struct_.self)!
    var fields = [ArrowField]()
    for index in 0..<field.childrenCount {
      let childField = field.children(at: index)!
      let childType = findArrowType(childField)
      fields.append(
        ArrowField(childField.name ?? "", type: childType, isNullable: childField.nullable))
    }

    return ArrowNestedType(ArrowType.ArrowStruct, fields: fields)
  default:
    return ArrowType(ArrowType.ArrowUnknown)
  }
}

func validateBufferIndex(_ recordBatch: org_apache_arrow_flatbuf_RecordBatch, index: Int32) throws {
  if index >= recordBatch.buffersCount {
    throw ArrowError.outOfBounds(index: Int64(index))
  }
}

func validateFileData(_ data: Data) -> Bool {
  let markerLength = FILEMARKER.utf8.count
  let startString = String(decoding: data[..<markerLength], as: UTF8.self)
  let endString = String(decoding: data[(data.count - markerLength)...], as: UTF8.self)
  return startString == FILEMARKER && endString == FILEMARKER
}

func getUInt32(_ data: Data, offset: Int) -> UInt32 {
  let token = data.withUnsafeBytes { rawBuffer in
    rawBuffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self)
  }
  return token
}
