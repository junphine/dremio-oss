package com.dremio.plugins.mongo.execution;

import com.dremio.exec.vector.complex.fn.*;
import org.apache.arrow.memory.*;
import com.dremio.exec.physical.base.*;
import com.dremio.common.expression.*;
import com.google.common.base.*;
import java.util.*;
import java.io.*;
import com.dremio.plugins.mongo.planning.rels.*;
import org.bson.types.*;
import com.dremio.common.exceptions.*;
import java.nio.*;
import org.bson.*;
import org.apache.arrow.vector.complex.writer.*;
import java.nio.charset.*;
import java.math.*;
import org.apache.arrow.vector.holders.*;
import com.dremio.exec.catalog.*;
import org.slf4j.*;

public class BsonRecordReader
{
    static final Logger logger;
    private final FieldSelection fieldSelection;
    private final int maxFieldSize;
    private final int maxLeafLimit;
    private final boolean readNumbersAsDouble;
    protected ArrowBuf workBuf;
    private final Map<String, Integer> fieldDecimalScale;
    protected int currentLeafCount;
    
    public BsonRecordReader(final ArrowBuf managedBuf, final int maxFieldSize, final int maxLeafLimit, final boolean allTextMode, final boolean readNumbersAsDouble) {
        this(managedBuf, (List<SchemaPath>)GroupScan.ALL_COLUMNS, maxFieldSize, maxLeafLimit, readNumbersAsDouble, null);
    }
    
    public BsonRecordReader(final ArrowBuf managedBuf, final List<SchemaPath> sanitizedColumns, final int maxFieldSize, final int maxLeafLimit, final boolean readNumbersAsDouble, final Map<String, Integer> decimalScales) {
        assert (Preconditions.checkNotNull(sanitizedColumns)).size() > 0 : "bson record reader requires at least a column";
        this.maxFieldSize = maxFieldSize;
        this.maxLeafLimit = maxLeafLimit;
        this.readNumbersAsDouble = readNumbersAsDouble;
        this.workBuf = managedBuf;
        this.fieldSelection = FieldSelection.getFieldSelection(sanitizedColumns);
        this.fieldDecimalScale = ((null == decimalScales) ? new HashMap<String, Integer>() : decimalScales);
        this.currentLeafCount = 0;
    }
    
    public void write(final BaseWriter.ComplexWriter writer, final BsonReader reader) throws IOException {
        this.currentLeafCount = 0;
        reader.readStartDocument();
        final BsonType readBsonType = reader.getCurrentBsonType();
        switch (readBsonType) {
            case DOCUMENT: {
                this.writeToListOrMap(reader, (FieldWriter)writer.rootAsStruct(), false, null, this.fieldSelection);
                break;
            }
            default: {
                throw new RuntimeException("Root object must be DOCUMENT type. Found: " + readBsonType);
            }
        }
    }
    
    private void writeToListOrMap(final BsonReader reader, final FieldWriter writer, final boolean isList, String fieldName, final FieldSelection fieldSelection) {
        if (isList) {
            writer.startList();
        }
        else {
            writer.start();
        }
        final int originalLeafCount = this.currentLeafCount;
        int maxArrayLeafCount = 0;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            if (!isList) {
                fieldName = reader.readName();
            }
            final BsonType currentBsonType = reader.getCurrentBsonType();
            final FieldSelection childFieldSelection = fieldSelection.getChild(fieldName);
            if (childFieldSelection.isNeverValid() && fieldName.equals("_id")) {
                reader.skipValue();
            }
            else {
                fieldName = MongoColumnNameSanitizer.unsanitizeColumnName(fieldName);
                if (isList) {
                    this.currentLeafCount = originalLeafCount;
                }
                switch (currentBsonType) {
                    case INT32: {
                        final int readInt32 = reader.readInt32();
                        if (this.readNumbersAsDouble) {
                            this.writeDouble(readInt32, writer, fieldName, isList);
                            break;
                        }
                        this.writeInt32(readInt32, writer, fieldName, isList);
                        break;
                    }
                    case INT64: {
                        final long readInt33 = reader.readInt64();
                        if (this.readNumbersAsDouble) {
                            this.writeDouble(readInt33, writer, fieldName, isList);
                            break;
                        }
                        this.writeInt64(readInt33, writer, fieldName, isList);
                        break;
                    }
                    case ARRAY: {
                        reader.readStartArray();
                        if (isList) {
                            this.writeToListOrMap(reader, (FieldWriter)writer.list(), true, fieldName, childFieldSelection);
                            break;
                        }
                        this.writeToListOrMap(reader, (FieldWriter)writer.list(fieldName), true, fieldName, childFieldSelection);
                        break;
                    }
                    case BINARY: {
                        this.writeBinary(reader, writer, fieldName, isList);
                        break;
                    }
                    case BOOLEAN: {
                        final boolean readBoolean = reader.readBoolean();
                        this.writeBoolean(readBoolean, writer, fieldName, isList);
                        break;
                    }
                    case DATE_TIME: {
                        final long readDateTime = reader.readDateTime();
                        this.writeDateTime(readDateTime, writer, fieldName, isList);
                        break;
                    }
                    case DOCUMENT: {
                        reader.readStartDocument();
                        FieldWriter _writer;
                        if (!isList) {
                            _writer = (FieldWriter)writer.struct(fieldName);
                        }
                        else {
                            _writer = (FieldWriter)writer.struct();
                        }
                        this.writeToListOrMap(reader, _writer, false, fieldName, childFieldSelection);
                        break;
                    }
                    case DECIMAL128: {
                        final Decimal128 decimal = reader.readDecimal128();
                        this.writeDecimal(decimal.bigDecimalValue(), writer, fieldName, isList);
                        break;
                    }
                    case DOUBLE: {
                        final double readDouble = reader.readDouble();
                        this.writeDouble(readDouble, writer, fieldName, isList);
                        break;
                    }
                    case JAVASCRIPT: {
                        final String readJavaScript = reader.readJavaScript();
                        this.writeString(readJavaScript, writer, fieldName, isList);
                        break;
                    }
                    case JAVASCRIPT_WITH_SCOPE: {
                        this.writeJavaScriptWithScope(reader, isList ? writer.struct() : writer.struct(fieldName));
                        break;
                    }
                    case NULL: {
                        reader.readNull();
                        break;
                    }
                    case OBJECT_ID: {
                        this.writeObjectId(reader, writer, fieldName, isList);
                        break;
                    }
                    case STRING: {
                        final String readString = reader.readString();
                        this.writeString(readString, writer, fieldName, isList);
                        break;
                    }
                    case SYMBOL: {
                        final String readSymbol = reader.readSymbol();
                        this.writeString(readSymbol, writer, fieldName, isList);
                        break;
                    }
                    case TIMESTAMP: {
                        final int time = reader.readTimestamp().getTime();
                        this.writeTimeStampMilli(time, writer, fieldName, isList);
                        break;
                    }
                    case DB_POINTER: {
                        this.writeDbPointer(reader.readDBPointer(), isList ? writer.struct() : writer.struct(fieldName));
                        break;
                    }
                    case REGULAR_EXPRESSION: {
                        final BaseWriter.StructWriter structWriter = isList ? writer.struct() : writer.struct(fieldName);
                        final BsonRegularExpression regex = reader.readRegularExpression();
                        structWriter.start();
                        this.writeString(regex.getPattern(), (FieldWriter)structWriter, "pattern", false);
                        this.writeString(regex.getOptions(), (FieldWriter)structWriter, "options", false);
                        structWriter.end();
                        break;
                    }
                    case MIN_KEY: {
                        reader.readMinKey();
                        break;
                    }
                    case MAX_KEY: {
                        reader.readMaxKey();
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("UnSupported Bson type: " + currentBsonType);
                    }
                }
                if (!isList) {
                    continue;
                }
                maxArrayLeafCount = Math.max(maxArrayLeafCount, this.currentLeafCount);
            }
        }
        if (!isList) {
            reader.readEndDocument();
            writer.end();
        }
        else {
            this.currentLeafCount = maxArrayLeafCount;
            reader.readEndArray();
            writer.endList();
        }
    }
    
    private void writeDbPointer(final BsonDbPointer dbPointer, final BaseWriter.StructWriter structWriter) {
        this.writeString(dbPointer.getNamespace(), (FieldWriter)structWriter, "namespace", false);
        this.incrementLeafCount();
        final byte[] objBytes = dbPointer.getId().toByteArray();
        this.ensure(objBytes.length);
        this.workBuf.setBytes(0L, objBytes);
        structWriter.varBinary("id").writeVarBinary(0, objBytes.length, this.workBuf);
    }
    
    private void writeJavaScriptWithScope(final BsonReader reader, final BaseWriter.StructWriter writer) {
        writer.start();
        this.writeString(reader.readJavaScriptWithScope(), (FieldWriter)writer, "code", false);
        reader.readStartDocument();
        final FieldWriter scopeWriter = (FieldWriter)writer.struct("scope");
        this.writeToListOrMap(reader, scopeWriter, false, "scope", FieldSelection.ALL_VALID);
        writer.end();
    }
    
    private void writeBinary(final BsonReader reader, final FieldWriter writer, final String fieldName, final boolean isList) {
        final VarBinaryHolder vb = new VarBinaryHolder();
        FieldSizeLimitExceptionHelper.checkSizeLimit(reader.peekBinarySize(), this.maxFieldSize, fieldName, BsonRecordReader.logger);
        final BsonBinary readBinaryData = reader.readBinaryData();
        final byte[] data = readBinaryData.getData();
        final Byte type = readBinaryData.getType();
        switch (type) {
            case 1: {
                this.writeDouble(ByteBuffer.wrap(data).getDouble(), writer, fieldName, isList);
                break;
            }
            case 2: {
                this.writeString(new String(data), writer, fieldName, isList);
                break;
            }
            case 8: {
                final boolean boolValue = data != null && data.length != 0 && data[0] != 0;
                this.writeBoolean(boolValue, writer, fieldName, isList);
                break;
            }
            case 9: {
                this.writeDateTime(ByteBuffer.wrap(data).getLong(), writer, fieldName, isList);
                break;
            }
            case 13: {
                this.writeString(new String(data), writer, fieldName, isList);
                break;
            }
            case 14: {
                this.writeString(new String(data), writer, fieldName, isList);
                break;
            }
            case 15: {
                this.writeString(new String(data), writer, fieldName, isList);
                break;
            }
            case 16: {
                this.writeInt32(ByteBuffer.wrap(data).getInt(), writer, fieldName, isList);
                break;
            }
            case 17: {
                this.writeTimeStampMilli(ByteBuffer.wrap(data).getInt(), writer, fieldName, isList);
                break;
            }
            case 18: {
                this.writeInt64(ByteBuffer.wrap(data).getInt(), writer, fieldName, isList);
                break;
            }
            default: {
                final byte[] bytes = readBinaryData.getData();
                this.writeBinary(writer, fieldName, isList, vb, bytes);
                break;
            }
        }
    }
    
    private void writeTimeStampMilli(final int timestamp, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        TimeStampMilliWriter t;
        if (!isList) {
            t = writer.timeStampMilli(fieldName);
        }
        else {
            t = writer.timeStampMilli();
        }
        t.writeTimeStampMilli(timestamp * 1000L);
    }
    
    private void writeString(final String readString, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        final byte[] readStringBytes = readString.getBytes(StandardCharsets.UTF_8);
        FieldSizeLimitExceptionHelper.checkSizeLimit(readStringBytes.length, this.maxFieldSize, fieldName, BsonRecordReader.logger);
        this.ensure(readStringBytes.length);
        this.workBuf.setBytes(0L, readStringBytes);
        if (!isList) {
            writer.varChar(fieldName).writeVarChar(0, readStringBytes.length, this.workBuf);
        }
        else {
            writer.varChar().writeVarChar(0, readStringBytes.length, this.workBuf);
        }
    }
    
    private void writeObjectId(final BsonReader reader, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        final VarBinaryHolder vObj = new VarBinaryHolder();
        final byte[] objBytes = reader.readObjectId().toByteArray();
        this.writeBinary(writer, fieldName, isList, vObj, objBytes);
    }
    
    private void writeDecimal(BigDecimal readDecimal, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        if (readDecimal.precision() > 38 && readDecimal.scale() > 6) {
            readDecimal = readDecimal.setScale(Math.max(6, readDecimal.scale() - (readDecimal.precision() - 38)), 5);
        }
        final Integer scale = this.fieldDecimalScale.get(fieldName);
        if (scale == null) {
            this.fieldDecimalScale.put(fieldName, readDecimal.scale());
        }
        else if (readDecimal.scale() < scale) {
            readDecimal = readDecimal.setScale(scale);
        }
        else if (readDecimal.scale() > scale) {
            BsonRecordReader.logger.debug("Scale change detected on field {}, from {} to {}", new Object[] { fieldName, scale, readDecimal.scale() });
            this.fieldDecimalScale.put(fieldName, readDecimal.scale());
            throw new ChangedScaleException();
        }
        if (!isList) {
            writer.decimal(fieldName, readDecimal.scale(), 38).writeDecimal(readDecimal);
        }
        else {
            writer.decimal().writeDecimal(readDecimal);
        }
    }
    
    private void writeDouble(final double readDouble, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        if (!isList) {
            writer.float8(fieldName).writeFloat8(readDouble);
        }
        else {
            writer.float8().writeFloat8(readDouble);
        }
    }
    
    private void writeDateTime(final long readDateTime, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        TimeStampMilliWriter dt;
        if (!isList) {
            dt = writer.timeStampMilli(fieldName);
        }
        else {
            dt = writer.timeStampMilli();
        }
        dt.writeTimeStampMilli(readDateTime);
    }
    
    private void writeBoolean(final boolean readBoolean, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        final BitHolder bit = new BitHolder();
        bit.value = (readBoolean ? 1 : 0);
        if (!isList) {
            writer.bit(fieldName).writeBit(bit.value);
        }
        else {
            writer.bit().writeBit(bit.value);
        }
    }
    
    private void writeBinary(final FieldWriter writer, final String fieldName, final boolean isList, final VarBinaryHolder vb, final byte[] bytes) {
        this.incrementLeafCount();
        FieldSizeLimitExceptionHelper.checkSizeLimit(bytes.length, this.maxFieldSize, fieldName, BsonRecordReader.logger);
        this.ensure(bytes.length);
        this.workBuf.setBytes(0L, bytes);
        vb.buffer = this.workBuf;
        vb.start = 0;
        vb.end = bytes.length;
        if (!isList) {
            writer.varBinary(fieldName).writeVarBinary(vb.start, vb.end, vb.buffer);
        }
        else {
            writer.varBinary().writeVarBinary(vb.start, vb.end, vb.buffer);
        }
    }
    
    private void writeInt64(final long readInt64, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        if (!isList) {
            writer.bigInt(fieldName).writeBigInt(readInt64);
        }
        else {
            writer.bigInt().writeBigInt(readInt64);
        }
    }
    
    private void writeInt32(final int readInt32, final FieldWriter writer, final String fieldName, final boolean isList) {
        this.incrementLeafCount();
        if (!isList) {
            writer.integer(fieldName).writeInt(readInt32);
        }
        else {
            writer.integer().writeInt(readInt32);
        }
    }
    
    private void ensure(final int length) {
        this.workBuf = this.workBuf.reallocIfNeeded((long)length);
    }
    
    Map<String, Integer> getDecimalScales() {
        return this.fieldDecimalScale;
    }
    
    private void incrementLeafCount() {
        if (++this.currentLeafCount > this.maxLeafLimit) {
            throw new ColumnCountTooLargeException(this.maxLeafLimit);
        }
    }
    
    static {
        logger = LoggerFactory.getLogger(BsonRecordReader.class);
    }
    
    public static class ChangedScaleException extends RuntimeException
    {
    }
}
