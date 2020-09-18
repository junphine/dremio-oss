package com.dremio.exec.store.jdbc.dialect;

import org.apache.arrow.vector.types.pojo.*;
import com.dremio.common.expression.*;
import com.dremio.common.types.*;
import com.dremio.common.util.*;

public class JdbcToFieldMapping
{
    private final Field field;
    private final SourceTypeDescriptor source;
    private final int jdbcOrdinal;
    
    public JdbcToFieldMapping(final CompleteType type, final int jdbcColumnNumber, final SourceTypeDescriptor source) {
        if (type.isDecimal()) {
            int precision = source.getPrecision();
            int scale = source.getScale();
            if (precision > 38) {
                if (scale > 6) {
                    scale = Math.max(6, scale - (precision - 38));
                }
                precision = 38;
            }
            this.field = MajorTypeHelper.getFieldForNameAndMajorType(source.getFieldName(), Types.withScaleAndPrecision(type.toMinorType(), TypeProtos.DataMode.OPTIONAL, scale, precision));
        }
        else {
            this.field = MajorTypeHelper.getFieldForNameAndMajorType(source.getFieldName(), Types.optional(type.toMinorType()));
        }
        this.jdbcOrdinal = jdbcColumnNumber;
        this.source = source;
    }
    
    public static JdbcToFieldMapping createSkippedField(final int jdbcOrdinal, final SourceTypeDescriptor source) {
        return new JdbcToFieldMapping(CompleteType.NULL, jdbcOrdinal, source);
    }
    
    public int getJdbcOrdinal() {
        return this.jdbcOrdinal;
    }
    
    public Field getField() {
        return this.field;
    }
    
    public SourceTypeDescriptor getSourceTypeDescriptor() {
        return this.source;
    }
    
    @Override
    public String toString() {
        return String.format("JDBC Column Number: %d%n, Field: %s%n", this.jdbcOrdinal, this.field) + this.source.toString();
    }
}
