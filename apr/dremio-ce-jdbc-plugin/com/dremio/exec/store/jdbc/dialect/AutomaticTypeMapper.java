package com.dremio.exec.store.jdbc.dialect;

import com.google.common.collect.*;
import com.dremio.exec.store.jdbc.*;
import org.apache.arrow.vector.types.*;
import com.dremio.common.expression.*;
import com.dremio.common.util.*;
import java.util.*;

public final class AutomaticTypeMapper extends TypeMapper
{
    public static final AutomaticTypeMapper INSTANCE;
    
    @Override
    protected List<JdbcToFieldMapping> mapSourceToArrowFields(final UnrecognizedTypeCallback unrecognizedTypeCallback, final AddPropertyCallback addColumnPropertyCallback, final List<SourceTypeDescriptor> columnInfo, final boolean mapSkippedColumnsAsNullType) {
        final ImmutableList.Builder<JdbcToFieldMapping> builder = (ImmutableList.Builder<JdbcToFieldMapping>)ImmutableList.builder();
        int jdbcColumnIndex = 0;
        final JdbcTypeConverter converter = this.useDecimalToDoubleMapping ? new JdbcTypeConverter.DecimalToDoubleJdbcTypeConverter() : new JdbcTypeConverter();
        for (final SourceTypeDescriptor column : columnInfo) {
            ++jdbcColumnIndex;
            final Types.MinorType minorType = converter.getMinorType(column.getReportedJdbcType());
            if (minorType == null) {
                unrecognizedTypeCallback.mark(column, true);
            }
            else {
                JdbcToFieldMapping newMapping;
                if (minorType == Types.MinorType.DECIMAL) {
                    newMapping = new JdbcToFieldMapping(CompleteType.fromDecimalPrecisionScale(column.getPrecision(), column.getScale()), jdbcColumnIndex, column);
                }
                else {
                    newMapping = new JdbcToFieldMapping(CompleteType.fromMinorType(MajorTypeHelper.getMinorTypeFromArrowMinorType(minorType)), jdbcColumnIndex, column);
                }
                if (addColumnPropertyCallback != null) {
                    addColumnPropertyCallback.addProperty(newMapping.getField().getName(), "sourceTypeName", newMapping.getSourceTypeDescriptor().getDataSourceTypeName());
                }
                builder.add((Object)newMapping);
            }
        }
        return (List<JdbcToFieldMapping>)builder.build();
    }
    
    private AutomaticTypeMapper() {
        super(true);
    }
    
    static {
        INSTANCE = new AutomaticTypeMapper();
    }
}
