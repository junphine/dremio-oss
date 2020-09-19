package com.dremio.exec.store.jdbc.dialect;

import org.apache.calcite.rel.type.*;
import com.dremio.common.expression.*;
import com.dremio.exec.planner.sql.*;

public class SourceTypeDescriptor
{
    private final String fieldName;
    private final int reportedJdbcType;
    private final String dataSourceTypeName;
    private final int columnIndex;
    private final int precision;
    private final int scale;
    
    SourceTypeDescriptor(final String fieldName, final int reportedJdbcType, final String dataSourceName, final int colIndex, final int precision, final int scale) {
        this.fieldName = fieldName;
        this.reportedJdbcType = reportedJdbcType;
        this.dataSourceTypeName = dataSourceName;
        this.columnIndex = colIndex;
        this.precision = precision;
        this.scale = scale;
    }
    
    public String getFieldName() {
        return this.fieldName;
    }
    
    public int getReportedJdbcType() {
        return this.reportedJdbcType;
    }
    
    public String getDataSourceTypeName() {
        return this.dataSourceTypeName;
    }
    
    public int getColumnIndex() {
        return this.columnIndex;
    }
    
    public int getPrecision() {
        return this.precision;
    }
    
    public int getScale() {
        return this.scale;
    }
    
    public <T extends SourceTypeDescriptor> T unwrap(final Class<T> iface) {
        if (iface.isAssignableFrom(this.getClass())) {
            return (T)this;
        }
        return null;
    }
    
    @Override
    public String toString() {
        return String.format("Field Name: %s%n, JDBC type: %s%n, Data source type: %s", this.fieldName, TypeMapper.nameFromType(this.reportedJdbcType), this.dataSourceTypeName);
    }
    
    public static CompleteType getType(final RelDataType relDataType) {
        return CalciteArrowHelper.fromRelAndMinorType(relDataType, TypeInferenceUtils.getMinorTypeFromCalciteType(relDataType));
    }
}
