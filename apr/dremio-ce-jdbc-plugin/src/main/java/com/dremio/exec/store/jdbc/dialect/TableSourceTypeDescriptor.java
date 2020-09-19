package com.dremio.exec.store.jdbc.dialect;

public class TableSourceTypeDescriptor extends SourceTypeDescriptor
{
    private final String catalog;
    private final String schema;
    private final String table;
    
    TableSourceTypeDescriptor(final String fieldName, final int reportedJdbcType, final String dataSourceName, final String catalog, final String schema, final String table, final int colIndex, final int precision, final int scale) {
        super(fieldName, reportedJdbcType, dataSourceName, colIndex, precision, scale);
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
    }
    
    public String getCatalog() {
        return this.catalog;
    }
    
    public String getSchema() {
        return this.schema;
    }
    
    public String getTable() {
        return this.table;
    }
    
    @Override
    public String toString() {
        return String.format("Table identifier: %s.%s.%s%n", this.catalog, this.schema, this.table) + super.toString();
    }
}
