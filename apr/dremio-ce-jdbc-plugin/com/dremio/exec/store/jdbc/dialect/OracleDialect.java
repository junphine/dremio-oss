package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.dialect.arp.*;
import org.apache.calcite.rel.type.*;
import com.dremio.common.dialect.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.parser.*;
import java.util.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.*;
import com.dremio.exec.store.jdbc.legacy.*;
import javax.sql.*;
import com.google.common.base.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import java.sql.*;

public final class OracleDialect extends ArpDialect
{
    private static final int ORACLE_MAX_VARCHAR_LENGTH = 4000;
    private static final Integer ORACLE_MAX_IDENTIFIER_LENGTH;
    private final ArpTypeMapper typeMapper;
    
    public OracleDialect(final ArpYaml yaml) {
        super(yaml);
        this.typeMapper = new OracleTypeMapper(yaml);
    }
    
    @Override
    public SqlNode emulateNullDirection(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        return null;
    }
    
    @Override
    public SqlNode getCastSpec(final RelDataType type) {
        switch (type.getSqlTypeName()) {
            case VARCHAR: {
                if (type.getPrecision() > 4000 || type.getPrecision() == -1) {
                    return getVarcharWithPrecision((DremioSqlDialect)this, type, 4000);
                }
                return getVarcharWithPrecision((DremioSqlDialect)this, type, type.getPrecision());
            }
            case DOUBLE: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier(SqlTypeName.FLOAT.name(), SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
            case INTEGER:
            case BIGINT: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier(SqlTypeName.DECIMAL.name(), SqlParserPos.ZERO), 38, 0, (String)null, (TimeZone)null, SqlParserPos.ZERO);
            }
            case TIME:
            case DATE: {
                return (SqlNode)new SqlDataTypeSpec(new SqlIdentifier(OracleKeyWords.TIMESTAMP.toString(), SqlParserPos.ZERO), -1, -1, null, null, SqlParserPos.ZERO) {
                    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
                        writer.keyword(OracleKeyWords.TIMESTAMP.toString());
                    }
                };
            }
            default: {
                return super.getCastSpec(type);
            }
        }
    }
    
    @Override
    public TypeMapper getDataTypeMapper() {
        return this.typeMapper;
    }
    
    public boolean supportsAliasedValues() {
        return false;
    }
    
    @Override
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
            OracleSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    protected boolean allowsAs() {
        return false;
    }
    
    @Override
    public OracleRelToSqlConverter getConverter() {
        return new OracleRelToSqlConverter(this);
    }
    
    @Override
    public ArpSchemaFetcher getSchemaFetcher(final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
        final StringBuilder queryBuilder = new StringBuilder("SELECT * FROM (SELECT NULL CAT, OWNER SCH, TABLE_NAME NME FROM SYS.ALL_ALL_TABLES UNION ALL SELECT NULL CAT, OWNER SCH, VIEW_NAME NME FROM SYS.ALL_VIEWS");
        if (!config.getHiddenTableTypes().contains("SYNONYM")) {
            queryBuilder.append(" UNION ALL SELECT NULL CAT, OWNER SCH, SYNONYM_NAME NME FROM SYS.ALL_SYNONYMS");
        }
        queryBuilder.append(") WHERE SCH NOT IN ('%s')");
        final String query = String.format(queryBuilder.toString(), Joiner.on("','").join((Iterable)config.getHiddenSchemas()));
        return new ArpSchemaFetcher(query, name, dataSource, timeout, config, true, false);
    }
    
    @Override
    public boolean supportsLiteral(final CompleteType type) {
        return !CompleteType.BIT.equals((Object)type) && (CompleteType.INT.equals((Object)type) || CompleteType.BIGINT.equals((Object)type) || CompleteType.DATE.equals((Object)type) || super.supportsLiteral(type));
    }
    
    public boolean supportsBooleanAggregation() {
        return false;
    }
    
    @Override
    public boolean supportsSort(final boolean isCollationEmpty, final boolean isOffsetEmpty) {
        return isOffsetEmpty;
    }
    
    public Integer getIdentifierLengthLimit() {
        return OracleDialect.ORACLE_MAX_IDENTIFIER_LENGTH;
    }
    
    static {
        ORACLE_MAX_IDENTIFIER_LENGTH = 30;
    }
    
    public enum OracleKeyWords
    {
        ROWNUM, 
        TIMESTAMP;
    }
    
    private static class OracleTypeMapper extends ArpTypeMapper
    {
        public OracleTypeMapper(final ArpYaml yaml) {
            super(yaml);
        }
        
        @Override
        protected boolean shouldIgnore(final SourceTypeDescriptor column) {
            return column.getDataSourceTypeName().equals("SYS.XMLTYPE");
        }
        
        @Override
        protected SourceTypeDescriptor createTypeDescriptor(final AddPropertyCallback addColumnPropertyCallback, final InvalidMetaDataCallback invalidMetaDataCallback, final Connection connection, final TableIdentifier table, final ResultSetMetaData metaData, final String columnLabel, final int colIndex) throws SQLException {
            int precision = metaData.getPrecision(colIndex);
            int scale = metaData.getScale(colIndex);
            final String columnClassType = metaData.getColumnClassName(colIndex);
            int colType;
            String colTypeName;
            if ("java.lang.Double".equals(columnClassType)) {
                colType = 8;
                colTypeName = SqlTypeName.FLOAT.getName();
            }
            else {
                colType = metaData.getColumnType(colIndex);
                colTypeName = metaData.getColumnTypeName(colIndex);
                if (colType == 2 && precision == 0) {
                    precision = 38;
                    scale = 6;
                    if (null != addColumnPropertyCallback) {
                        addColumnPropertyCallback.addProperty(columnLabel, "explicitCast", Boolean.TRUE.toString());
                    }
                }
            }
            return new SourceTypeDescriptor(columnLabel, colType, colTypeName, colIndex, precision, scale);
        }
    }
}
