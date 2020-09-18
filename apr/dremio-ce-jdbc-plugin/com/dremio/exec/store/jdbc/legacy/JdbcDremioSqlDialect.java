package com.dremio.exec.store.jdbc.legacy;

import com.dremio.common.dialect.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import org.apache.calcite.config.*;
import javax.sql.*;
import com.dremio.exec.store.jdbc.*;
import com.dremio.exec.store.jdbc.dialect.*;
import com.dremio.common.rel2sql.*;
import org.apache.calcite.sql.*;

public class JdbcDremioSqlDialect extends DremioSqlDialect
{
    public static final JdbcDremioSqlDialect DERBY;
    
    public JdbcDremioRelToSqlConverter getConverter() {
        return new JdbcDremioRelToSqlConverterBase(this);
    }
    
    protected JdbcDremioSqlDialect(final String databaseProductName, final String identifierQuoteString, final NullCollation nullCollation) {
        super(databaseProductName, identifierQuoteString, nullCollation);
    }
    
    public JdbcSchemaFetcher getSchemaFetcher(final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
        return new JdbcSchemaFetcher(name, dataSource, timeout, config);
    }
    
    public TypeMapper getDataTypeMapper() {
        return AutomaticTypeMapper.INSTANCE;
    }
    
    static {
        DERBY = new JdbcDremioSqlDialect(SqlDialect.DatabaseProduct.DERBY.name(), "\"", NullCollation.HIGH);
    }
}
