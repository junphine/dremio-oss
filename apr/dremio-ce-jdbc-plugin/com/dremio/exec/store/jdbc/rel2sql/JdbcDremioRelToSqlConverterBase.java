package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.legacy.*;

public class JdbcDremioRelToSqlConverterBase extends JdbcDremioRelToSqlConverter
{
    public JdbcDremioRelToSqlConverterBase(final JdbcDremioSqlDialect dremioDialect) {
        super(dremioDialect);
    }
    
    @Override
    protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
        return this;
    }
}
