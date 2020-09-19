package com.dremio.exec.store.jdbc.legacy;

import org.apache.calcite.sql.*;
import org.apache.calcite.config.*;

public abstract class LegacyDialect extends JdbcDremioSqlDialect
{
    protected LegacyDialect(final SqlDialect.DatabaseProduct databaseProduct, final String databaseProductName, final String identifierQuoteString, final NullCollation nullCollation) {
        super(databaseProductName, identifierQuoteString, nullCollation);
    }
}
