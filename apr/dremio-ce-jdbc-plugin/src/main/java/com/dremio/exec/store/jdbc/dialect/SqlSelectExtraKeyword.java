package com.dremio.exec.store.jdbc.dialect;

import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.*;

public enum SqlSelectExtraKeyword
{
    TOP;
    
    public SqlLiteral symbol(final SqlParserPos pos) {
        return SqlLiteral.createSymbol((Enum)this, pos);
    }
}
