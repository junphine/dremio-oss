package com.dremio.exec.store.jdbc.dialect.arp;

import org.apache.calcite.sql.parser.*;
import java.util.*;
import org.apache.calcite.sql.*;

class SourceTypeSpec extends SqlDataTypeSpec
{
    private final String sourceTypeName;
    private final Mapping.RequiredCastArgs castArgs;
    
    SourceTypeSpec(final String sourceTypeName, final Mapping.RequiredCastArgs castArgs, final int precision, final int scale) {
        super(new SqlIdentifier(sourceTypeName, SqlParserPos.ZERO), precision, scale, (String)null, (TimeZone)null, SqlParserPos.ZERO);
        this.sourceTypeName = sourceTypeName;
        this.castArgs = castArgs;
    }
    
    public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
        final boolean hasPrecision = this.getPrecision() != -1;
        final boolean hasScale = this.getScale() != Integer.MIN_VALUE;
        final Mapping.RequiredCastArgs args = Mapping.RequiredCastArgs.getRequiredArgsBasedOnInputs(hasPrecision, hasScale, this.castArgs);
        writer.print(args.serializeArguments(this.sourceTypeName, this.getPrecision(), this.getScale()));
    }
}
