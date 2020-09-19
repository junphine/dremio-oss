package com.dremio.plugins.mongo.planning.rules;

import java.time.format.*;
import org.bson.*;
import java.util.*;
import org.apache.calcite.rex.*;
import com.dremio.common.expression.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.sql.type.*;
import java.sql.*;
import java.time.temporal.*;
import java.time.*;

public final class RexToFilterDocumentUtils
{
    private static final DateTimeFormatter ISO_DATE_FORMATTER;
    
    static Document constructOperatorDocument(final String opName, final Object... args) {
        return new Document(opName, Arrays.asList(args));
    }
    
    static Object getMongoFormattedLiteral(final RexLiteral literal, final CompleteType typeOfFieldBeingCompared) {
        return getLiteralDocument(literal, typeOfFieldBeingCompared).get(MongoFunctions.LITERAL.getMongoOperator());
    }
    
    static Document getLiteralDocument(final RexLiteral literal, final CompleteType typeOfFieldBeingCompared) {
        if (null != typeOfFieldBeingCompared && typeOfFieldBeingCompared.isComplex()) {
            throw new IllegalArgumentException("Cannot push down values of unknown or complex type.");
        }
        final SqlTypeName literalSqlType = literal.getType().getSqlTypeName();
        String val;
        if (literalSqlType.equals(SqlTypeName.DATE) || literalSqlType.equals(SqlTypeName.TIMESTAMP) || literalSqlType.equals(SqlTypeName.TIME)) {
            val = formatDateTimeLiteralAsISODateString(literal.toString());
        }
        else if (null != typeOfFieldBeingCompared && typeOfFieldBeingCompared.isTemporal() && literal.getType().getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            String litVal = literal.toString();
            if (2 < litVal.length() && '\'' == litVal.charAt(0) && '\'' == litVal.charAt(litVal.length() - 1)) {
                litVal = litVal.substring(1, litVal.length() - 1);
            }
            val = formatDateTimeLiteralAsISODateString(litVal);
        }
        else if (null != typeOfFieldBeingCompared && typeOfFieldBeingCompared.isDecimal() && literal.getType().getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
            val = "NumberDecimal(\"" + literal.toString() + "\")";
        }
        else {
            val = literal.toString();
        }
        return Document.parse(String.format("{ \"%s\": %s }", MongoFunctions.LITERAL.getMongoOperator(), val));
    }
    
    private static String formatDateTimeLiteralAsISODateString(final String dateTimeLiteralText) {
        final LocalDateTime ldt = Timestamp.valueOf(dateTimeLiteralText).toLocalDateTime();
        return String.format("ISODate(\"%s\")", RexToFilterDocumentUtils.ISO_DATE_FORMATTER.format(ldt));
    }
    
    static {
        ISO_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    }
}
