package com.dremio.plugins.mongo.planning.rels;

import org.apache.calcite.rel.type.*;
import java.util.*;

public final class MongoColumnNameSanitizer
{
    private static final String DOLLAR_FIELDNAME_PREFIX = "MongoRename_";
    private static final int DOLLAR_FIELDNAME_PREFIX_LENGTH = 12;
    
    public static RelDataType sanitizeColumnNames(final RelDataType inputRowType) {
        boolean needRenaming = false;
        final List<String> currentFieldNames = (List<String>)inputRowType.getFieldNames();
        final List<String> newFieldNames = new ArrayList<String>(currentFieldNames.size());
        for (final String currentName : currentFieldNames) {
            if (currentName.charAt(0) == '$') {
                newFieldNames.add("MongoRename_" + currentName);
                needRenaming = true;
            }
            else {
                newFieldNames.add(currentName);
            }
        }
        if (!needRenaming) {
            return inputRowType;
        }
        final List<RelDataTypeField> currentFields = (List<RelDataTypeField>)inputRowType.getFieldList();
        final List<RelDataTypeField> newFields = new ArrayList<RelDataTypeField>(newFieldNames.size());
        for (int i = 0; i != newFieldNames.size(); ++i) {
            newFields.add((RelDataTypeField)new RelDataTypeFieldImpl((String)newFieldNames.get(i), i, currentFields.get(i).getType()));
        }
        return (RelDataType)new RelRecordType((List)newFields);
    }
    
    public static String sanitizeColumnName(final String columnName) {
        if (columnName.charAt(0) == '$') {
            return "MongoRename_" + columnName;
        }
        return columnName;
    }
    
    public static String unsanitizeColumnName(final String columnName) {
        if (columnName.startsWith("MongoRename_")) {
            return columnName.substring(12);
        }
        return columnName;
    }
}
