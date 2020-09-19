package com.dremio.plugins.mongo.planning.rules;

import org.apache.calcite.sql.fun.*;
import java.util.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.rel.type.*;

public final class MongoRulesUtil
{
    public static boolean isInputRef(final RexNode node) {
        if (node instanceof RexInputRef) {
            return true;
        }
        if (node instanceof RexCall) {
            final RexCall call = (RexCall)node;
            if (call.getOperator() == SqlStdOperatorTable.ITEM) {
                final List<RexNode> operands = (List<RexNode>)call.getOperands();
                if ((isInputRef(operands.get(0)) && isStringLiteral(operands.get(1))) || (isInputRef(operands.get(1)) && isStringLiteral(operands.get(0)))) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public static boolean isLiteral(final RexNode node) {
        return node instanceof RexLiteral;
    }
    
    public static boolean isStringLiteral(final RexNode node) {
        return node instanceof RexLiteral && node.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
    }
    
    public static String getCompoundFieldName(final RexNode node, final List<RelDataTypeField> fields) {
        if (node instanceof RexInputRef) {
            final int index = ((RexInputRef)node).getIndex();
            final RelDataTypeField field = fields.get(index);
            return field.getName();
        }
        if (node instanceof RexCall) {
            final RexCall call = (RexCall)node;
            if (call.getOperator() == SqlStdOperatorTable.ITEM) {
                final List<RexNode> operands = (List<RexNode>)call.getOperands();
                if (isInputRef(operands.get(0)) && isLiteral(operands.get(1))) {
                    return getCompoundFieldName(operands.get(0), fields) + "." + RexLiteral.stringValue((RexNode)operands.get(1));
                }
                if (isInputRef(operands.get(1)) && isLiteral(operands.get(0))) {
                    return getCompoundFieldName(operands.get(1), fields) + "." + RexLiteral.stringValue((RexNode)operands.get(0));
                }
            }
            else if (call.getOperator() == SqlStdOperatorTable.CAST) {
                return getCompoundFieldName(call.getOperands().get(0), fields);
            }
        }
        throw new RuntimeException("Unexpected RexNode received: " + node);
    }
    
    static boolean checkForUnneededCast(final RexNode expr) {
        if (expr instanceof RexCall) {
            final RexCall arg2Call = (RexCall)expr;
            final RelDataType inputType = arg2Call.getOperands().get(0).getType();
            if (arg2Call.getOperator().getName().equalsIgnoreCase("CAST") && (arg2Call.getType().equals(inputType) || inputType.getSqlTypeName().equals(SqlTypeName.ANY) || arg2Call.getType().getSqlTypeName().equals(SqlTypeName.ANY) || arg2Call.getType().getFamily().equals(inputType.getFamily()))) {
                return true;
            }
        }
        return false;
    }
    
    static boolean isSupportedCast(final RexNode node) {
        return checkForUnneededCast(node) || CollationFilterChecker.checkForCollationCast(node);
    }
}
