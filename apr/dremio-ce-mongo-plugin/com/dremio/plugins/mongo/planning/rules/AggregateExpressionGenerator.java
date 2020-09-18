package com.dremio.plugins.mongo.planning.rules;

import org.bson.*;
import com.dremio.exec.record.*;
import com.dremio.common.expression.*;
import com.dremio.plugins.mongo.planning.*;
import java.util.function.*;
import com.google.common.base.*;
import java.util.*;
import org.apache.calcite.util.*;
import org.apache.calcite.sql.fun.*;
import java.math.*;
import org.apache.calcite.rel.type.*;
import com.dremio.plugins.mongo.planning.rels.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;

public class AggregateExpressionGenerator extends RexVisitorImpl<Document>
{
    public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB project expression.";
    private static final Document NULL_LITERAL_EXPR;
    protected final RelDataType recordType;
    protected final BatchSchema schema;
    protected CompleteType leftFieldType;
    protected boolean needsCollation;
    
    public AggregateExpressionGenerator(final BatchSchema schema, final RelDataType recordType) {
        super(true);
        this.needsCollation = false;
        this.schema = schema;
        this.recordType = recordType;
        this.leftFieldType = null;
    }
    
    public boolean needsCollation() {
        return this.needsCollation;
    }
    
    public Document visitInputRef(final RexInputRef inputRef) {
        return this.visitUnknown((RexNode)inputRef);
    }
    
    public Document visitLiteral(final RexLiteral literal) {
        return this.visitUnknown((RexNode)literal);
    }
    
    public Document visitCall(final RexCall call) {
        final String funcName = call.getOperator().getName();
        final MongoFunctions mongoFuncName = MongoFunctions.getMongoOperator(funcName);
        switch (mongoFuncName) {
            case EXTRACT: {
                return this.handleExtractFunction(call);
            }
            case IFNULL: {
                return this.handleIsNullFunction(call, false);
            }
            case IFNOTNULL: {
                return this.handleIsNullFunction(call, true);
            }
            case SUBSTR: {
                return this.handleSubStrFunction(call);
            }
            case TRUNC: {
                return this.handleTruncFunction(call);
            }
            case CASE: {
                return this.handleCaseFunction(call);
            }
            case LOG: {
                if (call.getOperands().size() == 1) {
                    return this.handleGenericFunction(call, "ln");
                }
                return this.handleGenericFunction(call, funcName);
            }
            case ITEM: {
                return this.visitUnknown((RexNode)call);
            }
            default: {
                return this.handleGenericFunction(call, funcName);
            }
        }
    }
    
    protected Document handleGenericFunction(final RexCall call, final String functionName) {
        final String funcName = MongoFunctions.convertToMongoFunction(functionName);
        boolean handleNull = false;
        switch (call.op.kind) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case EQUALS: {
                handleNull = true;
                Object[] args = null;
                if (call.operands.get(0) instanceof RexCall && call.operands.get(1) instanceof RexLiteral) {
                    args = this.getCollationFilterArgs((RexCall)call.operands.get(0), (RexLiteral)call.operands.get(1));
                }
                else if (call.operands.get(1) instanceof RexCall && call.operands.get(0) instanceof RexLiteral) {
                    args = this.getCollationFilterArgs((RexCall)call.operands.get(1), (RexLiteral)call.operands.get(0));
                }
                if (args != null) {
                    this.needsCollation = true;
                    final Document doc = RexToFilterDocumentUtils.constructOperatorDocument(funcName, args);
                    return addNotNullCheck(doc, args[0]);
                }
                break;
            }
        }
        Object[] args = call.getOperands().stream().map(this::formatArgForMongoOperator).toArray();
        final Document baseCall = RexToFilterDocumentUtils.constructOperatorDocument(funcName, args);
        if (handleNull) {
            Preconditions.checkArgument(args.length > 1, (Object)"Null check only needed for operators with at least one argument.");
            return addNotNullCheck(baseCall, args[0]);
        }
        return baseCall;
    }
    
    private Object[] getCollationFilterArgs(final RexCall rexCall, final RexLiteral rexLiteral) {
        if (CollationFilterChecker.hasCollationFilter((RexNode)rexCall, (RexNode)rexLiteral)) {
            return new Object[] { this.formatArgForMongoOperator(rexCall.getOperands().get(0)), rexLiteral.getValue3().toString() };
        }
        return null;
    }
    
    private Object handleCastFunction(final RexCall call) {
        Preconditions.checkArgument(call.getOperands().size() == 1, (Object)"Cast expects a single argument");
        if (MongoRulesUtil.checkForUnneededCast((RexNode)call)) {
            return this.formatArgForMongoOperator(call.getOperands().get(0));
        }
        throw new RuntimeException("Cannot handle anything but unnecessary casts");
    }
    
    private Document handleExtractFunction(final RexCall call) {
        final String unit = call.getOperands().get(0).getValue().toString().toLowerCase();
        final String funcName = MongoFunctions.convertToMongoFunction("extract_" + unit);
        final Object arg = this.formatArgForMongoOperator(call.getOperands().get(1));
        final Document extractFn = RexToFilterDocumentUtils.constructOperatorDocument(funcName, arg);
        final Document isNotNullExpr = RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.NOT_EQUAL.getMongoOperator(), arg, AggregateExpressionGenerator.NULL_LITERAL_EXPR);
        return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.CASE.getMongoOperator(), isNotNullExpr, extractFn, AggregateExpressionGenerator.NULL_LITERAL_EXPR);
    }
    
    private Document handleIsNullFunction(final RexCall call, final boolean reverse) {
        return RexToFilterDocumentUtils.constructOperatorDocument(reverse ? MongoFunctions.GREATER.getMongoOperator() : MongoFunctions.LESS_OR_EQUAL.getMongoOperator(), this.formatArgForMongoOperator(call.getOperands().get(0)), AggregateExpressionGenerator.NULL_LITERAL_EXPR);
    }
    
    private Document handleSubStrFunction(final RexCall call) {
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        final Object stringToSubstr = this.formatArgForMongoOperator(operands.get(0));
        final Object zeroBasedStartIndex = RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.MAX.getMongoOperator(), RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.SUBTRACT.getMongoOperator(), this.formatArgForMongoOperator(operands.get(1)), 1), 0);
        Object numberOfChars;
        if (operands.size() > 2) {
            numberOfChars = this.formatArgForMongoOperator(operands.get(2));
        }
        else {
            numberOfChars = -1;
        }
        return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.SUBSTR.getMongoOperator(), stringToSubstr, zeroBasedStartIndex, numberOfChars);
    }
    
    private Object handleItemFunctionRoot(final RexCall call) {
        final Pair<Object, Boolean> result = this.handleItemFunctionRecursive(call);
        if (result.right) {
            return result.left;
        }
        return "$" + result.left;
    }
    
    private Pair<Object, Boolean> handleItemFunctionRecursive(final RexCall call) {
        final RexNode leftInput = call.getOperands().get(0);
        boolean hitArrayIndex = false;
        Preconditions.checkArgument(leftInput instanceof RexInputRef || leftInput instanceof RexCall);
        Object leftInputObject;
        if (leftInput instanceof RexInputRef) {
            final RexInputRef left = (RexInputRef)leftInput;
            leftInputObject = this.recordType.getFieldNames().get(left.getIndex());
        }
        else {
            final RexCall leftFunc = (RexCall)leftInput;
            Preconditions.checkArgument(leftFunc.getOperator() == SqlStdOperatorTable.ITEM);
            final Pair<Object, Boolean> result = this.handleItemFunctionRecursive(leftFunc);
            leftInputObject = result.left;
            hitArrayIndex = (boolean)result.right;
        }
        final RexNode rightInput = call.getOperands().get(1);
        if (!(rightInput instanceof RexLiteral)) {
            throw new RuntimeException("Error converting item expression. The second argument not a literal or varchar or number");
        }
        final RexLiteral rightLit = (RexLiteral)rightInput;
        switch (rightInput.getType().getSqlTypeName()) {
            case DECIMAL:
            case INTEGER: {
                final Integer val = ((BigDecimal)rightLit.getValue()).intValue();
                if (hitArrayIndex) {
                    assert leftInputObject instanceof Document : "Expected a Document for array selection, but got " + leftInputObject.getClass().getName();
                    return (Pair<Object, Boolean>)new Pair((Object)RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.ARRAYELEMAT.getMongoOperator(), leftInputObject, val), (Object)true);
                }
                else {
                    assert leftInputObject instanceof String : "Expected a String for array selection, but got " + leftInputObject.getClass().getName();
                    return (Pair<Object, Boolean>)new Pair((Object)RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.ARRAYELEMAT.getMongoOperator(), "$" + leftInputObject, val), (Object)true);
                }
                break;
            }
            case CHAR:
            case VARCHAR: {
                if (hitArrayIndex) {
                    throw new RuntimeException("Need to implement more than one array index in a complex column reference");
                }
                assert leftInputObject instanceof String : "Expected a String for array selection with second argument as varchar";
                final String rightLitString = (String)rightLit.getValue2();
                if (rightLitString.startsWith("$")) {
                    throw new RuntimeException("Mongo aggregation pipeline does not support reference to fields that start with '$', field " + rightLitString);
                }
                return (Pair<Object, Boolean>)new Pair((Object)String.format("%s.%s", leftInputObject, rightLitString), (Object)hitArrayIndex);
            }
            default: {
                throw new RuntimeException("error converting item expression, second argument not a literal or varchar or number");
            }
        }
    }
    
    private Document handleTruncFunction(final RexCall call) {
        if (call.getOperands().size() > 1) {
            throw new RuntimeException("Method 'trunc(x, y)' is not supported. Only 'trunc(x)' (which strips the decimal part) is supported");
        }
        return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.TRUNC.getMongoOperator(), this.formatArgForMongoOperator(call.getOperands().get(0)));
    }
    
    private Document handleCaseFunction(final RexCall call) {
        Preconditions.checkArgument(call.getOperands().size() % 2 == 1, (Object)"Number of arguments to a case function should be an odd numbered.");
        return (Document)this.handleCaseFunctionHelper(call.getOperands(), 0);
    }
    
    private Object handleCaseFunctionHelper(final List<RexNode> operands, final int start) {
        if (start == operands.size() - 1) {
            return this.formatArgForMongoOperator(operands.get(start));
        }
        return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.CASE.getMongoOperator(), this.formatArgForMongoOperator(operands.get(start)), this.formatArgForMongoOperator(operands.get(start + 1)), this.handleCaseFunctionHelper(operands, start + 2));
    }
    
    public Document visitDynamicParam(final RexDynamicParam dynamicParam) {
        return this.visitUnknown((RexNode)dynamicParam);
    }
    
    public Document visitRangeRef(final RexRangeRef rangeRef) {
        return this.visitUnknown((RexNode)rangeRef);
    }
    
    public Document visitFieldAccess(final RexFieldAccess fieldAccess) {
        return this.visitUnknown((RexNode)fieldAccess);
    }
    
    public Document visitLocalRef(final RexLocalRef localRef) {
        return this.visitUnknown((RexNode)localRef);
    }
    
    public Document visitOver(final RexOver over) {
        return this.visitUnknown((RexNode)over);
    }
    
    public Document visitCorrelVariable(final RexCorrelVariable correlVariable) {
        return this.visitUnknown((RexNode)correlVariable);
    }
    
    protected Document visitUnknown(final RexNode o) {
        throw new RuntimeException(String.format("Cannot convert RexNode to equivalent MongoDB project expression.RexNode Class: %s, RexNode Digest: %s", o.getClass().getName(), o.toString()));
    }
    
    private Object formatArgForMongoOperator(final RexNode arg) {
        if (arg instanceof RexInputRef) {
            final int index = ((RexInputRef)arg).getIndex();
            final RelDataTypeField field = this.recordType.getFieldList().get(index);
            return "$" + MongoColumnNameSanitizer.sanitizeColumnName(field.getName());
        }
        if (arg instanceof RexCall) {
            final RexCall call = (RexCall)arg;
            final SqlOperator function = call.getOperator();
            if (function == SqlStdOperatorTable.CAST) {
                return this.handleCastFunction(call);
            }
            if (function == SqlStdOperatorTable.ITEM) {
                return this.handleItemFunctionRoot(call);
            }
        }
        else if (arg instanceof RexLiteral) {
            return RexToFilterDocumentUtils.getMongoFormattedLiteral((RexLiteral)arg, null);
        }
        return arg.accept((RexVisitor)this);
    }
    
    private static Document addNotNullCheck(final Document comparisonFilter, final Object firstArg) {
        final Document nullFilter = RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.NOT_EQUAL.getMongoOperator(), firstArg, null);
        return RexToFilterDocumentUtils.constructOperatorDocument(MongoFunctions.convertToMongoFunction(SqlKind.AND.sql), comparisonFilter, nullFilter);
    }
    
    static {
        NULL_LITERAL_EXPR = new Document(MongoFunctions.LITERAL.getMongoOperator(), (Object)null);
    }
}
