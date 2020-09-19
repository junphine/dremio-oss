package com.dremio.plugins.mongo.planning.rules;

import org.bson.*;
import org.apache.calcite.rel.type.*;
import com.dremio.plugins.mongo.planning.rels.*;
import org.apache.calcite.sql.type.*;
import java.text.*;
import java.sql.*;
import org.apache.calcite.sql.*;
import com.google.common.base.*;
import org.apache.calcite.util.*;
import org.apache.calcite.sql.fun.*;
import java.math.*;
import com.dremio.common.exceptions.*;
import com.dremio.plugins.mongo.planning.*;
import java.util.*;
import org.apache.calcite.rex.*;
import com.dremio.common.expression.*;
import com.dremio.exec.record.*;
import org.slf4j.*;

@Deprecated
public class ProjectExpressionConverter extends RexVisitorImpl<Object>
{
    private static final Logger logger;
    public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB project expression.";
    private static final Document NULL_LITERAL_EXPR;
    protected final RelDataType recordType;
    protected final boolean enableMongo3_2;
    protected final boolean enableMongo3_4;
    protected final BatchSchema schema;
    protected CompleteType leftFieldType;
    protected boolean needsCollation;
    
    public ProjectExpressionConverter(final RelDataType recordType, final boolean enableMongo3_2, final boolean enableMongo3_4) {
        this(null, recordType, enableMongo3_2, enableMongo3_4);
    }
    
    public ProjectExpressionConverter(final BatchSchema schema, final RelDataType recordType, final boolean enableMongo3_2, final boolean enableMongo3_4) {
        super(true);
        this.needsCollation = false;
        this.schema = schema;
        this.recordType = recordType;
        this.enableMongo3_2 = enableMongo3_2;
        this.enableMongo3_4 = enableMongo3_4;
        this.leftFieldType = null;
    }
    
    public boolean needsCollation() {
        return this.needsCollation && this.enableMongo3_4;
    }
    
    public Object visitInputRef(final RexInputRef inputRef) {
        final int index = inputRef.getIndex();
        final RelDataTypeField field = this.recordType.getFieldList().get(index);
        return "$" + MongoColumnNameSanitizer.sanitizeColumnName(field.getName());
    }
    
    protected Document getLiteralDocument(final RexLiteral literal) {
        if (null != this.leftFieldType && this.leftFieldType.isComplex()) {
            throw new IllegalArgumentException("Cannot push down values of unknown or complex type.");
        }
        String val;
        if (literal.getType().getSqlTypeName().equals(SqlTypeName.DATE) || literal.getType().getSqlTypeName().equals(SqlTypeName.TIMESTAMP) || literal.getType().getSqlTypeName().equals(SqlTypeName.TIME)) {
            val = "ISODate(\"" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(Timestamp.valueOf(literal.toString())) + "\")";
        }
        else if (null != this.leftFieldType && this.leftFieldType.isTemporal() && literal.getType().getSqlTypeName().equals(SqlTypeName.VARCHAR)) {
            String litVal = literal.toString();
            if (2 < litVal.length() && '\'' == litVal.charAt(0) && '\'' == litVal.charAt(litVal.length() - 1)) {
                litVal = litVal.substring(1, litVal.length() - 1);
            }
            val = "ISODate(\"" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(Timestamp.valueOf(litVal)) + "\")";
        }
        else if (null != this.leftFieldType && this.leftFieldType.isDecimal() && literal.getType().getSqlTypeName().equals(SqlTypeName.DECIMAL)) {
            val = "NumberDecimal(\"" + literal.toString() + "\")";
        }
        else {
            val = literal.toString();
        }
        final String toParse = "{ " + MongoFunctions.LITERAL.getMongoOperator() + " : " + val + " }";
        return Document.parse(toParse);
    }
    
    public Object visitLiteral(final RexLiteral literal) {
        return new Document(MongoFunctions.LITERAL.getMongoOperator(), this.getLiteralDocument(literal).get(MongoFunctions.LITERAL.getMongoOperator()));
    }
    
    public Object visitCall(final RexCall call) {
        final String funcName = call.getOperator().getName().toLowerCase();
        final MongoFunctions functions = MongoFunctions.getMongoOperator(funcName);
        switch (functions) {
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
            case DIVIDE: {
                return this.handleDivideFunction(call);
            }
            case TRUNC: {
                return this.handleTruncFunction(call);
            }
            case CASE: {
                return this.handleCaseFunction(call);
            }
            case ITEM: {
                return this.handleItemFunctionRoot(call);
            }
            case CAST: {
                return this.handleCastFunction(call);
            }
            default: {
                return this.handleGenericFunction(call, funcName);
            }
        }
    }
    
    protected Object handleGenericFunction(final RexCall call, final String functionName) {
        final String funcName = this.convertToMongoFunction(functionName);
        boolean handleNull = false;
        if (this.enableMongo3_4) {
            switch (call.op.kind) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case NOT_EQUALS: {
                    handleNull = true;
                }
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case EQUALS: {
                    Object[] args = null;
                    if (call.operands.get(0) instanceof RexCall && call.operands.get(1) instanceof RexLiteral) {
                        args = this.getCollationFilterArgs((RexCall)call.operands.get(0), (RexLiteral)call.operands.get(1));
                    }
                    else if (call.operands.get(1) instanceof RexCall && call.operands.get(0) instanceof RexLiteral) {
                        args = this.getCollationFilterArgs((RexCall)call.operands.get(1), (RexLiteral)call.operands.get(0));
                    }
                    if (args != null) {
                        this.needsCollation = true;
                        Document doc = this.constructJson(funcName, args);
                        if (handleNull) {
                            final Document nullFilter = this.constructJson(this.convertToMongoFunction(SqlKind.GREATER_THAN.sql), args[0], null);
                            doc = this.constructJson(this.convertToMongoFunction(SqlKind.AND.sql), doc, nullFilter);
                        }
                        return doc;
                    }
                    break;
                }
            }
        }
        Object[] args = new Object[call.getOperands().size()];
        for (int i = 0; i < call.getOperands().size(); ++i) {
            args[i] = call.getOperands().get(i).accept((RexVisitor)this);
        }
        return this.constructJson(funcName, args);
    }
    
    private Object[] getCollationFilterArgs(final RexCall rexCall, final RexLiteral rexLiteral) {
        if (CollationFilterChecker.hasCollationFilter((RexNode)rexCall, (RexNode)rexLiteral)) {
            final Object[] args = { rexCall.getOperands().get(0).accept((RexVisitor)this), rexLiteral.getValue3().toString() };
            return args;
        }
        return null;
    }
    
    private Object handleCastFunction(final RexCall call) {
        Preconditions.checkArgument(call.getOperands().size() == 1, "Cast expects a single argument");
        if (MatchExpressionConverter.checkForUnneededCast((RexNode)call)) {
            return call.getOperands().get(0).accept((RexVisitor)this);
        }
        throw new RuntimeException("Cannot handle anything but unnecessary casts");
    }
    
    private Object handleExtractFunction(final RexCall call) {
        final String unit = call.getOperands().get(0).toString().toLowerCase();
        final String funcName = this.convertToMongoFunction("extract_" + unit);
        final Object arg = call.getOperands().get(1).accept((RexVisitor)this);
        final Object extractFn = this.constructJson(funcName, arg);
        final Object isNotNullExpr = this.constructJson(MongoFunctions.NOT_EQUAL.getMongoOperator(), arg, ProjectExpressionConverter.NULL_LITERAL_EXPR);
        final Object condExpr = this.constructJson(MongoFunctions.CASE.getMongoOperator(), isNotNullExpr, extractFn, ProjectExpressionConverter.NULL_LITERAL_EXPR);
        return condExpr;
    }
    
    private Object handleIsNullFunction(final RexCall call, final boolean reverse) {
        return this.constructJson(reverse ? MongoFunctions.GREATER.getMongoOperator() : MongoFunctions.LESS_OR_EQUAL.getMongoOperator(), call.getOperands().get(0).accept((RexVisitor)this), ProjectExpressionConverter.NULL_LITERAL_EXPR);
    }
    
    private Object handleDivideFunction(final RexCall call) {
        return this.constructJson(MongoFunctions.DIVIDE.getMongoOperator(), call.getOperands().get(0).accept((RexVisitor)this), call.getOperands().get(1).accept((RexVisitor)this));
    }
    
    private Object handleSubStrFunction(final RexCall call) {
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        final Object arg0 = operands.get(0).accept((RexVisitor)this);
        final Object arg2 = operands.get(1).accept((RexVisitor)this);
        Object arg3;
        if (operands.size() > 2) {
            arg3 = operands.get(2).accept((RexVisitor)this);
        }
        else {
            arg3 = -1;
        }
        return this.constructJson(MongoFunctions.SUBSTR.getMongoOperator(), arg0, arg2, arg3);
    }
    
    private Object handleItemFunctionRoot(final RexCall call) {
        final Pair<Object, Boolean> result = this.handleItemFunction(call, false);
        if (result.right) {
            return result.left;
        }
        return "$" + result.left;
    }
    
    private Pair<Object, Boolean> handleItemFunction(final RexCall call, boolean hitArrayIndex) {
        final RexNode leftInput = call.getOperands().get(0);
        Object leftInputObject;
        if (leftInput instanceof RexInputRef) {
            final RexInputRef left = (RexInputRef)leftInput;
            leftInputObject = this.recordType.getFieldNames().get(left.getIndex());
        }
        else {
            if (!(leftInput instanceof RexCall)) {
                throw new RuntimeException("left input to ITEM was not another ITEM or RexInputRef");
            }
            final RexCall leftFunc = (RexCall)leftInput;
            if (leftFunc.getOperator() != SqlStdOperatorTable.ITEM) {
                throw new RuntimeException("Error");
            }
            final Pair<Object, Boolean> result = this.handleItemFunction(leftFunc, false);
            leftInputObject = result.left;
            hitArrayIndex = (boolean)result.right;
        }
        final RexNode rightInput = call.getOperands().get(1);
        if (!(rightInput instanceof RexLiteral)) {
            throw new RuntimeException("error converting item expression, second argument not a literal or varchar or number");
        }
        final RexLiteral rightLit = (RexLiteral)rightInput;
        switch (rightInput.getType().getSqlTypeName()) {
            case DECIMAL:
            case INTEGER: {
                if (!this.enableMongo3_2) {
                    throw new RuntimeException("Mongo version less than 3.2 does not support " + MongoFunctions.ARRAYELEMAT.getMongoOperator());
                }
                final Integer val = ((BigDecimal)rightLit.getValue()).intValue();
                if (hitArrayIndex) {
                    assert leftInputObject instanceof Document : "Expected a Document for array selection, but got " + leftInputObject.getClass().getName();
                    return (Pair<Object, Boolean>)new Pair(this.constructJson(MongoFunctions.ARRAYELEMAT.getMongoOperator(), leftInputObject, val), true);
                }
                else {
                    assert leftInputObject instanceof String : "Expected a String for array selection, but got " + leftInputObject.getClass().getName();
                    return (Pair<Object, Boolean>)new Pair(this.constructJson(MongoFunctions.ARRAYELEMAT.getMongoOperator(), "$" + leftInputObject, val), true);
                }
               // break;
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
                final String rightInputConvStr = "." + rightLitString;
                return (Pair<Object, Boolean>)new Pair((leftInputObject + rightInputConvStr), hitArrayIndex);
            }
            default: {
                throw new RuntimeException("error converting item expression, second argument not a literal or varchar or number");
            }
        }
    }
    
    private Object handleTruncFunction(final RexCall call) {
        if (!this.enableMongo3_2) {
            throw UserException.planError().message("Mongo version less than 3.2 does not support " + MongoFunctions.TRUNC.getMongoOperator(), new Object[0]).build(ProjectExpressionConverter.logger);
        }
        if (call.getOperands().size() > 1) {
            throw UserException.planError().message("Method 'trunc(x, y)' is not supported. Only 'trunc(x)' (which strips the decimal part) is supported", new Object[0]).build(ProjectExpressionConverter.logger);
        }
        return this.constructJson(MongoFunctions.TRUNC.getMongoOperator(), call.getOperands().get(0).accept((RexVisitor)this));
    }
    
    private Object handleCaseFunction(final RexCall call) {
        Preconditions.checkArgument(call.getOperands().size() % 2 == 1, "Number of arguments to a case function should be an odd numbered.");
        return this.handleCaseFunctionHelper(call.getOperands(), 0);
    }
    
    private Object handleCaseFunctionHelper(final List<RexNode> operands, final int start) {
        if (start == operands.size() - 1) {
            return operands.get(start).accept((RexVisitor)this);
        }
        return this.constructJson(MongoFunctions.CASE.getMongoOperator(), operands.get(start).accept((RexVisitor)this), operands.get(start + 1).accept((RexVisitor)this), this.handleCaseFunctionHelper(operands, start + 2));
    }
    
    private String convertToMongoFunction(final String sqlFunction) {
        final MongoFunctions mongoOp = MongoFunctions.getMongoOperator(sqlFunction);
        if (mongoOp != null && mongoOp.canUseInStage(MongoPipelineOperators.PROJECT)) {
            if (this.enableMongo3_2) {
                return mongoOp.getMongoOperator();
            }
            if (mongoOp.supportedInVersion("3.1")) {
                return mongoOp.getMongoOperator();
            }
        }
        throw UserException.planError().message("Unsupported function %s, Mongo 3.2 features enabled: %s", new Object[] { sqlFunction, this.enableMongo3_2 }).build(ProjectExpressionConverter.logger);
    }
    
    private Document constructJson(final String opName, final Object... args) {
        return new Document(opName, Arrays.asList(args));
    }
    
    public Object visitDynamicParam(final RexDynamicParam dynamicParam) {
        return this.visitUnknown((RexNode)dynamicParam);
    }
    
    public Object visitRangeRef(final RexRangeRef rangeRef) {
        return this.visitUnknown((RexNode)rangeRef);
    }
    
    public Object visitFieldAccess(final RexFieldAccess fieldAccess) {
        return this.visitUnknown((RexNode)fieldAccess);
    }
    
    public Object visitLocalRef(final RexLocalRef localRef) {
        return this.visitUnknown((RexNode)localRef);
    }
    
    public Object visitOver(final RexOver over) {
        return this.visitUnknown((RexNode)over);
    }
    
    public Object visitCorrelVariable(final RexCorrelVariable correlVariable) {
        return this.visitUnknown((RexNode)correlVariable);
    }
    
    protected Object visitUnknown(final RexNode o) {
        throw UserException.planError().message("Cannot convert RexNode to equivalent MongoDB project expression.RexNode Class: %s, RexNode Digest: %s", new Object[] { o.getClass().getName(), o.toString() }).build(ProjectExpressionConverter.logger);
    }
    
    protected CompleteType getFieldType(final RexNode curNode) {
        if (this.schema == null) {
            return null;
        }
        final String compoundName = MongoRulesUtil.getCompoundFieldName(curNode, this.recordType.getFieldList());
        final SchemaPath compoundPath = SchemaPath.getCompoundPath(compoundName.split("\\."));
        if (null != compoundPath) {
            final TypedFieldId fieldId = this.schema.getFieldId((BasePath)compoundPath);
            if (null != fieldId) {
                return fieldId.getFinalType();
            }
        }
        return null;
    }
    
    static {
        logger = LoggerFactory.getLogger(ProjectExpressionConverter.class);
        NULL_LITERAL_EXPR = new Document(MongoFunctions.LITERAL.getMongoOperator(), null);
    }
}
