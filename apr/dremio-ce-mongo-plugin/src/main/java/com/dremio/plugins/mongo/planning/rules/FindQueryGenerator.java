package com.dremio.plugins.mongo.planning.rules;

import org.bson.*;
import com.google.common.base.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.rel.type.*;
import com.dremio.exec.expr.fn.impl.*;
import com.dremio.common.expression.*;
import com.dremio.exec.record.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;
import java.util.*;
import org.slf4j.*;

public class FindQueryGenerator extends RexVisitorImpl<Document>
{
    private static final Logger logger;
    public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB match expression.";
    protected final RelDataType recordType;
    protected final BatchSchema schema;
    protected CompleteType leftFieldType;
    protected boolean needsCollation;
    
    public FindQueryGenerator(final BatchSchema schema, final RelDataType recordType) {
        super(true);
        this.needsCollation = false;
        this.schema = schema;
        this.recordType = recordType;
        this.leftFieldType = null;
    }
    
    public Document visitInputRef(final RexInputRef inputRef) {
        return this.visitUnknown((RexNode)inputRef);
    }
    
    public Document visitLiteral(final RexLiteral literal) {
        return this.visitUnknown((RexNode)literal);
    }
    
    public Document visitCall(final RexCall call) {
        final String funcName = call.getOperator().getName();
        final MongoFunctions operator = MongoFunctions.getMongoOperator(funcName);
        switch (operator) {
            case NOT:
            case AND:
            case OR: {
                return this.handleGenericFunction(call, funcName);
            }
            case EQUAL:
            case NOT_EQUAL:
            case GREATER:
            case GREATER_OR_EQUAL:
            case LESS:
            case LESS_OR_EQUAL: {
                return this.handleFindQueryComparison(call, funcName);
            }
            case IFNULL: {
                return this.handleIsNullFunction(call, true);
            }
            case IFNOTNULL: {
                return this.handleIsNullFunction(call, false);
            }
            case REGEX: {
                return this.handleLikeFunction(call);
            }
            default: {
                return this.getNodeAsAggregateExprDoc((RexNode)call);
            }
        }
    }
    
    private Document handleFindQueryComparison(final RexCall call, final String funcName) {
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        Preconditions.checkArgument(operands.size() == 2);
        final RexNode firstOp = operands.get(0);
        final RexNode secondOp = operands.get(1);
        if (secondOp instanceof RexLiteral && (MongoFunctions.getMongoOperator(funcName) == MongoFunctions.EQUAL || MongoFunctions.getMongoOperator(funcName) == MongoFunctions.NOT_EQUAL) && ((RexLiteral)secondOp).getValue() == null) {
            throw new RuntimeException("Equality comparison operators are invalid when used with NULL, use operator IS NULL or IS NOT NULL instead.");
        }
        if (!MongoRulesUtil.isInputRef(firstOp) && !MongoRulesUtil.isSupportedCast(firstOp) && !MongoRulesUtil.isInputRef(secondOp) && !MongoRulesUtil.isSupportedCast(secondOp)) {
            return this.getNodeAsAggregateExprDoc((RexNode)call);
        }
        final boolean firstArgInputRef = MongoRulesUtil.isInputRef(firstOp) || MongoRulesUtil.isSupportedCast(firstOp);
        RexNode colRef;
        RexNode otherArgValue;
        Object otherArg;
        try {
            if (!firstArgInputRef) {
                colRef = secondOp;
                otherArgValue = firstOp;
            }
            else {
                colRef = firstOp;
                otherArgValue = secondOp;
            }
            this.leftFieldType = this.getFieldType(colRef);
            if (!(otherArgValue instanceof RexLiteral)) {
                return this.getNodeAsAggregateExprDoc((RexNode)call);
            }
            otherArg = RexToFilterDocumentUtils.getMongoFormattedLiteral((RexLiteral)otherArgValue, this.leftFieldType);
        }
        finally {
            this.leftFieldType = null;
        }
        final MongoFunctions mongoOperator = MongoFunctions.getMongoOperator(funcName);
        if (mongoOperator == null) {
            throw new RuntimeException("Encountered a function that cannot be converted to MongoOperator, " + funcName);
        }
        Preconditions.checkState(mongoOperator.canUseInStage(MongoPipelineOperators.MATCH));
        String mongoFunction;
        if (!firstArgInputRef) {
            if (!mongoOperator.canFlip()) {
                throw new RuntimeException("Encountered a function that cannot swap the left and right operands, " + funcName);
            }
            mongoFunction = mongoOperator.getFlipped();
        }
        else {
            mongoFunction = mongoOperator.getMongoOperator();
        }
        if (CollationFilterChecker.hasCollationFilter(colRef, otherArgValue) || isCollationEquality(call, colRef, otherArgValue)) {
            otherArg = otherArg.toString();
            this.needsCollation = true;
        }
        final String compoundFieldName = MongoRulesUtil.getCompoundFieldName(colRef, this.recordType.getFieldList());
        final Document filter = new Document(compoundFieldName, new Document(mongoFunction, otherArg));
        return addNotNullCheck(filter, compoundFieldName);
    }
    
    private Document handleLikeFunction(final RexCall call) {
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        assert operands.size() == 2;
        if (operands.get(0) instanceof RexInputRef) {
            final RexLiteral arg2 = (RexLiteral)operands.get(1);
            final RexInputRef arg3 = (RexInputRef)operands.get(0);
            String sqlRegex = arg2.toString();
            sqlRegex = sqlRegex.substring(1, sqlRegex.length() - 1);
            final String regex = RegexpUtil.sqlToRegexLike(sqlRegex);
            final int index = arg3.getIndex();
            final RelDataTypeField field = this.recordType.getFieldList().get(index);
            return new Document(field.getName(), new Document(MongoFunctions.REGEX.getMongoOperator(), regex.toString()));
        }
        return this.visitUnknown((RexNode)call);
    }
    
    protected Document visitUnknown(final RexNode o) {
        throw new RuntimeException(String.format("Cannot convert RexNode to equivalent MongoDB match expression. RexNode Class: %s, RexNode Digest: %s", o.getClass().getName(), o.toString()));
    }
    
    protected CompleteType getFieldType(final RexNode curNode) {
        if (this.schema == null) {
            return null;
        }
        final String compoundName = MongoRulesUtil.getCompoundFieldName(curNode, this.recordType.getFieldList());
        final SchemaPath compoundPath = SchemaPath.getCompoundPath(compoundName.split("\\."));
        final TypedFieldId fieldId = this.schema.getFieldId((BasePath)compoundPath);
        if (null != fieldId) {
            return fieldId.getFinalType();
        }
        return null;
    }
    
    protected Document handleGenericFunction(final RexCall call, final String functionName) {
        final String funcName = MongoFunctions.convertToMongoFunction(functionName);
        final Object[] args = new Object[call.getOperands().size()];
        for (int i = 0; i < call.getOperands().size(); ++i) {
            args[i] = this.formatArgForMongoOperator(call.getOperands().get(i));
        }
        return RexToFilterDocumentUtils.constructOperatorDocument(funcName, args);
    }
    
    private Object formatArgForMongoOperator(final RexNode arg) {
        if (arg instanceof RexInputRef) {
            final int columnRefIndex = ((RexInputRef)arg).getIndex();
            final RelDataTypeField field = this.recordType.getFieldList().get(columnRefIndex);
            Preconditions.checkArgument(field.getType().getSqlTypeName() == SqlTypeName.BOOLEAN);
            return new Document(field.getName(), true);
        }
        try {
            return arg.accept((RexVisitor)this);
        }
        catch (RuntimeException ex) {
            return this.getNodeAsAggregateExprDoc(arg);
        }
    }
    
    private Document getNodeAsAggregateExprDoc(final RexNode node) {
        final AggregateExpressionGenerator aggExprGenerator = new AggregateExpressionGenerator(this.schema, this.recordType);
        final Document result = new Document("$expr", node.accept((RexVisitor)aggExprGenerator));
        if (aggExprGenerator.needsCollation()) {
            this.needsCollation = true;
        }
        return result;
    }
    
    private static boolean isCollationEquality(final RexCall call, final RexNode colRef, final RexNode otherValue) {
        return call.isA(SqlKind.EQUALS) && otherValue instanceof RexLiteral && colRef.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER && otherValue.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
    }
    
    private static Document addNotNullCheck(final Document filter, final String mongoFormattedColReference) {
        final Document notNullFilter = new Document(mongoFormattedColReference, new Document(MongoFunctions.NOT_EQUAL.getMongoOperator(), null));
        return new Document(MongoFunctions.AND.getMongoOperator(), Arrays.asList(notNullFilter, filter));
    }
    
    private Document handleIsNullFunction(final RexCall call, final boolean isNull) {
        final RexNode firstOp = call.getOperands().get(0);
        if (firstOp instanceof RexInputRef) {
            final String mongoFunction = isNull ? MongoFunctions.EQUAL.getMongoOperator() : MongoFunctions.NOT_EQUAL.getMongoOperator();
            final String compoundFieldName = MongoRulesUtil.getCompoundFieldName(firstOp, this.recordType.getFieldList());
            return new Document(compoundFieldName, new Document(mongoFunction, null));
        }
        return this.getNodeAsAggregateExprDoc((RexNode)call);
    }
    
    static {
        logger = LoggerFactory.getLogger(FindQueryGenerator.class);
    }
}
