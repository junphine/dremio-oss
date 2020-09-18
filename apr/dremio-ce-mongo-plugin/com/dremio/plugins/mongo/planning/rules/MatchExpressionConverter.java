package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.record.*;
import org.bson.*;
import org.apache.calcite.rel.type.*;
import com.dremio.plugins.mongo.planning.*;
import com.dremio.common.exceptions.*;
import org.apache.calcite.rex.*;
import java.util.*;
import com.dremio.exec.expr.fn.impl.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;
import org.slf4j.*;

@Deprecated
public class MatchExpressionConverter extends ProjectExpressionConverter
{
    private static final Logger logger;
    public static final String UNSUPPORTED_REX_NODE_ERROR = "Cannot convert RexNode to equivalent MongoDB match expression.";
    
    public MatchExpressionConverter(final BatchSchema schema, final RelDataType dataType, final boolean mongo3_2enabled, final boolean mongo3_4enabled) {
        super(schema, dataType, mongo3_2enabled, mongo3_4enabled);
    }
    
    @Override
    public Document visitInputRef(final RexInputRef inputRef) {
        final int index = inputRef.getIndex();
        final RelDataTypeField field = this.recordType.getFieldList().get(index);
        return new Document(field.getName(), (Object)true);
    }
    
    @Override
    public Object visitLiteral(final RexLiteral literal) {
        return this.getLiteralDocument(literal).get((Object)MongoFunctions.LITERAL.getMongoOperator());
    }
    
    @Override
    public Object visitCall(final RexCall call) {
        final String funcName = call.getOperator().getName().toLowerCase();
        final MongoFunctions operator = MongoFunctions.getMongoOperator(funcName);
        if (!operator.canUseInStage(MongoPipelineOperators.MATCH)) {
            throw UserException.planError().message("Found mongo operator " + operator.getMongoOperator() + ", which cannot be used in mongo pipeline, " + MongoPipelineOperators.MATCH.getOperator(), new Object[0]).build(MatchExpressionConverter.logger);
        }
        switch (operator) {
            case AND:
            case OR: {
                return this.handleGenericFunction(call, funcName);
            }
            case NOT:
            case EQUAL:
            case NOT_EQUAL:
            case GREATER:
            case GREATER_OR_EQUAL:
            case LESS:
            case LESS_OR_EQUAL: {
                return this.handleGenericMatchFunction(call, funcName);
            }
            case REGEX: {
                return this.handleLikeFunction(call);
            }
            default: {
                return this.visitUnknown((RexNode)call);
            }
        }
    }
    
    public static boolean checkForUnneededCast(final RexNode expr) {
        if (expr instanceof RexCall) {
            final RexCall arg2Call = (RexCall)expr;
            final RelDataType inputType = arg2Call.getOperands().get(0).getType();
            if (arg2Call.getOperator().getName().equalsIgnoreCase("CAST") && (arg2Call.getType().equals(inputType) || inputType.getSqlTypeName().equals((Object)SqlTypeName.ANY) || arg2Call.getType().getSqlTypeName().equals((Object)SqlTypeName.ANY) || arg2Call.getType().getFamily().equals(inputType.getFamily()))) {
                return true;
            }
        }
        return false;
    }
    
    private Object handleGenericMatchFunction(final RexCall call, final String funcName) {
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        assert operands.size() == 2;
        assert MongoRulesUtil.isInputRef(operands.get(0)) || MongoRulesUtil.isSupportedCast(operands.get(1));
        final boolean firstArgInputRef = MongoRulesUtil.isInputRef(operands.get(0)) || MongoRulesUtil.isSupportedCast(operands.get(0));
        RexNode colRef;
        RexNode otherArgValue;
        Object otherArg;
        try {
            if (!firstArgInputRef) {
                colRef = operands.get(1);
                otherArgValue = operands.get(0);
            }
            else {
                colRef = operands.get(0);
                otherArgValue = operands.get(1);
            }
            this.leftFieldType = this.getFieldType(colRef);
            otherArg = otherArgValue.accept((RexVisitor)this);
        }
        finally {
            this.leftFieldType = null;
        }
        final MongoFunctions mongoOperator = MongoFunctions.getMongoOperator(funcName);
        if (mongoOperator == null) {
            throw new RuntimeException("Encountered a function that cannot be converted to MongoOperator, " + funcName);
        }
        if (!mongoOperator.canUseInStage(MongoPipelineOperators.MATCH)) {
            throw UserException.planError().message("Found mongo operator " + mongoOperator.getMongoOperator() + ", which cannot be used in mongo pipeline, " + MongoPipelineOperators.MATCH.getOperator(), new Object[0]).build(MatchExpressionConverter.logger);
        }
        String mongoFunction;
        if (!firstArgInputRef) {
            if (!mongoOperator.canFlip()) {
                throw new RuntimeException("Encountered a function that cannot swap the left and right operads, " + funcName);
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
        return new Document(compoundFieldName, (Object)new Document(mongoFunction, otherArg));
    }
    
    private Object handleLikeFunction(final RexCall call) {
        final List<RexNode> operands = (List<RexNode>)call.getOperands();
        assert operands.size() == 2;
        RexLiteral arg2;
        RexInputRef arg3;
        if (!(operands.get(0) instanceof RexInputRef)) {
            arg2 = (RexLiteral)operands.get(0);
            arg3 = (RexInputRef)operands.get(1);
        }
        else {
            arg2 = (RexLiteral)operands.get(1);
            arg3 = (RexInputRef)operands.get(0);
        }
        String sqlRegex = arg2.toString();
        sqlRegex = sqlRegex.substring(1, sqlRegex.length() - 1);
        final String regex = RegexpUtil.sqlToRegexLike(sqlRegex);
        final int index = arg3.getIndex();
        final RelDataTypeField field = this.recordType.getFieldList().get(index);
        return new Document(field.getName(), (Object)new Document(MongoFunctions.REGEX.getMongoOperator(), (Object)regex.toString()));
    }
    
    @Override
    protected String visitUnknown(final RexNode o) {
        throw UserException.planError().message("Cannot convert RexNode to equivalent MongoDB match expression.RexNode Class: %s, RexNode Digest: %s", new Object[] { o.getClass().getName(), o.toString() }).build(MatchExpressionConverter.logger);
    }
    
    private static boolean isCollationEquality(final RexCall call, final RexNode colRef, final RexNode otherValue) {
        return call.isA(SqlKind.EQUALS) && otherValue instanceof RexLiteral && colRef.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER && otherValue.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)MatchExpressionConverter.class);
    }
}
