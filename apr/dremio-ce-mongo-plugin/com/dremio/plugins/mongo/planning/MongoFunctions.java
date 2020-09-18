package com.dremio.plugins.mongo.planning;

import com.dremio.plugins.*;
import java.util.*;
import com.dremio.plugins.mongo.*;
import com.google.common.collect.*;

public enum MongoFunctions
{
    EQUAL("=", "$eq", "$eq", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    NOT_EQUAL("<>", "$ne", "$ne", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    GREATER_OR_EQUAL(">=", "$gte", "$lte", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    GREATER(">", "$gt", "$lt", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    LESS_OR_EQUAL("<=", "$lte", "$gte", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    LESS("<", "$lt", "$gt", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    AND("and", "$and", "$and", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    OR("or", "$or", "$or", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    NOT("not", "$not", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH), 
    ABS("abs", "$abs", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    ADD("+", "$add", "$add", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    CEIL("ceil", "$ceil", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    DIVIDE(new String[] { "divide", "/" }, "$divide", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    EXP("exp", "$exp", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    FLOOR("floor", "$floor", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    LN("ln", "$ln", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    LOG("log", "$log", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    LOG10("log10", "$log10", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    MAX("max", "$max", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    MIN("min", "$min", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    MOD("mod", "$mod", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    MULTIPLY("*", "$multiply", "$multiply", MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    POW("power", "$pow", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    SQRT("sqrt", "$sqrt", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    SUBTRACT("-", "$subtract", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    TRUNC(new String[] { "trunc", "truncate" }, "$trunc", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    REGEX("like", "$regex", "$regex", MongoConstants.VERSION_0_0, MongoPipelineOperators.MATCH_ONLY), 
    CONCAT(new String[] { "||", "concat" }, "$concat", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    SUBSTR(new String[] { "substring", "substr" }, "$substr", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    TO_LOWER("lower", "$toLower", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    TO_UPPER("upper", "$toUpper", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    EXTRACT("extract", (String)null, (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    YEAR("extract_year", "$year", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    MONTH("extract_month", "$month", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    HOUR("extract_hour", "$hour", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    MINUTE("extract_minute", "$minute", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    SECOND("extract_second", "$second", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    DAY_OF_MONTH("extract_day", "$dayOfMonth", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    DATE_TO_STRING("dateToString", "$dateToString", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    CASE("case", "$cond", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    IFNULL("is null", "$ifNull", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    IFNOTNULL("is not null", "$ifNotNull", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    ITEM("item", (String)null, (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    ARRAYELEMAT("", "$arrayElemAt", (String)null, MongoConstants.VERSION_3_2, MongoPipelineOperators.PROJECT_ONLY), 
    CAST("cast", (String)null, (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_ONLY), 
    LITERAL("", "$literal", (String)null, MongoConstants.VERSION_0_0, MongoPipelineOperators.PROJECT_MATCH);
    
    private final String[] operators;
    private final String mongoOperator;
    private final String flipped;
    private final Version version;
    private final Set<MongoPipelineOperators> stagesSupported;
    private static final Map<String, MongoFunctions> OPERATOR_MAP;
    
    private MongoFunctions(final String operator, final String mongoOperator, final String flippedOperator, final Version version, final Set<MongoPipelineOperators> supportedStages) {
        this(new String[] { operator }, mongoOperator, flippedOperator, version, supportedStages);
    }
    
    private MongoFunctions(final String[] operators, final String operator, final String flippedOperator, final Version version, final Set<MongoPipelineOperators> supportedStages) {
        this.mongoOperator = operator;
        this.flipped = flippedOperator;
        this.version = version;
        this.operators = operators;
        this.stagesSupported = supportedStages;
    }
    
    public String getMongoOperator() {
        if (this.mongoOperator == null) {
            throw new RuntimeException("Tried to get invalid MongoOperator for " + Arrays.toString(this.operators));
        }
        return this.mongoOperator;
    }
    
    public boolean canFlip() {
        return this.flipped != null;
    }
    
    public String getFlipped() {
        return this.flipped;
    }
    
    public boolean supportedInVersion(final String version) {
        return this.version.compareTo(Version.parse(version)) <= 0;
    }
    
    public Version getVersion() {
        return this.version;
    }
    
    public String[] getOperators() {
        return this.operators;
    }
    
    public static MongoFunctions getMongoOperator(final String functionName) {
        return MongoFunctions.OPERATOR_MAP.get(functionName.toLowerCase());
    }
    
    public boolean isProjectOnly() {
        return this.stagesSupported == MongoPipelineOperators.PROJECT_ONLY;
    }
    
    public boolean canUseInStage(final MongoPipelineOperators stage) {
        return stage != null && this.stagesSupported != null && !this.stagesSupported.isEmpty() && this.stagesSupported.contains(stage);
    }
    
    public static String convertToMongoFunction(final String sqlFunction) {
        final MongoFunctions mongoOp = getMongoOperator(sqlFunction);
        if (mongoOp != null && mongoOp.canUseInStage(MongoPipelineOperators.PROJECT)) {
            return mongoOp.getMongoOperator();
        }
        throw new RuntimeException(String.format("Unsupported function %s.", sqlFunction));
    }
    
    static {
        final ImmutableMap.Builder<String, MongoFunctions> builder = (ImmutableMap.Builder<String, MongoFunctions>)ImmutableMap.builder();
        for (final MongoFunctions operator : values()) {
            if (operator.getOperators() != null && operator.getOperators().length != 0) {
                for (final String op : operator.getOperators()) {
                    if (op != null && !op.isEmpty()) {
                        builder.put((Object)op, (Object)operator);
                    }
                }
            }
        }
        OPERATOR_MAP = (Map)builder.build();
    }
}
