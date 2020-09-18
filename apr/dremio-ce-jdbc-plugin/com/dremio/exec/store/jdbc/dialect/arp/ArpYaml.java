package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.dataformat.yaml.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.google.common.io.*;
import java.nio.charset.*;
import java.net.*;
import java.io.*;
import com.fasterxml.jackson.annotation.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.jdbc.dialect.*;
import com.dremio.exec.work.foreman.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.rel.core.*;
import com.dremio.common.dialect.arp.transformer.*;
import org.apache.calcite.avatica.util.*;
import com.google.common.base.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.config.*;
import org.apache.calcite.sql.*;
import java.util.*;
import org.slf4j.*;

public final class ArpYaml
{
    private static final Logger logger;
    private final Metadata metadata;
    private final Syntax syntax;
    private final DataTypes dataTypes;
    private final RelationalAlgebraOperations relationalAlgebra;
    private final Expressions expressions;
    
    public static ArpYaml createFromFile(final String arpFile) throws IOException {
        final ObjectMapper mapper = new ObjectMapper((JsonFactory)new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        final URL url = Resources.getResource(arpFile);
        final String content = Resources.toString(url, StandardCharsets.UTF_8);
        return (ArpYaml)mapper.readValue(content, (Class)ArpYaml.class);
    }
    
    private ArpYaml(@JsonProperty("metadata") final Metadata meta, @JsonProperty("syntax") final Syntax syntax, @JsonProperty("data_types") final DataTypes dataTypes, @JsonProperty("relational_algebra") final RelationalAlgebraOperations algebra, @JsonProperty("expressions") final Expressions expressions) {
        this.metadata = meta;
        this.syntax = syntax;
        this.dataTypes = dataTypes;
        this.relationalAlgebra = algebra;
        this.expressions = expressions;
    }
    
    public Metadata getMetadata() {
        return this.metadata;
    }
    
    public Syntax getSyntax() {
        return this.syntax;
    }
    
    public boolean supportsLiteral(final CompleteType type) {
        if (type.isDecimal()) {
            final Mapping mapping = this.dataTypes.getDefaultCastSpecMap().get(type.getSqlTypeName());
            return null != mapping && (null == mapping.getSource().getMaxScale() || type.getScale() <= mapping.getSource().getMaxScale()) && (null == mapping.getSource().getMaxPrecision() || type.getPrecision() <= mapping.getSource().getMaxPrecision());
        }
        return this.dataTypes.getDefaultCastSpecMap().containsKey(type.getSqlTypeName());
    }
    
    public Mapping getMapping(final SourceTypeDescriptor sourceType) throws UnsupportedDataTypeException {
        final String dataSourceName = sourceType.getDataSourceTypeName();
        final Mapping mapping = this.dataTypes.getSourceTypeToMappingMap().get(dataSourceName);
        if (null == mapping) {
            final int index = dataSourceName.lastIndexOf(40);
            if (-1 != index && index < dataSourceName.lastIndexOf(41)) {
                return this.dataTypes.getSourceTypeToMappingMap().get(dataSourceName.substring(0, index).trim());
            }
        }
        return mapping;
    }
    
    public SqlNode getCastSpec(final RelDataType type) {
        final CompleteType completeType = SourceTypeDescriptor.getType(type);
        final Mapping mapping = this.dataTypes.getDefaultCastSpecMap().get(completeType.getSqlTypeName());
        if (mapping == null) {
            ArpYaml.logger.debug("No cast spec found for type: '{}'", (Object)type);
            return null;
        }
        int precision = type.getPrecision();
        if (completeType.isDecimal()) {
            if (this.isInvalidDecimal(completeType, mapping)) {
                return null;
            }
            precision = 38;
        }
        return (SqlNode)new SourceTypeSpec(mapping.getSource().getName().toUpperCase(Locale.ROOT), mapping.getRequiredCastArgs(), precision, type.getScale());
    }
    
    public SqlNode getSqlNodeForOperator(final SqlCall sqlCall, final RexCall rexCall, final CallTransformer transformer) {
        if (ArpYaml.logger.isDebugEnabled()) {
            ArpYaml.logger.debug("Searching for scalar operator {} or windowed aggregate for SQL generation.", (Object)rexCall);
        }
        Signature sig;
        if (sqlCall.isA((Set)SqlKind.AGGREGATE)) {
            if (sqlCall.getKind() == SqlKind.COUNT) {
                return this.getNodeForCountOperation(sqlCall);
            }
            final OperatorDescriptor op = OperatorDescriptor.createFromRexCall(rexCall, transformer, false, false);
            ArpYaml.logger.debug("Searching in aggregation/functions YAML section for operator {}", (Object)op);
            sig = this.relationalAlgebra.getAggregation().getFunctionMap().get(op);
        }
        else {
            if (sqlCall.getKind() == SqlKind.OTHER_FUNCTION && "TO_DATE".equalsIgnoreCase(sqlCall.getOperator().getName())) {
                final SqlNode operand = sqlCall.getOperandList().get(1);
                if (operand.getKind().equals((Object)SqlKind.LITERAL)) {
                    final String formatStr = (String)((SqlLiteral)operand).getValueAs((Class)String.class);
                    if (null != formatStr) {
                        final String transformedFormatStr = this.transformDateTimeFormatString(formatStr);
                        sqlCall.setOperand(1, (SqlNode)SqlLiteral.createCharString(transformedFormatStr, SqlParserPos.ZERO));
                    }
                }
            }
            OperatorDescriptor op = OperatorDescriptor.createFromRexCall(rexCall, transformer, false, false);
            ArpYaml.logger.debug("Searching in operators YAML section for operator {}", (Object)op);
            sig = this.expressions.getOperators().get(op);
            if (sig == null) {
                op = OperatorDescriptor.createFromRexCall(rexCall, transformer, false, true);
                ArpYaml.logger.debug("Searching in variable_length_operators YAML section for operator {}", (Object)op);
                sig = this.expressions.getVariableOperators().get(OperatorDescriptor.createFromRexCall(rexCall, transformer, false, true));
            }
        }
        if (sig == null || !sig.hasRewrite()) {
            ArpYaml.logger.debug("No rewriting signature found. Returning default unparsing syntax.");
            return (SqlNode)sqlCall;
        }
        ArpYaml.logger.debug("Applying rewrite during unparsing: {}", (Object)sig);
        return (SqlNode)new ArpSqlCall(sig, sqlCall, transformer);
    }
    
    public SqlNode getSqlNodeForOperator(final SqlCall undecoratedNode, final AggregateCall aggCall, final List<RelDataType> types) {
        if (ArpYaml.logger.isDebugEnabled()) {
            ArpYaml.logger.debug("Searching for aggregate function {} with params {} for SQL generation.", (Object)aggCall.getName(), (Object)types);
        }
        if (undecoratedNode.getOperator().getKind() == SqlKind.COUNT) {
            return this.getNodeForCountOperation(undecoratedNode);
        }
        final Signature sig = this.relationalAlgebra.getAggregation().getFunctionMap().get(OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(aggCall.getAggregation().getName()), aggCall.isDistinct(), aggCall.getType(), types, false));
        if (sig == null || !sig.hasRewrite()) {
            ArpYaml.logger.debug("No rewriting signature found. Returning default unparsing syntax.");
            return (SqlNode)undecoratedNode;
        }
        ArpYaml.logger.debug("Applying rewrite during unparsing: {}", (Object)sig);
        return (SqlNode)new ArpSqlCall(sig, undecoratedNode, (CallTransformer)NoOpTransformer.INSTANCE);
    }
    
    public boolean supportsScalarOperator(final SqlOperator operator, final List<RelDataType> argTypes, final RelDataType returnType) {
        ArpYaml.logger.debug("Identifying if operator {} is supported.", (Object)operator.getName());
        OperatorDescriptor op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()), false, returnType, argTypes, false);
        ArpYaml.logger.debug("Searching for operator {} in expressions/operators section in YAML.", (Object)op);
        boolean supportsOperator = this.expressions.getOperators().containsKey(op);
        if (!supportsOperator) {
            ArpYaml.logger.debug("Operator not found in expressions/operators section.");
            op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()), false, returnType, argTypes, true);
            ArpYaml.logger.debug("Searching for operator {} in variable_length_operators section in YAML.", (Object)op);
            supportsOperator = this.expressions.getVariableOperators().containsKey(op);
        }
        if (!supportsOperator) {
            ArpYaml.logger.debug("Operator {} not supported. Aborting pushdown.", (Object)operator.getName());
        }
        else {
            ArpYaml.logger.debug("Operator {} supported.", (Object)operator.getName());
        }
        return supportsOperator;
    }
    
    public boolean supportsAggregate(final SqlOperator operator, final boolean isDistinct, final List<RelDataType> argTypes, final RelDataType returnType) {
        ArpYaml.logger.debug("Checking if aggregation is enabled.");
        if (!this.relationalAlgebra.getAggregation().isEnabled()) {
            return false;
        }
        final OperatorDescriptor op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()), isDistinct, returnType, argTypes, false);
        ArpYaml.logger.debug("Searching aggregation/functions in YAML for aggregate {}", (Object)op);
        final boolean supportsAggregate = this.relationalAlgebra.getAggregation().getFunctionMap().containsKey(op);
        if (!supportsAggregate) {
            ArpYaml.logger.debug("Aggregate {} not supported. Aborting pushdown.", (Object)op);
        }
        else {
            ArpYaml.logger.debug("Aggregate {} supported.", (Object)op);
        }
        return supportsAggregate;
    }
    
    public boolean supportsTimeUnitFunction(final SqlOperator operator, final TimeUnitRange unit, final List<RelDataType> argTypes, final RelDataType returnType) {
        Preconditions.checkArgument(argTypes.size() > 1, (Object)String.format("At least one argument other than time unit is expected for %s.", OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName())));
        final OperatorDescriptor op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()) + "_" + unit.toString(), false, returnType, argTypes.subList(1, argTypes.size()), false);
        ArpYaml.logger.debug("Checking expressions/operators the time unit-based function {} is supported.", (Object)op);
        final boolean isSupported = this.expressions.getOperators().containsKey(op);
        if (!isSupported) {
            ArpYaml.logger.debug("Time unit {} not supported. Aborting pushdown.", (Object)op);
        }
        else {
            ArpYaml.logger.debug("Time unit function {} supported.", (Object)op);
        }
        return isSupported;
    }
    
    public boolean supportsAggregation() {
        return this.relationalAlgebra.getAggregation().isEnabled();
    }
    
    public boolean supportsDistinct() {
        return this.relationalAlgebra.getAggregation().supportsDistinct();
    }
    
    public boolean supportsCountOperation(final AggregateCall call) {
        Preconditions.checkArgument(call.getAggregation().getKind() == SqlKind.COUNT);
        ArpYaml.logger.debug("Checking if count operation is supported.");
        CountOperations.CountOperationType opType;
        if (call.isDistinct()) {
            if (call.getArgList().size() > 1) {
                opType = CountOperations.CountOperationType.COUNT_DISTINCT_MULTI;
            }
            else {
                opType = CountOperations.CountOperationType.COUNT_DISTINCT;
            }
        }
        else if (call.getArgList().isEmpty()) {
            opType = CountOperations.CountOperationType.COUNT_STAR;
        }
        else if (call.getArgList().size() > 1) {
            opType = CountOperations.CountOperationType.COUNT_MULTI;
        }
        else {
            opType = CountOperations.CountOperationType.COUNT;
        }
        ArpYaml.logger.debug("Searching count_functions section in YAML for {}.", (Object)opType);
        if (this.relationalAlgebra.getAggregation().getCountOperation(opType).isEnable()) {
            ArpYaml.logger.debug("Count function supported.");
            return true;
        }
        ArpYaml.logger.debug("Count function not supported. Aborting pushdown.");
        return false;
    }
    
    public boolean supportsCountOperation(final SqlOperator op, final List<RelDataType> args) {
        Preconditions.checkArgument(op.getKind() == SqlKind.COUNT);
        ArpYaml.logger.debug("Checking if count operation is supported.");
        boolean isSupported;
        if (args.isEmpty()) {
            ArpYaml.logger.debug("Searching count_functions section in YAML for count_star.");
            isSupported = this.relationalAlgebra.getAggregation().getCountOperation(CountOperations.CountOperationType.COUNT_STAR).isEnable();
        }
        else {
            ArpYaml.logger.debug("Searching count_functions section in YAML for count.");
            isSupported = this.relationalAlgebra.getAggregation().getCountOperation(CountOperations.CountOperationType.COUNT).isEnable();
        }
        if (isSupported) {
            ArpYaml.logger.debug("Count function supported.");
        }
        else {
            ArpYaml.logger.debug("Count function not supported. Aborting pushdown.");
        }
        return isSupported;
    }
    
    private SqlNode getNodeForCountOperation(final SqlCall call) {
        Preconditions.checkArgument(call.getOperator() == SqlStdOperatorTable.COUNT);
        ArpYaml.logger.debug("Checking if count operation has a variable_rewrite.");
        CountOperations.CountOperationType opType;
        if (call.getFunctionQuantifier() != null && call.getFunctionQuantifier().getValue().equals(SqlSelectKeyword.DISTINCT)) {
            if (call.operandCount() > 1) {
                opType = CountOperations.CountOperationType.COUNT_DISTINCT_MULTI;
            }
            else {
                opType = CountOperations.CountOperationType.COUNT_DISTINCT;
            }
        }
        else if (call.operandCount() == 0) {
            opType = CountOperations.CountOperationType.COUNT_STAR;
        }
        else if (call.operandCount() > 1) {
            opType = CountOperations.CountOperationType.COUNT_MULTI;
        }
        else {
            opType = CountOperations.CountOperationType.COUNT;
        }
        ArpYaml.logger.debug("Searching count_functions section in YAML for {}.", (Object)opType);
        final Signature sig = this.relationalAlgebra.getAggregation().getCountOperation(opType).getSignature();
        if (sig == null) {
            ArpYaml.logger.debug("Count rewrite not needed.");
            return (SqlNode)call;
        }
        ArpYaml.logger.debug("Count variable rewrite: {}", (Object)sig);
        return (SqlNode)new ArpSqlCall(sig, call, (CallTransformer)NoOpTransformer.INSTANCE);
    }
    
    private boolean isInvalidDecimal(final CompleteType completeType, final Mapping mapping) {
        return (null != mapping.getSource().getMaxPrecision() && completeType.getPrecision() > mapping.getSource().getMaxPrecision()) || (null != mapping.getSource().getMaxScale() && (completeType.getScale() > mapping.getSource().getMaxScale() || completeType.getScale() < 0));
    }
    
    public String getValuesDummyTable() {
        if (this.relationalAlgebra.getValues().getMethod() == Values.Method.DUMMY_TABLE) {
            return this.relationalAlgebra.getValues().getDummyTable();
        }
        return null;
    }
    
    public boolean supportsUnion() {
        return this.relationalAlgebra.getUnion().isEnabled();
    }
    
    public boolean supportsUnionAll() {
        return this.relationalAlgebra.getUnionAll().isEnabled();
    }
    
    public boolean allowsSortInSetOperand() {
        return this.relationalAlgebra.supportsSortInSetOperator();
    }
    
    public boolean supportsLimit() {
        return this.relationalAlgebra.getSort().getFetchOffset().getFetchOnly().isEnable();
    }
    
    public boolean supportsFetchOffset() {
        return this.relationalAlgebra.getSort().getFetchOffset().getOffsetFetch().isEnable();
    }
    
    public boolean supportsOffset() {
        return this.relationalAlgebra.getSort().getFetchOffset().getOffsetOnly().isEnable();
    }
    
    public boolean supportsOrderBy() {
        return this.relationalAlgebra.getSort().getOrderBy().isEnabled();
    }
    
    public NullCollation getNullCollation() {
        switch (this.relationalAlgebra.getSort().getOrderBy().getDefaultNullsOrdering()) {
            case FIRST: {
                return NullCollation.FIRST;
            }
            case HIGH: {
                return NullCollation.HIGH;
            }
            case LAST: {
                return NullCollation.LAST;
            }
            default: {
                return NullCollation.LOW;
            }
        }
    }
    
    public String getLimitFormat() {
        return this.relationalAlgebra.getSort().getFetchOffset().getFetchOnly().getFormat();
    }
    
    public String getFetchOffsetFormat() {
        return this.relationalAlgebra.getSort().getFetchOffset().getOffsetFetch().getFormat();
    }
    
    public String getOffsetFormat() {
        return this.relationalAlgebra.getSort().getFetchOffset().getOffsetOnly().getFormat();
    }
    
    public JoinOp getJoinSupport(final JoinType joinType) {
        final JoinOp joinOp = this.relationalAlgebra.getJoin().getJoinOp(joinType);
        ArpYaml.logger.debug("Searching join section in YAML for join type '{}', found {}", (Object)joinType, (Object)joinOp);
        return joinOp;
    }
    
    public DateTimeFormatSupport getDateTimeFormatSupport() {
        return this.expressions.getDateTimeFormatSupport();
    }
    
    public boolean supportsSubquery() {
        return this.expressions.getSubQuerySupport().isEnabled();
    }
    
    public boolean supportsCorrelatedSubquery() {
        return this.expressions.getSubQuerySupport().getCorrelatedSubQuerySupport();
    }
    
    public boolean supportsScalarSubquery() {
        return this.expressions.getSubQuerySupport().getScalarSupport();
    }
    
    public boolean supportsInClause() {
        return this.expressions.getSubQuerySupport().getInClauseSupport();
    }
    
    public boolean shouldInjectNumericCastToProject() {
        return this.syntax.shouldInjectNumericCastToProject();
    }
    
    public boolean shouldInjectApproxNumericCastToProject() {
        return this.syntax.shouldInjectApproxNumericCastToProject();
    }
    
    private String transformDateTimeFormatString(String dremioDateTimeFormatStr) {
        for (final DateTimeFormatSupport.DateTimeFormatMapping dtFormat : this.getDateTimeFormatSupport().getDateTimeFormatMappings()) {
            if (dremioDateTimeFormatStr.contains(dtFormat.getDremioDateTimeFormatString())) {
                if (null == dtFormat.getSourceDateTimeFormat() || !dtFormat.getSourceDateTimeFormat().isEnable()) {
                    throw new IllegalStateException("Datetime string format '" + dtFormat.getDremioDateTimeFormatString() + "' is not supported.");
                }
                if (dtFormat.areDateTimeFormatsEqual()) {
                    continue;
                }
                final String formatRegex = "((" + dtFormat.getDremioDateTimeFormatString() + ")(?=(?:[^\"]|\"[^\"]*\")*$))+";
                dremioDateTimeFormatStr = dremioDateTimeFormatStr.replaceAll(formatRegex, dtFormat.getSourceDateTimeFormat().getFormat());
            }
        }
        return dremioDateTimeFormatStr;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)ArpYaml.class);
    }
}
