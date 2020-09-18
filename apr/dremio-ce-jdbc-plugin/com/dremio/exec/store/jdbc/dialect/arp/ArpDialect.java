package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.jdbc.dialect.*;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.avatica.util.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.sql.pretty.*;
import java.text.*;
import com.dremio.common.dialect.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.rex.*;
import java.util.stream.*;
import java.util.function.*;
import org.apache.calcite.sql.parser.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.*;
import org.slf4j.*;
import com.google.common.annotations.*;
import javax.sql.*;
import com.dremio.exec.store.jdbc.*;
import java.sql.*;
import java.util.*;
import com.google.common.base.*;
import com.dremio.connector.metadata.*;
import com.dremio.common.*;
import com.google.common.collect.*;
import com.dremio.common.dialect.arp.transformer.*;
import com.dremio.exec.store.jdbc.dialect.arp.transformer.*;

public class ArpDialect extends JdbcDremioSqlDialect
{
    private final ArpYaml yaml;
    private final ArpTypeMapper typeMapper;
    private static final Set<SqlKind> MANDATORY_OPERATORS;
    private static final Logger logger;
    
    public ArpDialect(final ArpYaml yaml) {
        super(yaml.getMetadata().getName(), yaml.getSyntax().getIdentifierQuote(), yaml.getNullCollation());
        this.yaml = yaml;
        this.typeMapper = new ArpTypeMapper(yaml);
    }
    
    public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec, final int rightPrec) {
        if (call.getOperator() instanceof SqlMonotonicBinaryOperator || call.getOperator() == SqlStdOperatorTable.DIVIDE || call.getOperator() == SqlStdOperatorTable.AND || call.getOperator() == SqlStdOperatorTable.OR) {
            final SqlWriter.Frame frame = writer.startList("(", ")");
            super.unparseCall(writer, call, leftPrec, rightPrec);
            writer.endList(frame);
        }
        else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
        }
    }
    
    public SqlNode emulateNullDirection(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        return this.emulateNullDirectionWithIsNull(node, nullsFirst, desc);
    }
    
    public boolean shouldInjectNumericCastToProject() {
        return this.yaml.shouldInjectNumericCastToProject();
    }
    
    public boolean shouldInjectApproxNumericCastToProject() {
        return this.yaml.shouldInjectApproxNumericCastToProject();
    }
    
    public boolean supportsLiteral(final CompleteType type) {
        return this.yaml.supportsLiteral(type);
    }
    
    public boolean supportsAggregateFunction(final SqlKind kind) {
        return true;
    }
    
    public boolean supportsAggregation() {
        return this.yaml.supportsAggregation();
    }
    
    public boolean supportsDateTimeFormatString(String dateTimeFormatStr) {
        dateTimeFormatStr = dateTimeFormatStr.replaceAll("\".*?\"+", "");
        for (final DateTimeFormatSupport.DateTimeFormatMapping dtFormat : this.yaml.getDateTimeFormatSupport().getDateTimeFormatMappings()) {
            if (dateTimeFormatStr.contains(dtFormat.getDremioDateTimeFormatString())) {
                if (null == dtFormat.getSourceDateTimeFormat() || !dtFormat.getSourceDateTimeFormat().isEnable()) {
                    return false;
                }
                dateTimeFormatStr = dateTimeFormatStr.replaceAll(dtFormat.getDremioDateTimeFormatString(), "");
            }
        }
        return true;
    }
    
    public boolean supportsDistinct() {
        return this.yaml.supportsDistinct();
    }
    
    @Override
    public TypeMapper getDataTypeMapper() {
        return this.typeMapper;
    }
    
    public SqlNode getCastSpec(final RelDataType type) {
        return this.yaml.getCastSpec(type);
    }
    
    public boolean supportsFunction(final SqlOperator operator, final RelDataType type, final List<RelDataType> paramTypes) {
        if (ArpDialect.MANDATORY_OPERATORS.contains(operator.getKind()) || (operator == SqlStdOperatorTable.CAST && this.getCastSpec(type) != null)) {
            return true;
        }
        if (operator.requiresOver()) {
            return super.supportsFunction(operator, type, (List)paramTypes);
        }
        if (!operator.isAggregator()) {
            return this.yaml.supportsScalarOperator(operator, paramTypes, type);
        }
        if (operator.getKind() == SqlKind.COUNT) {
            return this.yaml.supportsCountOperation(operator, paramTypes);
        }
        return this.yaml.supportsAggregate(operator, false, paramTypes, type);
    }
    
    public boolean supportsCount(final AggregateCall call) {
        return this.yaml.supportsCountOperation(call);
    }
    
    public boolean supportsFunction(final AggregateCall aggCall, final List<RelDataType> paramTypes) {
        return this.yaml.supportsAggregate((SqlOperator)aggCall.getAggregation(), aggCall.isDistinct(), paramTypes, aggCall.getType());
    }
    
    public boolean supportsTimeUnitFunction(final SqlOperator operator, final TimeUnitRange timeUnit, final RelDataType returnType, final List<RelDataType> paramTypes) {
        return this.yaml.supportsTimeUnitFunction(operator, timeUnit, paramTypes, returnType);
    }
    
    public boolean supportsSubquery() {
        return this.yaml.supportsSubquery();
    }
    
    public boolean supportsCorrelatedSubquery() {
        return this.yaml.supportsCorrelatedSubquery();
    }
    
    public String getValuesDummyTable() {
        return this.yaml.getValuesDummyTable();
    }
    
    public boolean supportsFetchOffsetInSetOperand() {
        return this.yaml.allowsSortInSetOperand();
    }
    
    public boolean supportsUnion() {
        return this.yaml.supportsUnion();
    }
    
    public boolean supportsUnionAll() {
        return this.yaml.supportsUnionAll();
    }
    
    public boolean supportsSort(final boolean isCollationEmpty, final boolean isOffsetEmpty) {
        final boolean requiresLimit = true;
        final boolean requiresOrderBy = !isCollationEmpty;
        final boolean requiresOffset = !isOffsetEmpty;
        return (!requiresOrderBy || this.yaml.supportsOrderBy()) && (!requiresOffset || this.yaml.supportsFetchOffset()) && (requiresOffset || this.yaml.supportsLimit());
    }
    
    public boolean supportsSort(final Sort sort) {
        final boolean requiresOrderBy = !JdbcSort.isCollationEmpty(sort);
        final boolean requiresOffsetAndFetch = !JdbcSort.isOffsetEmpty(sort) && sort.fetch != null;
        final boolean requiresLimitWithoutOffset = sort.fetch != null && JdbcSort.isOffsetEmpty(sort);
        final boolean requiresOffsetWithoutLimit = sort.fetch == null && !JdbcSort.isOffsetEmpty(sort);
        return (this.yaml.supportsOrderBy() || !requiresOrderBy) && (this.yaml.supportsFetchOffset() || !requiresOffsetAndFetch) && (this.yaml.supportsLimit() || !requiresLimitWithoutOffset) && (this.yaml.supportsOffset() || !requiresOffsetWithoutLimit);
    }
    
    public void unparseOffsetFetch(final SqlWriter writer, final SqlNode offset, final SqlNode fetch) {
        final SqlPrettyWriter tempWriter = new SqlPrettyWriter(writer.getDialect());
        tempWriter.setAlwaysUseParentheses(false);
        tempWriter.setSelectListItemsOnSeparateLines(false);
        tempWriter.setIndentation(0);
        String offsetString = null;
        String fetchString = null;
        if (offset != null) {
            offset.unparse((SqlWriter)tempWriter, 0, 0);
            offsetString = tempWriter.toString();
            tempWriter.reset();
        }
        if (fetch != null) {
            fetch.unparse((SqlWriter)tempWriter, 0, 0);
            fetchString = tempWriter.toString();
            tempWriter.reset();
        }
        if (offset != null && fetch != null) {
            writer.print(MessageFormat.format(this.yaml.getFetchOffsetFormat(), offsetString, fetchString));
        }
        else if (offset != null) {
            writer.print(MessageFormat.format(this.yaml.getOffsetFormat(), offsetString));
        }
        else if (fetch != null) {
            writer.print(MessageFormat.format(this.yaml.getLimitFormat(), fetchString));
        }
    }
    
    public boolean supportsJoin(final JoinType type) {
        return this.yaml.getJoinSupport(type).isEnable();
    }
    
    public void unparseJoin(final SqlWriter writer, final SqlJoin join, final int leftPrec, final int rightPrec) {
        JoinType type = join.getJoinType();
        if (type == JoinType.COMMA) {
            type = JoinType.CROSS;
        }
        final String rewriteFormat = this.yaml.getJoinSupport(type).getRewrite();
        if (rewriteFormat == null) {
            super.unparseJoin(writer, join, leftPrec, rightPrec);
            return;
        }
        final SqlPrettyWriter tempWriter = new SqlPrettyWriter(writer.getDialect());
        tempWriter.setAlwaysUseParentheses(false);
        tempWriter.setSelectListItemsOnSeparateLines(false);
        tempWriter.setIndentation(0);
        join.getLeft().unparse((SqlWriter)tempWriter, 0, 0);
        final String left = tempWriter.toString();
        tempWriter.reset();
        join.getRight().unparse((SqlWriter)tempWriter, 0, 0);
        final String right = tempWriter.toString();
        tempWriter.reset();
        if (type == JoinType.CROSS) {
            writer.print(MessageFormat.format(rewriteFormat, left, right));
        }
        else {
            join.getCondition().unparse((SqlWriter)tempWriter, 0, 0);
            final String condition = tempWriter.toString();
            writer.print(MessageFormat.format(rewriteFormat, left, right, condition));
        }
    }
    
    public DremioSqlDialect.ContainerSupport supportsCatalogs() {
        return this.yaml.getSyntax().supportsCatalogs();
    }
    
    public DremioSqlDialect.ContainerSupport supportsSchemas() {
        return this.yaml.getSyntax().supportsSchemas();
    }
    
    public CallTransformer getCallTransformer(final RexCall call) {
        return ArpCallTransformers.getTransformer(call);
    }
    
    public CallTransformer getCallTransformer(final SqlOperator op) {
        return ArpCallTransformers.getTransformer(op);
    }
    
    public boolean hasBooleanLiteralOrRexCallReturnsBoolean(final RexNode node, boolean rexCallCanReturnBoolean) {
        final SqlTypeName nodeDataType = node.getType().getSqlTypeName();
        if (node instanceof RexLiteral) {
            final boolean toReturn = nodeDataType == SqlTypeName.BOOLEAN;
            if (toReturn) {
                ArpDialect.logger.debug("Boolean RexLiteral found, {}", (Object)node);
            }
            return toReturn;
        }
        if (node instanceof RexInputRef) {
            return false;
        }
        if (node instanceof RexCall) {
            final RexCall call = (RexCall)node;
            if (nodeDataType == SqlTypeName.BOOLEAN && (!rexCallCanReturnBoolean || call.getOperator().getKind() == SqlKind.CAST)) {
                ArpDialect.logger.debug("RexCall returns boolean, {}", (Object)node);
                return true;
            }
            List<RexNode> operandsToCheck;
            if (call.getOperator().getKind() == SqlKind.CASE) {
                assert call.getOperands().size() >= 2;
                final List<RexNode> clausesToCheck = (List<RexNode>)call.getOperands().stream().filter(operand -> operand.getType() != node.getType()).collect(Collectors.toList());
                for (final RexNode clause : clausesToCheck) {
                    if (this.hasBooleanLiteralOrInputRef(clause)) {
                        return true;
                    }
                }
                rexCallCanReturnBoolean = this.supportsLiteral(CompleteType.BIT);
                operandsToCheck = (List<RexNode>)call.getOperands().stream().filter(operand -> operand.getType() == node.getType()).collect(Collectors.toList());
            }
            else {
                operandsToCheck = (List<RexNode>)call.getOperands();
            }
            for (final RexNode operand2 : operandsToCheck) {
                if (this.hasBooleanLiteralOrRexCallReturnsBoolean(operand2, rexCallCanReturnBoolean)) {
                    ArpDialect.logger.debug("RexCall has boolean inputs, {}", (Object)node);
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean hasBooleanLiteralOrInputRef(final RexNode node) {
        if (node instanceof RexLiteral || node instanceof RexInputRef) {
            final SqlTypeName nodeDataType = node.getType().getSqlTypeName();
            final boolean toReturn = nodeDataType == SqlTypeName.BOOLEAN;
            if (toReturn) {
                ArpDialect.logger.debug("Boolean {} found, {}", (Object)((node instanceof RexLiteral) ? "RexLiteral" : "RexInputRef"), (Object)node);
            }
            return toReturn;
        }
        if (node instanceof RexCall) {
            for (final RexNode operand : ((RexCall)node).getOperands()) {
                if (this.hasBooleanLiteralOrInputRef(operand)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public SqlNode decorateSqlNode(final RexNode rexNode, final Supplier<SqlNode> defaultNodeSupplier) {
        final SqlNode undecoratedNode = defaultNodeSupplier.get();
        if (!(undecoratedNode instanceof SqlCall) || ArpDialect.MANDATORY_OPERATORS.contains(undecoratedNode.getKind())) {
            return defaultNodeSupplier.get();
        }
        Preconditions.checkArgument(rexNode instanceof RexCall);
        final RexCall rexCall = (RexCall)rexNode;
        final CallTransformer transformer = this.getCallTransformer(rexCall);
        return this.yaml.getSqlNodeForOperator((SqlCall)undecoratedNode, rexCall, transformer);
    }
    
    public SqlNode decorateSqlNode(final AggregateCall aggCall, final Supplier<List<RelDataType>> argTypes, final Supplier<SqlNode> defaultNodeSupplier) {
        return this.yaml.getSqlNodeForOperator((SqlCall)defaultNodeSupplier.get(), aggCall, argTypes.get());
    }
    
    protected final SqlNode emulateNullDirectionWithCaseIsNull(final SqlNode node, final boolean nullsFirst, final boolean desc) {
        if (this.nullCollation.isDefaultOrder(nullsFirst, desc)) {
            return null;
        }
        final SqlNode orderingNode = (SqlNode)new SqlCase(SqlParserPos.ZERO, (SqlNode)null, SqlNodeList.of((SqlNode)SqlStdOperatorTable.IS_NULL.createCall(SqlParserPos.ZERO, new SqlNode[] { node })), SqlNodeList.of((SqlNode)SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)), (SqlNode)SqlNodeList.of((SqlNode)SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)));
        if (!nullsFirst) {
            return orderingNode;
        }
        return (SqlNode)SqlStdOperatorTable.DESC.createCall(SqlParserPos.ZERO, new SqlNode[] { orderingNode });
    }
    
    static {
        MANDATORY_OPERATORS = (Set)new ImmutableSet.Builder().add((Object[])new SqlKind[] { SqlKind.SELECT, SqlKind.AS, SqlKind.CASE }).build();
        logger = LoggerFactory.getLogger((Class)ArpDialect.class);
    }
    
    @VisibleForTesting
    public static class ArpSchemaFetcher extends JdbcSchemaFetcher
    {
        private static final Logger logger;
        private final String query;
        private final boolean usePrepareForColumnMeta;
        private final boolean usePrepareForGetTables;
        
        @VisibleForTesting
        public String getQuery() {
            return this.query;
        }
        
        public ArpSchemaFetcher(final String query, final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config) {
            super(name, dataSource, timeout, config);
            this.query = query;
            this.usePrepareForColumnMeta = false;
            this.usePrepareForGetTables = false;
        }
        
        public ArpSchemaFetcher(final String query, final String name, final DataSource dataSource, final int timeout, final JdbcStoragePlugin.Config config, final boolean usePrepareForColumnMeta, final boolean usePrepareForGetTables) {
            super(name, dataSource, timeout, config);
            this.query = query;
            this.usePrepareForColumnMeta = usePrepareForColumnMeta;
            this.usePrepareForGetTables = usePrepareForGetTables;
        }
        
        @Override
        public DatasetHandleListing getTableHandles() {
            if (this.config.shouldSkipSchemaDiscovery()) {
                ArpSchemaFetcher.logger.debug("Skip schema discovery enabled, skipping getting tables '{}'", (Object)this.storagePluginName);
                return (DatasetHandleListing)new EmptyDatasetHandleListing();
            }
            ArpSchemaFetcher.logger.debug("Getting all tables for plugin '{}'.", (Object)this.storagePluginName);
            try {
                final Connection connection = this.dataSource.getConnection();
                return (DatasetHandleListing)new JdbcIteratorListing((Iterator)new ArpJdbcDatasetMetadataIterable(this.storagePluginName, connection, this.filterQuery(this.query, connection.getMetaData())));
            }
            catch (SQLException e) {
                return (DatasetHandleListing)new EmptyDatasetHandleListing();
            }
        }
        
        @Override
        protected boolean usePrepareForColumnMetadata() {
            return this.usePrepareForColumnMeta;
        }
        
        @Override
        protected boolean usePrepareForGetTables() {
            return this.usePrepareForGetTables;
        }
        
        protected String filterQuery(final String query, final DatabaseMetaData metaData) throws SQLException {
            final StringBuilder filterQuery = new StringBuilder(query);
            if (this.config.getDatabase() != null && this.config.showOnlyConnDatabase()) {
                if (JdbcSchemaFetcher.supportsCatalogs(this.config.getDialect(), metaData)) {
                    filterQuery.append(" AND CAT = ").append(this.config.getDialect().quoteStringLiteral(this.config.getDatabase()));
                }
                else if (JdbcSchemaFetcher.supportsSchemas(this.config.getDialect(), metaData)) {
                    filterQuery.append(" AND SCH = ").append(this.config.getDialect().quoteStringLiteral(this.config.getDatabase()));
                }
            }
            return filterQuery.toString();
        }
        
        static {
            logger = LoggerFactory.getLogger((Class)ArpSchemaFetcher.class);
        }
        
        protected static class ArpJdbcDatasetMetadataIterable extends AbstractIterator<DatasetHandle> implements AutoCloseable
        {
            private final String storagePluginName;
            private final Connection connection;
            private Statement statement;
            private ResultSet tablesResult;
            
            protected ArpJdbcDatasetMetadataIterable(final String storagePluginName, final Connection connection, final String query) {
                this.storagePluginName = storagePluginName;
                this.connection = connection;
                try {
                    this.statement = connection.createStatement();
                    this.tablesResult = this.statement.executeQuery(query);
                }
                catch (SQLException e) {
                    ArpSchemaFetcher.logger.error(String.format("Error retrieving all tables for %s", storagePluginName), (Throwable)e);
                }
            }
            
            public DatasetHandle computeNext() {
                try {
                    if (this.tablesResult == null || !this.tablesResult.next()) {
                        ArpSchemaFetcher.logger.debug("Done fetching all schema and tables for '{}'.", (Object)this.storagePluginName);
                        return (DatasetHandle)this.endOfData();
                    }
                    final List<String> path = new ArrayList<String>(4);
                    path.add(this.storagePluginName);
                    final String currCatalog = this.tablesResult.getString(1);
                    if (!Strings.isNullOrEmpty(currCatalog)) {
                        path.add(currCatalog);
                    }
                    final String currSchema = this.tablesResult.getString(2);
                    if (!Strings.isNullOrEmpty(currSchema)) {
                        path.add(currSchema);
                    }
                    path.add(this.tablesResult.getString(3));
                    return (DatasetHandle)new JdbcDatasetHandle(new EntityPath((List)path));
                }
                catch (SQLException e) {
                    ArpSchemaFetcher.logger.error(String.format("Error listing datasets for '%s'", this.storagePluginName), (Throwable)e);
                    return (DatasetHandle)this.endOfData();
                }
            }
            
            public void close() throws Exception {
                try {
                    AutoCloseables.close(new AutoCloseable[] { this.tablesResult, this.statement, this.connection });
                }
                catch (Exception e) {
                    ArpSchemaFetcher.logger.warn("Error closing connection when listing JDBC datasets.", (Throwable)e);
                }
            }
        }
    }
    
    private static final class ArpCallTransformers
    {
        private static final ImmutableMap<SqlOperator, CallTransformer> transformers;
        
        static CallTransformer getTransformer(final RexCall call) {
            final CallTransformer transformer = (CallTransformer)ArpCallTransformers.transformers.get((Object)call.getOperator());
            if (transformer != null && transformer.matches(call)) {
                return transformer;
            }
            return (CallTransformer)NoOpTransformer.INSTANCE;
        }
        
        static CallTransformer getTransformer(final SqlOperator operator) {
            final CallTransformer transformer = (CallTransformer)ArpCallTransformers.transformers.get((Object)operator);
            if (transformer != null) {
                return transformer;
            }
            return (CallTransformer)NoOpTransformer.INSTANCE;
        }
        
        private static void registerTransformer(final CallTransformer transformer, final ImmutableMap.Builder<SqlOperator, CallTransformer> builder) {
            for (final SqlOperator op : transformer.getCompatibleOperators()) {
                builder.put((Object)op, (Object)transformer);
            }
        }
        
        static {
            final ImmutableMap.Builder<SqlOperator, CallTransformer> builder = (ImmutableMap.Builder<SqlOperator, CallTransformer>)ImmutableMap.builder();
            registerTransformer(TrimTransformer.INSTANCE, builder);
            registerTransformer(TimeUnitFunctionTransformer.INSTANCE, builder);
            transformers = builder.build();
        }
    }
}
