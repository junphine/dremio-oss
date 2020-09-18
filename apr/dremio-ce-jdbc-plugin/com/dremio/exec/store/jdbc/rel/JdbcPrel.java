package com.dremio.exec.store.jdbc.rel;

import com.dremio.options.*;
import com.dremio.exec.catalog.*;
import com.dremio.common.expression.*;
import com.dremio.exec.record.*;
import com.dremio.exec.store.jdbc.proto.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.exec.store.jdbc.conf.*;
import org.apache.calcite.sql.pretty.*;
import org.apache.calcite.sql.*;
import com.google.common.base.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.store.jdbc.rel2sql.*;
import com.dremio.common.rel2sql.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.physical.base.*;
import com.dremio.exec.store.jdbc.*;
import java.io.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.metadata.*;
import com.dremio.exec.planner.physical.visitor.*;
import com.dremio.exec.planner.common.*;
import com.dremio.exec.planner.sql.*;
import org.slf4j.*;
import com.dremio.exec.planner.*;
import com.google.common.annotations.*;
import com.google.common.collect.*;
import org.apache.calcite.rel.core.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;
import org.apache.calcite.rex.*;
import com.google.protobuf.*;
import com.dremio.service.namespace.dataset.proto.*;

@Options
public class JdbcPrel extends JdbcRelBase implements Prel
{
    private static final Logger logger;
    public static final TypeValidators.LongValidator RESERVE;
    public static final TypeValidators.LongValidator LIMIT;
    private final String sql;
    private final double rows;
    private final StoragePluginId pluginId;
    private final List<SchemaPath> columns;
    private final BatchSchema schema;
    private final Set<List<String>> tableList;
    private final Set<String> skippedColumns;
    private final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties;
    
    public JdbcPrel(final RelOptCluster cluster, final RelTraitSet traitSet, final JdbcIntermediatePrel prel, final FunctionLookupContext context, final StoragePluginId pluginId) {
        super(cluster, traitSet, prel.getSubTree());
        this.pluginId = pluginId;
        this.rows = cluster.getMetadataQuery().getRowCount(this.jdbcSubTree);
        RelNode jdbcInput = rewriteJdbcSubtree(this.jdbcSubTree, PrelUtil.getPlannerSettings(cluster).getOptions().getOption(PlannerSettings.JDBC_PUSH_DOWN_PLUS));
        final TableInfoAccumulator tableListGenerator = new TableInfoAccumulator();
        jdbcInput = jdbcInput.accept((RelShuttle)tableListGenerator);
        this.tableList = (Set<List<String>>)ImmutableSet.copyOf((Collection)tableListGenerator.getTableList());
        this.skippedColumns = (Set<String>)ImmutableSet.copyOf((Collection)tableListGenerator.getSkippedColumns());
        this.columnProperties = (Map<String, List<JdbcReaderProto.ColumnProperty>>)ImmutableMap.copyOf((Map)tableListGenerator.getColumnProperties());
        this.rowType = jdbcInput.getRowType();
        final DialectConf<?, ?> conf = (DialectConf<?, ?>)pluginId.getConnectionConf();
        final JdbcDremioSqlDialect dialect = conf.getDialect();
        final JdbcDremioRelToSqlConverter jdbcImplementor = dialect.getConverter();
        final Map<String, Map<String, String>> colProperties = new HashMap<String, Map<String, String>>();
        for (final String key : this.columnProperties.keySet()) {
            final Map<String, String> properties = new HashMap<String, String>();
            for (final JdbcReaderProto.ColumnProperty prop : this.columnProperties.get(key)) {
                properties.put(prop.getKey(), prop.getValue());
            }
            colProperties.put(key, properties);
        }
        jdbcImplementor.setColumnProperties(colProperties);
        final SqlImplementor.Result result = jdbcImplementor.visitChild(0, jdbcInput);
        final SqlPrettyWriter writer = new SqlPrettyWriter((SqlDialect)dialect);
        writer.setAlwaysUseParentheses(false);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        result.asQueryOrValues().unparse((SqlWriter)writer, 0, 0);
        this.sql = writer.toString();
        Preconditions.checkState(this.sql != null && !this.sql.isEmpty(), (Object)"JDBC pushdown sql string cannot be empty");
        this.columns = new ArrayList<SchemaPath>();
        for (final String colNames : this.rowType.getFieldNames()) {
            this.columns.add(SchemaPath.getSimplePath(colNames));
        }
        this.schema = this.getSchema(jdbcInput, context);
    }
    
    public static RelNode rewriteJdbcSubtree(final RelNode root, final boolean addIdentityProjects) {
        final RelNode rewrite1 = root.accept((RelShuttle)new MoreRelOptUtil.SubsetRemover());
        final RelNode rewrite2 = rewrite1.accept((RelShuttle)new MoreRelOptUtil.OrderByInSubQueryRemover(rewrite1));
        return addIdentityProjects ? rewrite2.accept((RelShuttle)new AddIdentityProjectsOnJoins()) : rewrite2;
    }
    
    public PhysicalOperator getPhysicalOperator(final PhysicalPlanCreator creator) throws IOException {
        return (PhysicalOperator)new JdbcGroupScan(creator.props((Prel)this, "$dremio$", this.schema, JdbcPrel.RESERVE, JdbcPrel.LIMIT), this.sql, this.columns, this.pluginId, this.schema, this.tableList, this.skippedColumns, this.columnProperties);
    }
    
    public RelWriter explainTerms(final RelWriter pw) {
        return super.explainTerms(pw).item("sql", (Object)this.sql);
    }
    
    public double estimateRowCount(final RelMetadataQuery mq) {
        return this.rows;
    }
    
    public Iterator<Prel> iterator() {
        return Collections.emptyIterator();
    }
    
    public <T, X, E extends Throwable> T accept(final PrelVisitor<T, X, E> logicalVisitor, final X value) throws E, Throwable {
        return (T)logicalVisitor.visitPrel((Prel)this, (Object)value);
    }
    
    public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
        return BatchSchema.SelectionVectorMode.DEFAULT;
    }
    
    public BatchSchema.SelectionVectorMode getEncoding() {
        return BatchSchema.SelectionVectorMode.NONE;
    }
    
    public boolean needsFinalColumnReordering() {
        return false;
    }
    
    private BatchSchema getSchema(final RelNode node, final FunctionLookupContext context) {
        if (node == null) {
            return null;
        }
        assert node instanceof JdbcRelImpl : "Found non-JdbcRelImpl in a jdbc subtree, " + node;
        return CalciteArrowHelper.fromCalciteRowType(node.getRowType());
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcPrel.class);
        RESERVE = (TypeValidators.LongValidator)new TypeValidators.PositiveLongValidator("planner.op.jdbc.reserve_bytes", Long.MAX_VALUE, 1000000L);
        LIMIT = (TypeValidators.LongValidator)new TypeValidators.PositiveLongValidator("planner.op.jdbc.limit_bytes", Long.MAX_VALUE, Long.MAX_VALUE);
    }
    
    @VisibleForTesting
    public static class TableInfoAccumulator extends StatelessRelShuttleImpl
    {
        private final Set<List<String>> tableList;
        private final Set<String> skippedColumns;
        private final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties;
        
        public TableInfoAccumulator() {
            this.tableList = (Set<List<String>>)Sets.newHashSet();
            this.skippedColumns = (Set<String>)Sets.newHashSet();
            this.columnProperties = new HashMap<String, List<JdbcReaderProto.ColumnProperty>>();
        }
        
        public RelNode visit(final TableScan scan) {
            if (scan instanceof JdbcTableScan) {
                final JdbcTableScan tableScan = (JdbcTableScan)scan;
                this.tableList.add(scan.getTable().getQualifiedName());
                final ReadDefinition readDefinition = tableScan.getTableMetadata().getReadDefinition();
                if (readDefinition.getExtendedProperty() != null) {
                    try {
                        final JdbcReaderProto.JdbcTableXattr attrs = JdbcReaderProto.JdbcTableXattr.parseFrom(readDefinition.getExtendedProperty().asReadOnlyByteBuffer());
                        this.skippedColumns.addAll((Collection<? extends String>)attrs.getSkippedColumnsList());
                        for (final JdbcReaderProto.ColumnProperties colProp : attrs.getColumnPropertiesList()) {
                            this.columnProperties.put(colProp.getColumnName(), colProp.getPropertiesList());
                        }
                        final Set<String> projected = new HashSet<String>(tableScan.getRowType().getFieldNames());
                        final Set<String> skipped = new HashSet<String>((Collection<? extends String>)attrs.getSkippedColumnsList());
                        if (projected.size() == 0) {
                            final List<SchemaPath> columns = (List<SchemaPath>)tableScan.getTable().getRowType().getFieldNames().stream().filter(field -> !skipped.contains(field)).map(SchemaPath::getSimplePath).collect(Collectors.toList());
                            final JdbcTableScan newTableScan = (JdbcTableScan)tableScan.cloneWithProject(columns);
                            return (RelNode)new JdbcProject(newTableScan.getCluster(), newTableScan.getTraitSet(), (RelNode)newTableScan, newTableScan.getCluster().getRexBuilder().identityProjects(newTableScan.getRowType()), newTableScan.getRowType(), newTableScan.getPluginId(), true);
                        }
                        final Set set;
                        final Set set2;
                        if (tableScan.getTable().getRowType().getFieldNames().stream().anyMatch(field -> !set.contains(field) && !set2.contains(field))) {
                            return (RelNode)new JdbcProject(tableScan.getCluster(), tableScan.getTraitSet(), (RelNode)tableScan, tableScan.getCluster().getRexBuilder().identityProjects(tableScan.getRowType()), tableScan.getRowType(), tableScan.getPluginId());
                        }
                    }
                    catch (InvalidProtocolBufferException ex) {
                        JdbcPrel.logger.warn("Unable to get extended properties for table {}.", (Object)tableScan.getTableName(), (Object)ex);
                    }
                }
            }
            return super.visit(scan);
        }
        
        Set<List<String>> getTableList() {
            return this.tableList;
        }
        
        Set<String> getSkippedColumns() {
            return this.skippedColumns;
        }
        
        Map<String, List<JdbcReaderProto.ColumnProperty>> getColumnProperties() {
            return this.columnProperties;
        }
    }
    
    private static class AddIdentityProjectsOnJoins extends StatelessRelShuttleImpl
    {
        protected RelNode visitChild(final RelNode parent, final int i, final RelNode child) {
            RelNode child2 = child.accept((RelShuttle)this);
            if (child2 instanceof JdbcJoin && !(parent instanceof JdbcProject)) {
                final JdbcJoin join = (JdbcJoin)child2;
                child2 = (RelNode)new JdbcProject(join.getCluster(), join.getTraitSet(), (RelNode)join, join.getCluster().getRexBuilder().identityProjects(join.getRowType()), join.getRowType(), join.getPluginId());
            }
            if (child2 != child) {
                final List<RelNode> newInputs = new ArrayList<RelNode>(parent.getInputs());
                newInputs.set(i, child2);
                return parent.copy(parent.getTraitSet(), (List)newInputs);
            }
            return parent;
        }
    }
}
