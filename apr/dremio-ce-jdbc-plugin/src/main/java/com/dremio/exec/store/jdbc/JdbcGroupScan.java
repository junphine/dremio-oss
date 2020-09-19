package com.dremio.exec.store.jdbc;

import com.dremio.common.expression.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.record.*;
import com.dremio.exec.store.jdbc.proto.*;
import com.dremio.common.store.*;
import com.fasterxml.jackson.annotation.*;
import com.dremio.exec.physical.*;
import com.dremio.exec.store.schedule.*;
import com.google.common.collect.*;
import java.util.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.planner.fragment.*;
import com.google.common.base.*;
import com.google.common.base.Objects;
import com.dremio.exec.physical.base.*;

@JsonTypeName("jdbc-scan")
public class JdbcGroupScan extends AbstractBase implements GroupScan<CompleteWork>
{
    private final String sql;
    private final List<SchemaPath> columns;
    private final StoragePluginId pluginId;
    private final BatchSchema schema;
    private final Set<List<String>> tableList;
    private final Set<String> skippedColumns;
    private final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties;
    
    public JdbcGroupScan(@JsonProperty("opprops") final OpProps props, @JsonProperty("sql") final String sql, @JsonProperty("columns") final List<SchemaPath> columns, @JsonProperty("config") final StoragePluginConfig config, @JsonProperty("pluginId") final StoragePluginId pluginId, @JsonProperty("fullSchema") final BatchSchema fullSchema, @JsonProperty("tableList") final Set<List<String>> tableList, @JsonProperty("skipped-columns") final Set<String> skippedColumns, @JsonProperty("column-properties") final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties) {
        this(props, sql, columns, pluginId, fullSchema, tableList, skippedColumns, columnProperties);
    }
    
    public JdbcGroupScan(final OpProps props, final String sql, final List<SchemaPath> columns, final StoragePluginId pluginId, final BatchSchema schema, final Set<List<String>> tableList, final Set<String> skippedColumns, final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties) {
        super(props);
        this.sql = sql;
        this.columns = columns;
        this.pluginId = pluginId;
        this.schema = schema;
        this.tableList = tableList;
        this.skippedColumns = skippedColumns;
        this.columnProperties = columnProperties;
    }
    
    public JdbcGroupScan(final OpProps props, final String sql, final List<SchemaPath> columns, final StoragePluginId pluginId, final BatchSchema schema, final Set<String> skippedColumns) {
        this(props, sql, columns, pluginId, schema, Collections.emptySet(), skippedColumns, Collections.emptyMap());
    }
    
    public Set<List<String>> getReferencedTables() {
        return this.tableList;
    }
    
    @JsonIgnore
    public int getMaxParallelizationWidth() {
        return 1;
    }
    
    public Iterator<PhysicalOperator> iterator() {
        return Collections.emptyIterator();
    }
    
    public <T, X, E extends Throwable> T accept(final PhysicalVisitor<T, X, E> physicalVisitor, final X value) throws E {
        return (T)physicalVisitor.visitGroupScan((GroupScan)this, value);
    }
    
    public List<SchemaPath> getColumns() {
        return this.columns;
    }
    
    public Iterator<CompleteWork> getSplits(final ExecutionNodeMap nodeMap) {
        return Iterators.singletonIterator(new SimpleCompleteWork(1L, new EndpointAffinity[0]));
    }
    
    @JsonIgnore
    public int getMinParallelizationWidth() {
        return 1;
    }
    
    public PhysicalOperator getNewWithChildren(final List<PhysicalOperator> children) {
        Preconditions.checkArgument(children == null || children.isEmpty());
        return (PhysicalOperator)new JdbcGroupScan(this.props, this.sql, this.columns, this.pluginId, this.props.getSchema(), this.tableList, this.skippedColumns, this.columnProperties);
    }
    
    public String getSql() {
        return this.sql;
    }
    
    public JdbcSubScan getSpecificScan(final List<CompleteWork> work) throws ExecutionSetupException {
        return new JdbcSubScan(this.props, this.sql, this.getColumns(), this.pluginId, this.schema, this.getReferencedTables(), this.skippedColumns);
    }
    
    public int getOperatorType() {
        return 47;
    }
    
    public DistributionAffinity getDistributionAffinity() {
        return DistributionAffinity.NONE;
    }
    
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final JdbcGroupScan that = (JdbcGroupScan)o;
        return Objects.equal(this.sql, that.sql) && Objects.equal(this.columns, that.columns) && Objects.equal(this.pluginId.getConfig(), that.pluginId.getConfig()) && Objects.equal(this.schema, that.schema) && Objects.equal(this.tableList, that.tableList) && Objects.equal(this.skippedColumns, that.skippedColumns) && Objects.equal(this.columnProperties, that.columnProperties);
    }
    
    public int hashCode() {
        return Objects.hashCode(new Object[] { this.sql, (this.columns == null) ? 0 : this.columns.size(), this.pluginId.getConfig(), this.schema, this.tableList, this.skippedColumns, this.columnProperties });
    }
}
