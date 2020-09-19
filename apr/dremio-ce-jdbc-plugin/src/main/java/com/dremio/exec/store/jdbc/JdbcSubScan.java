package com.dremio.exec.store.jdbc;

import com.dremio.common.expression.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.physical.base.*;
import com.fasterxml.jackson.annotation.*;
import com.dremio.exec.record.*;
import java.util.*;
import com.google.common.base.*;
import com.dremio.common.exceptions.*;

@JsonTypeName("jdbc-sub-scan")
public class JdbcSubScan extends SubScanWithProjection
{
    private final String sql;
    private final List<SchemaPath> columns;
    private final StoragePluginId pluginId;
    private final Set<String> skippedColumns;
    
    public JdbcSubScan(@JsonProperty("props") final OpProps props, @JsonProperty("sql") final String sql, @JsonProperty("columns") final List<SchemaPath> columns, @JsonProperty("pluginId") final StoragePluginId pluginId, @JsonProperty("fullSchema") final BatchSchema fullSchema, @JsonProperty("referenced-tables") final Collection<List<String>> tableList, @JsonProperty("skipped-columns") final Set<String> skippedColumns) throws ExecutionSetupException {
        super(props, fullSchema, (Collection)tableList, (List)columns);
        Preconditions.checkArgument(sql != null && !sql.isEmpty(), "JDBC pushdown SQL string cannot be empty in JdbcSubScan");
        this.sql = sql;
        this.columns = columns;
        this.pluginId = pluginId;
        this.skippedColumns = skippedColumns;
    }
    
    public int getOperatorType() {
        return 47;
    }
    
    public String getSql() {
        return this.sql;
    }
    
    public List<SchemaPath> getColumns() {
        return this.columns;
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public Set<String> getSkippedColumns() {
        return this.skippedColumns;
    }
}
