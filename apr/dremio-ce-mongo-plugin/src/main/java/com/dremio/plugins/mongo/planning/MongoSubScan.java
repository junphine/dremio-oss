package com.dremio.plugins.mongo.planning;

import com.dremio.exec.catalog.*;
import com.dremio.common.expression.*;
import com.dremio.exec.record.*;
import com.google.common.base.*;
import com.dremio.common.exceptions.*;
import com.fasterxml.jackson.annotation.*;
import com.dremio.exec.physical.base.*;
import java.util.*;
import com.google.common.collect.*;
import com.dremio.exec.planner.fragment.*;
import org.slf4j.*;

@JsonTypeName("mongo-shard-read")
public class MongoSubScan extends SubScanWithProjection
{
    static final Logger logger;
    private static final String CHUNKS_ATTRIBUTE_KEY = "mongo-shard-read-chunks";
    private final StoragePluginId pluginId;
    private final List<SchemaPath> columns;
    private final List<SchemaPath> sanitizedColumns;
    private final boolean singleFragment;
    @JsonIgnore
    private List<MongoSubScanSpec> chunkScanSpecList;
    
    public MongoSubScan(final OpProps props, final StoragePluginId pluginId, final List<MongoSubScanSpec> chunkScanSpecList, final List<SchemaPath> columns, final List<SchemaPath> sanitizedColumns, final boolean singleFragment, final List<String> tableSchemaPath, final BatchSchema fullSchema) throws ExecutionSetupException {
        super(props, fullSchema, tableSchemaPath, columns);
        this.pluginId = pluginId;
        this.columns = columns;
        this.sanitizedColumns = sanitizedColumns;
        this.chunkScanSpecList = chunkScanSpecList;
        this.singleFragment = singleFragment;
        if (chunkScanSpecList != null) {
            Preconditions.checkArgument(!singleFragment || chunkScanSpecList.size() == 1);
        }
    }
    
    public MongoSubScan(@JsonProperty("props") final OpProps props, @JsonProperty("pluginId") final StoragePluginId pluginId, @JsonProperty("columns") final List<SchemaPath> columns, @JsonProperty("sanitizedColumns") final List<SchemaPath> sanitizedColumns, @JsonProperty("singleFragment") final boolean singleFragment, @JsonProperty("tableSchemaPath") final List<String> tableSchemaPath, @JsonProperty("fullSchema") final BatchSchema fullSchema) throws ExecutionSetupException {
        this(props, pluginId, null, columns, sanitizedColumns, singleFragment, tableSchemaPath, fullSchema);
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public <T, X, E extends Throwable> T accept(final PhysicalVisitor<T, X, E> physicalVisitor, final X value) throws E {
        return (T)physicalVisitor.visitSubScan((SubScan)this, value);
    }
    
    public boolean isSingleFragment() {
        return this.singleFragment;
    }
    
    public List<SchemaPath> getColumns() {
        return this.columns;
    }
    
    public List<SchemaPath> getSanitizedColumns() {
        return this.sanitizedColumns;
    }
    
    public List<MongoSubScanSpec> getChunkScanSpecList() {
        return this.chunkScanSpecList;
    }
    
    public PhysicalOperator getNewWithChildren(final List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return (PhysicalOperator)new MongoSubScan(this.props, this.pluginId, this.chunkScanSpecList, this.columns, this.sanitizedColumns, this.singleFragment, (List<String>)Iterables.getOnlyElement((Iterable)this.getReferencedTables()), this.getFullSchema());
    }
    
    public int getOperatorType() {
        return 37;
    }
    
    public Iterator<PhysicalOperator> iterator() {
        return ImmutableList.<PhysicalOperator>of().iterator();
    }
    
    public void collectMinorSpecificAttrs(final MinorDataWriter writer) throws Exception {
        writer.writeJsonEntry(this.getProps(), "mongo-shard-read-chunks", new MongoSubScanSpecList(this.chunkScanSpecList));
    }
    
    public void populateMinorSpecificAttrs(final MinorDataReader reader) throws Exception {
        final MongoSubScanSpecList list = (MongoSubScanSpecList)reader.readJsonEntry(this.getProps(), "mongo-shard-read-chunks", MongoSubScanSpecList.class);
        this.chunkScanSpecList = list.getSpecs();
        Preconditions.checkArgument(!this.singleFragment || this.chunkScanSpecList.size() == 1);
    }
    
    static {
        logger = LoggerFactory.getLogger(MongoSubScan.class);
    }
}
