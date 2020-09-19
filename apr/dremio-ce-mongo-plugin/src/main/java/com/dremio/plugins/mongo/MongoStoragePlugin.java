package com.dremio.plugins.mongo;

import com.dremio.connector.metadata.extensions.*;
import com.dremio.plugins.mongo.connection.*;
import com.dremio.exec.server.*;
import com.dremio.service.namespace.dataset.proto.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.record.*;
import com.dremio.connector.*;
import com.dremio.connector.metadata.*;
import java.util.stream.*;
import com.dremio.plugins.mongo.metadata.*;
import com.google.common.annotations.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.dremio.service.namespace.capabilities.*;

import com.dremio.exec.planner.sql.*;
import io.protostuff.*;
import com.dremio.plugins.mongo.planning.*;
import com.dremio.service.namespace.*;
import com.dremio.exec.store.*;
import com.dremio.exec.planner.logical.*;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.*;
import java.util.*;


public class MongoStoragePlugin implements StoragePlugin, SupportsListingDatasets
{
    public static final BooleanCapability MONGO_3_2_FEATURES;
    public static final BooleanCapability MONGO_3_4_FEATURES;
    public static final BooleanCapability MONGO_3_6_FEATURES;
    private final MongoConnectionManager manager;
    private final SabotContext context;
    private final String name;
    private final MongoConf config;
    private MongoVersion version;
    private boolean supportsMongo3_4;
    private boolean supportsMongo3_6;
    
    public MongoStoragePlugin(final MongoConf config, final SabotContext context, final String name) {
        this.version = MongoVersion.MIN_MONGO_VERSION;
        this.supportsMongo3_4 = false;
        this.supportsMongo3_6 = false;
        this.context = context;
        this.config = config;
        this.name = name;
        this.manager = new MongoConnectionManager(config, name);
    }
    
    public boolean hasAccessPermission(final String user, final NamespaceKey key, final DatasetConfig config) {
        return true;
    }
    
    public DatasetHandleListing listDatasetHandles(final GetDatasetOption... options) {
        final MongoCollections collections = this.manager.getMetadataClient().getCollections();
        final MongoTopology topology = this.manager.getTopology(collections.canDBStats(), collections.getAuthenticationDB());
        return () -> {           
            return StreamSupport.stream(collections.spliterator(), false).map(input -> {
            	EntityPath entityPath = new EntityPath(ImmutableList.of(this.name, input.getDatabase(), input.getCollection()));
                return (DatasetHandle)this.getDatasetInternal(entityPath, topology).get();
            }).iterator();
        };
    }
    
    public Optional<DatasetHandle> getDatasetHandle(final EntityPath datasetPath, final GetDatasetOption... options) {
        final List<String> components = (List<String>)datasetPath.getComponents();
        if (components.size() != 3) {
            return Optional.empty();
        }
        final MongoCollections collections = this.manager.getMetadataClient().getCollections();
        final MongoTopology topology = this.manager.getTopology(collections.canDBStats(), collections.getAuthenticationDB());
        return this.getDatasetInternal(datasetPath, topology);
    }
    
    public DatasetMetadata getDatasetMetadata(final DatasetHandle datasetHandle, final PartitionChunkListing chunkListing, final GetMetadataOption... options) throws ConnectorException {
        final BatchSchema oldSchema = CurrentSchemaOption.getSchema((MetadataOption[])options);
        return ((MongoTable)datasetHandle.unwrap(MongoTable.class)).getDatasetMetadata(oldSchema);
    }
    
    public PartitionChunkListing listPartitionChunks(final DatasetHandle datasetHandle, final ListPartitionChunkOption... options) throws ConnectorException {
        final BatchSchema oldSchema = CurrentSchemaOption.getSchema((MetadataOption[])options);
        return ((MongoTable)datasetHandle.unwrap(MongoTable.class)).listPartitionChunks(oldSchema);
    }
    
    public boolean containerExists(final EntityPath key) {
        final List<String> components = (List<String>)key.getComponents();
        if (components.size() != 2) {
            return false;
        }
        final String database = components.get(1);
        return StreamSupport.stream(this.manager.getMetadataClient().getCollections().spliterator(), false).anyMatch(input -> input.getDatabase().equalsIgnoreCase(database));
    }
    
    public void start() {
        this.version = this.manager.connect();
        this.supportsMongo3_4 = ((this.version.getMajor() > 3 || (this.version.getMajor() == 3 && this.version.getMinor() >= 4)) && (this.version.getCompatibilityMajor() > 3 || (this.version.getCompatibilityMajor() == 3 && this.version.getCompatibilityMinor() >= 4)));
        this.supportsMongo3_6 = ((this.version.getMajor() > 3 || (this.version.getMajor() == 3 && this.version.getMinor() >= 6)) && (this.version.getCompatibilityMajor() > 3 || (this.version.getCompatibilityMajor() == 3 && this.version.getCompatibilityMinor() >= 6)));
    }
    
    @VisibleForTesting
    Optional<DatasetHandle> getDatasetInternal(final EntityPath entityPath, final MongoTopology topology) {
        final List<String> components = (List<String>)entityPath.getComponents();
        if (components.size() != 3) {
            return Optional.empty();
        }
        final MongoCollection desiredCollection = new MongoCollection(components.get(1), components.get(2));
        return Optional.of((DatasetHandle)new MongoTable(this.context, this.config.subpartitionSize, entityPath, desiredCollection, this.manager, topology));
    }
    
    public SourceCapabilities getSourceCapabilities() {
        return new SourceCapabilities(new CapabilityValue[] { new BooleanCapabilityValue(MongoStoragePlugin.MONGO_3_2_FEATURES, this.version.enableNewFeatures()), new BooleanCapabilityValue(MongoStoragePlugin.MONGO_3_4_FEATURES, this.supportsMongo3_4), new BooleanCapabilityValue(MongoStoragePlugin.MONGO_3_6_FEATURES, this.supportsMongo3_6) });
    }
    
    public DatasetConfig createDatasetConfigFromSchema(final DatasetConfig oldConfig, final BatchSchema newSchema) {
        Preconditions.checkNotNull(oldConfig);
        Preconditions.checkNotNull(newSchema);
        BatchSchema merge;
        if (DatasetHelper.getSchemaBytes(oldConfig) == null) {
            merge = newSchema;
        }
        else {
            final List<Field> oldFields = new ArrayList<Field>();
            CalciteArrowHelper.fromDataset(oldConfig).forEach(f -> oldFields.add(this.updateFieldTypeToTimestamp(f)));
            final List<Field> newFields = new ArrayList<Field>();
            newSchema.forEach(f -> newFields.add(this.updateFieldTypeToTimestamp(f)));
            merge = new BatchSchema(oldFields).merge(new BatchSchema(newFields));
        }
        final DatasetConfig newConfig = (DatasetConfig)MongoStoragePlugin.DATASET_CONFIG_SERIALIZER.deserialize(MongoStoragePlugin.DATASET_CONFIG_SERIALIZER.serialize(oldConfig));
        newConfig.setRecordSchema(ByteString.copyFrom(merge.serialize()));
        return newConfig;
    }
    
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return (Class<? extends StoragePluginRulesFactory>)MongoRulesFactory.class;
    }
    
    public SourceState getState() {
        try {
            final MongoVersion probeVersion = this.manager.validateConnection();
            if (probeVersion.enableNewFeatures() != this.version.enableNewFeatures()) {
                return SourceState.badState(new String[] { "Mongo software version was modified at level that Dremio needs restart (one or more nodes moved across the 3.2.0 version horizon)." });
            }
            if (probeVersion.enableNewFeatures()) {
                return new SourceState(SourceState.SourceStatus.good, Collections.singletonList(probeVersion.getVersionInfo()));
            }
            return new SourceState(SourceState.SourceStatus.warn, Collections.singletonList(probeVersion.getVersionWarning()));
        }
        catch (Exception e) {
            return SourceState.badState(e);
        }
    }
    
    public MongoConnectionManager getManager() {
        return this.manager;
    }
    
    public ViewTable getView(final List<String> arg0, final SchemaConfig arg1) {
        return null;
    }
    
    public void close() throws Exception {
        this.manager.close();
    }
    
    private Field updateFieldTypeToTimestamp(final Field field) {
        final List<Field> children = new ArrayList<Field>();
        FieldType type;
        if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Union && field.getChildren().size() == 2 && field.getChildren().get(0).getType().getTypeID() == ArrowType.ArrowTypeID.Date && field.getChildren().get(1).getType().getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
            type = new FieldType(field.isNullable(), (ArrowType)new ArrowType.Timestamp(TimeUnit.MILLISECOND, (String)null), field.getDictionary(), field.getMetadata());
        }
        else {
            for (final Field child : field.getChildren()) {
                children.add(this.updateFieldTypeToTimestamp(child));
            }
            if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Date) {
                type = new FieldType(field.isNullable(), (ArrowType)new ArrowType.Timestamp(TimeUnit.MILLISECOND, (String)null), field.getDictionary(), field.getMetadata());
            }
            else {
                type = field.getFieldType();
            }
        }
        return new Field(field.getName(), type, children);
    }
    
    static {
        MONGO_3_2_FEATURES = new BooleanCapability("support_mongo_3_2_features", false);
        MONGO_3_4_FEATURES = new BooleanCapability("support_mongo_3_4_features", false);
        MONGO_3_6_FEATURES = new BooleanCapability("support_mongo_3_6_features", false);
    }
}
