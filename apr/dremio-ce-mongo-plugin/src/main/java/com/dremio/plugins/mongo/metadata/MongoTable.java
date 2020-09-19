package com.dremio.plugins.mongo.metadata;

import com.dremio.plugins.mongo.connection.*;
import com.dremio.exec.server.*;
import com.dremio.exec.record.*;
import com.google.common.base.*;
import com.dremio.connector.*;
import com.dremio.mongo.proto.*;
import com.dremio.exec.planner.cost.*;
import org.apache.arrow.vector.types.pojo.*;
import com.mongodb.client.*;
import org.bson.*;
import java.util.*;
import java.util.Objects;

import com.dremio.plugins.mongo.planning.*;
import com.dremio.exec.*;
import com.dremio.options.*;
import com.dremio.common.expression.*;
import com.google.common.collect.*;
import com.dremio.sabot.exec.context.*;
import com.dremio.exec.physical.base.*;
import com.dremio.sabot.op.scan.*;
import com.dremio.plugins.mongo.execution.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.store.*;
import org.apache.arrow.memory.*;
import com.google.common.annotations.*;
import com.dremio.connector.metadata.*;

public class MongoTable implements DatasetHandle
{
    private final MongoCollection collection;
    private final MongoConnectionManager manager;
    private final EntityPath entityPath;
    private final SabotContext context;
    private final MongoTopology topology;
    private final int subpartitionSize;
    private DatasetMetadata datasetMetadata;
    private List<PartitionChunk> partitionChunks;
    
    public MongoTable(final SabotContext context, final int subpartitionSize, final EntityPath entityPath, final MongoCollection collection, final MongoConnectionManager manager, final MongoTopology topology) {
        this.context = context;
        this.entityPath = entityPath;
        this.subpartitionSize = subpartitionSize;
        this.collection = collection;
        this.manager = manager;
        this.topology = topology;
    }
    
    private void buildIfNecessary(final BatchSchema oldSchema) throws ConnectorException {
        if (this.partitionChunks != null) {
            return;
        }
        final MongoDatabase db = this.manager.getMetadataClient().getDatabase(this.collection.getDatabase());
        final com.mongodb.client.MongoCollection<Document> docs = (com.mongodb.client.MongoCollection<Document>)db.getCollection(this.collection.getCollection());
        final long count = docs.count();
        BatchSchema schema;
        try {
            schema = this.getSampledSchema(oldSchema);
        }
        catch (Exception e) {
            //Throwables.throwIfUnchecked(e);
            throw new ConnectorException(e);
        }
        final MongoChunks chunks = new MongoChunks(this.collection, this.manager.getFirstConnection(), this.topology, this.subpartitionSize, this.entityPath.getComponents().get(0));
        this.partitionChunks = new ArrayList<PartitionChunk>();
        for (final MongoChunk chunk : chunks) {
            this.partitionChunks.add(chunk.toSplit());
        }
        final MongoReaderProto.MongoTableXattr extended = MongoReaderProto.MongoTableXattr.newBuilder().setDatabase(this.collection.getDatabase()).setCollection(this.collection.getCollection()).setType(chunks.getCollectionType()).build();
        final DatasetStats of = DatasetStats.of(count, ScanCostFactor.MONGO.getFactor());
        final BatchSchema batchSchema = schema;
        final MongoReaderProto.MongoTableXattr mongoTableXattr = extended;
        Objects.requireNonNull(mongoTableXattr);
        this.datasetMetadata = DatasetMetadata.of(of, (Schema)batchSchema, mongoTableXattr::writeTo);
    }
    
    private BatchSchema getSampledSchema(final BatchSchema oldSchema) throws Exception {
        final MongoSubScanSpec spec = new MongoSubScanSpec(this.collection.getDatabase(), this.collection.getCollection(), null, null, null, MongoPipeline.createMongoPipeline(null, false));
        final int fetchSize = (int)this.context.getOptionManager().getOption((TypeValidators.LongValidator)ExecConstants.TARGET_BATCH_RECORDS_MAX);
        final ImmutableList<SchemaPath> columns = (ImmutableList<SchemaPath>)ImmutableList.of(SchemaPath.getSimplePath("*"));
        final BufferAllocator sampleAllocator = this.context.getAllocator().newChildAllocator("mongo-sample-alloc", 0L, Long.MAX_VALUE);
        Throwable x0 = null;
        try {
            final OperatorContextImpl operatorContext = new OperatorContextImpl(this.context.getConfig(), sampleAllocator, this.context.getOptionManager(), fetchSize);
            Throwable x2 = null;
            try {
                final MongoRecordReader reader = new MongoRecordReader(this.manager, (OperatorContext)operatorContext, spec, (List<SchemaPath>)columns, (List<SchemaPath>)columns, operatorContext.getManagedBuffer(), true, fetchSize, oldSchema);
                Throwable x3 = null;
                try {
                    SampleMutator mutator = new SampleMutator(sampleAllocator);
                    try {
                        if (oldSchema != null) {
                            oldSchema.materializeVectors(GroupScan.ALL_COLUMNS, (OutputMutator)mutator);
                        }
                        while (true) {
                            try {
                                reader.setup((OutputMutator)mutator);
                                reader.next();
                            }
                            catch (BsonRecordReader.ChangedScaleException sce) {
                                mutator.close();
                                mutator = new SampleMutator(sampleAllocator);
                                continue;
                            }
                            break;
                        }
                        mutator.getContainer().buildSchema(BatchSchema.SelectionVectorMode.NONE);
                        final BatchSchema batchSchema = mutator.getContainer().getSchema();
                        if (batchSchema.getFieldCount() == 0) {
                            throw StoragePluginUtils.message(UserException.dataReadError(), (String)this.entityPath.getComponents().get(0), "The table %s has no rows or columns", new Object[] { this.entityPath.getName() }).build();
                        }
                        return batchSchema;
                    }
                    finally {
                        mutator.close();
                    }
                }
                catch (Throwable t) {
                    x3 = t;
                    throw t;
                }
                finally {
                    $closeResource(x3, (AutoCloseable)reader);
                }
            }
            catch (Throwable t2) {
                x2 = t2;
                throw t2;
            }
            finally {
                $closeResource(x2, (AutoCloseable)operatorContext);
            }
        }
        catch (Throwable t3) {
            x0 = t3;
            throw t3;
        }
        finally {
            if (sampleAllocator != null) {
                $closeResource(x0, (AutoCloseable)sampleAllocator);
            }
        }
    }
    
    @VisibleForTesting
    public int getSubpartitionSize() {
        return this.subpartitionSize;
    }
    
    public EntityPath getDatasetPath() {
        return this.entityPath;
    }
    
    public DatasetMetadata getDatasetMetadata(final BatchSchema oldSchema) throws ConnectorException {
        this.buildIfNecessary(oldSchema);
        return this.datasetMetadata;
    }
    
    public PartitionChunkListing listPartitionChunks(final BatchSchema oldSchema) throws ConnectorException {
        this.buildIfNecessary(oldSchema);
        return () -> this.partitionChunks.iterator();
    }
    
    private static /* synthetic */ void $closeResource(final Throwable x0, final AutoCloseable x1) {
        if (x0 != null) {
            try {
                x1.close();
            }
            catch (Throwable t) {
                x0.addSuppressed(t);
            }
        }
        else {
            try {
				x1.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
}
