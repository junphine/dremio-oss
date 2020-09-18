package com.dremio.plugins.mongo.execution;

import com.dremio.exec.store.*;
import com.dremio.common.expression.*;
import com.dremio.exec.vector.complex.fn.*;
import org.apache.arrow.vector.complex.impl.*;
import org.apache.arrow.memory.*;
import com.dremio.plugins.mongo.planning.*;
import com.dremio.exec.record.*;
import org.apache.arrow.vector.types.pojo.*;
import java.util.*;
import com.google.common.collect.*;
import com.mongodb.*;
import com.dremio.plugins.mongo.connection.*;
import com.mongodb.client.*;
import com.dremio.sabot.op.scan.*;
import com.dremio.exec.*;
import com.dremio.options.*;
import com.dremio.exec.catalog.*;
import com.dremio.common.exceptions.*;
import java.util.concurrent.*;
import org.bson.io.*;
import org.bson.codecs.*;
import org.apache.arrow.vector.complex.writer.*;
import com.google.common.base.*;
import java.io.*;
import org.bson.*;
import com.dremio.sabot.op.metrics.*;
import com.dremio.sabot.exec.context.*;
import org.slf4j.*;

public class MongoRecordReader extends AbstractRecordReader
{
    private static final Logger logger;
    private static final BsonDocumentCodec DOCUMENT_CODEC;
    private final List<SchemaPath> sanitizedColumns;
    private MongoCollection<RawBsonDocument> collection;
    private MongoCursor<RawBsonDocument> cursor;
    private final MongoConnectionManager manager;
    private JsonReader jsonReader;
    private BsonRecordReader bsonReader;
    private VectorContainerWriter writer;
    private final MongoPipeline pipeline;
    private final ArrowBuf managedbuf;
    private boolean localRead;
    private final boolean enableAllTextMode;
    private final boolean readNumbersAsDouble;
    private final boolean isBsonRecordReader;
    public static final int NUM_RECORDS_FOR_SETUP = 10;
    private final MongoReaderStats mongoReaderStats;
    private Map<String, Integer> decimalScales;
    
    public MongoRecordReader(final MongoConnectionManager manager, final OperatorContext context, final MongoSubScanSpec subScanSpec, final List<SchemaPath> projectedColumns, final List<SchemaPath> sanitizedColumns, final ArrowBuf managedBuf, final boolean isSingleThread, final int target, final BatchSchema fullSchema) {
        super(context, (List)projectedColumns);
        this.localRead = false;
        this.mongoReaderStats = new MongoReaderStats();
        this.manager = manager;
        MongoPipeline tempPipeline = subScanSpec.getPipeline();
        if (!isSingleThread) {
            tempPipeline = tempPipeline.applyMinMaxFilter(subScanSpec.getMinFiltersAsDocument(), subScanSpec.getMaxFiltersAsDocument());
        }
        final OptionManager options = context.getOptions();
        this.numRowsPerBatch = target;
        this.pipeline = tempPipeline;
        this.managedbuf = managedBuf;
        this.enableAllTextMode = options.getOption("store.mongo.all_text_mode").getBoolVal();
        this.readNumbersAsDouble = options.getOption("store.mongo.read_numbers_as_double").getBoolVal();
        this.isBsonRecordReader = options.getOption("store.mongo.bson.record.reader").getBoolVal();
        this.sanitizedColumns = sanitizedColumns;
        MongoRecordReader.logger.debug("BsonRecordReader is enabled? " + this.isBsonRecordReader);
        this.init(subScanSpec, isSingleThread);
        if (fullSchema != null) {
            final Map<String, Integer> values = new HashMap<String, Integer>();
            for (final Field field : fullSchema) {
                if (ArrowType.ArrowTypeID.Decimal == field.getType().getTypeID()) {
                    values.put(field.getName(), ((ArrowType.Decimal)field.getType()).getScale());
                }
            }
            this.decimalScales = Collections.unmodifiableMap((Map<? extends String, ? extends Integer>)values);
        }
    }
    
    protected Collection<SchemaPath> transformColumns(final Collection<SchemaPath> projectedColumns) {
        final Set<SchemaPath> transformed = (Set<SchemaPath>)Sets.newLinkedHashSet();
        if (!this.isStarQuery()) {
            transformed.addAll(projectedColumns);
        }
        else {
            transformed.add(AbstractRecordReader.STAR_COLUMN);
        }
        return transformed;
    }
    
    private void init(final MongoSubScanSpec subScanSpec, final boolean isSingle) {
        MongoConnection client = null;
        if (isSingle) {
            client = this.manager.getReadClient();
        }
        else {
            final List<String> hosts = subScanSpec.getHosts();
            final List<ServerAddress> addresses = (List<ServerAddress>)Lists.newArrayList();
            for (final String host : hosts) {
                addresses.add(new ServerAddress(host));
            }
            client = this.manager.getReadClients(addresses);
        }
        final MongoDatabase db = client.getDatabase(subScanSpec.getDbName());
        this.collection = (MongoCollection<RawBsonDocument>)db.getCollection(subScanSpec.getCollectionName(), (Class)RawBsonDocument.class);
    }
    
    public void setup(final OutputMutator output) throws ExecutionSetupException {
        int numRead = 0;
        final int numRowsPerBatchOriginal = this.numRowsPerBatch;
        try {
            this.writer = new VectorContainerWriter(output);
            if (this.decimalScales != null) {
                final FieldWriter fieldWriter = (FieldWriter)this.writer.rootAsStruct();
                for (final Map.Entry<String, Integer> fieldNameScale : this.decimalScales.entrySet()) {
                    fieldWriter.decimal((String)fieldNameScale.getKey(), (int)fieldNameScale.getValue(), 38);
                }
            }
            final int sizeLimit = Math.toIntExact(this.context.getOptions().getOption((TypeValidators.LongValidator)ExecConstants.LIMIT_FIELD_SIZE_BYTES));
            final int maxLeafLimit = Math.toIntExact(this.context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
            if (!this.sanitizedColumns.isEmpty()) {
                if (this.isBsonRecordReader) {
                    this.bsonReader = new BsonRecordReader(this.managedbuf, Lists.newArrayList((Iterable)this.sanitizedColumns), sizeLimit, maxLeafLimit, this.readNumbersAsDouble, this.decimalScales);
                    MongoRecordReader.logger.debug("Initialized BsonRecordReader.");
                }
                else {
                    this.jsonReader = new JsonReader(this.managedbuf, (List)Lists.newArrayList((Iterable)this.sanitizedColumns), sizeLimit, maxLeafLimit, this.enableAllTextMode, false, this.readNumbersAsDouble);
                    MongoRecordReader.logger.debug(" Initialized JsonRecordReader.");
                }
            }
            this.numRowsPerBatch = 10;
            try {
                numRead = this.next();
            }
            catch (BsonRecordReader.ChangedScaleException sce) {
                this.cursor = null;
                this.decimalScales = this.bsonReader.getDecimalScales();
                throw sce;
            }
        }
        finally {
            this.cursor = null;
            this.updateStats(-numRead, -this.mongoReaderStats.numBytesRead);
            this.writer.reset();
            this.numRowsPerBatch = numRowsPerBatchOriginal;
        }
    }
    
    public int next() {
        if (this.cursor == null) {
            final Stopwatch watch = Stopwatch.createStarted();
            this.startWait();
            try {
                this.cursor = this.pipeline.getCursor(this.collection, this.numRowsPerBatch);
            }
            finally {
                this.stopWait();
            }
            MongoRecordReader.logger.debug("Took {} ms to get cursor", (Object)watch.elapsed(TimeUnit.MILLISECONDS));
        }
        this.writer.allocate();
        this.writer.reset();
        int docCount = 0;
        long numBytesRead = 0L;
        final Stopwatch watch2 = Stopwatch.createStarted();
        try {
            while (docCount < this.numRowsPerBatch) {
                this.startWait();
                try {
                    if (!this.cursor.hasNext()) {
                        break;
                    }
                }
                finally {
                    this.stopWait();
                }
                this.writer.setPosition(docCount);
                this.startWait();
                RawBsonDocument rawBsonDocument;
                try {
                    rawBsonDocument = (RawBsonDocument)this.cursor.next();
                }
                finally {
                    this.stopWait();
                }
                final ByteBuf buffer = rawBsonDocument.getByteBuffer();
                numBytesRead += buffer.remaining();
                if (this.isBsonRecordReader) {
                    final BsonBinaryReader rawBsonReader = new BsonBinaryReader((BsonInput)new ByteBufferBsonInput(buffer));
                    Throwable t = null;
                    BsonDocument bsonDocument;
                    try {
                        bsonDocument = MongoRecordReader.DOCUMENT_CODEC.decode((BsonReader)rawBsonReader, DecoderContext.builder().build());
                    }
                    catch (Throwable t2) {
                        t = t2;
                        throw t2;
                    }
                    finally {
                        if (t != null) {
                            try {
                                rawBsonReader.close();
                            }
                            catch (Throwable t3) {
                                t.addSuppressed(t3);
                            }
                        }
                        else {
                            rawBsonReader.close();
                        }
                    }
                    if (!this.sanitizedColumns.isEmpty()) {
                        this.bsonReader.write((BaseWriter.ComplexWriter)this.writer, (BsonReader)new BsonDocumentReader(bsonDocument));
                    }
                }
                else if (!this.sanitizedColumns.isEmpty()) {
                    final String doc = rawBsonDocument.toJson();
                    this.jsonReader.setSource(doc.getBytes(Charsets.UTF_8));
                    this.jsonReader.write((BaseWriter.ComplexWriter)this.writer);
                }
                ++docCount;
            }
            this.writer.setValueCount(docCount);
            MongoRecordReader.logger.debug("Took {} ms to get {} records", (Object)watch2.elapsed(TimeUnit.MILLISECONDS), (Object)docCount);
            this.updateStats(docCount, numBytesRead);
            return docCount;
        }
        catch (IOException e) {
            final String msg = "Failure while reading document. - Parser was at record: " + (docCount + 1);
            MongoRecordReader.logger.error(msg, (Throwable)e);
            throw new RuntimeException(msg, e);
        }
    }
    
    private void updateStats(final long docCount, final long numBytesRead) {
        if (this.mongoReaderStats != null) {
            this.mongoReaderStats.numBytesRead += numBytesRead;
            this.mongoReaderStats.numRecordsRead += docCount;
            if (this.localRead) {
                this.mongoReaderStats.numRecordsReadLocal += docCount;
            }
            else {
                this.mongoReaderStats.numRecordsReadRemote += docCount;
            }
        }
    }
    
    private void startWait() {
        if (this.context != null && this.context.getStats() != null) {
            this.context.getStats().startWait();
        }
    }
    
    private void stopWait() {
        if (this.context != null && this.context.getStats() != null) {
            this.context.getStats().stopWait();
        }
    }
    
    public void close() {
        if (this.mongoReaderStats != null && this.context != null && this.context.getStats() != null) {
            this.context.getStats().setLongStat((MetricDef)MongoStats.Metric.TOTAL_RECORDS_READ, this.mongoReaderStats.numRecordsRead);
            this.context.getStats().setLongStat((MetricDef)MongoStats.Metric.NUM_LOCAL_RECORDS_READ, this.mongoReaderStats.numRecordsReadLocal);
            this.context.getStats().setLongStat((MetricDef)MongoStats.Metric.NUM_REMOTE_RECORDS_READ, this.mongoReaderStats.numRecordsReadRemote);
            this.context.getStats().setLongStat((MetricDef)MongoStats.Metric.TOTAL_BYTES_READ, this.mongoReaderStats.numBytesRead);
        }
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)MongoRecordReader.class);
        DOCUMENT_CODEC = new BsonDocumentCodec();
    }
    
    private static class MongoReaderStats
    {
        private long numRecordsRead;
        private long numRecordsReadLocal;
        private long numRecordsReadRemote;
        private long numBytesRead;
    }
}
