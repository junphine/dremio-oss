package com.dremio.extras.plugins.elastic.execution;

import com.dremio.plugins.elastic.planning.*;
import com.dremio.elastic.proto.*;
import org.apache.arrow.vector.complex.writer.*;
import com.google.common.util.concurrent.*;
import com.dremio.sabot.exec.context.*;
import com.dremio.exec.store.*;
import com.dremio.extras.plugins.elastic.planning.*;
import com.dremio.exec.record.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.Field;

import com.google.protobuf.*;
import com.google.common.collect.*;
import com.dremio.sabot.op.scan.*;
import com.dremio.common.util.concurrent.*;
import com.dremio.exec.*;
import com.dremio.options.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.vector.complex.fn.*;
import org.apache.arrow.vector.complex.impl.*;
import com.dremio.exec.store.easy.json.*;
import com.google.common.base.*;
import org.apache.arrow.vector.complex.*;
import com.dremio.exec.proto.*;
import java.util.*;
import com.dremio.common.exceptions.*;
import java.util.concurrent.*;
import java.io.*;
import org.apache.arrow.vector.complex.reader.*;
import com.dremio.common.expression.*;
import com.dremio.plugins.elastic.execution.*;
import javax.xml.bind.*;
import com.dremio.common.types.*;
import org.apache.arrow.vector.util.*;
import com.google.gson.internal.*;
import org.slf4j.*;
import com.dremio.plugins.elastic.*;

public class ElasticsearchAggregatorReader extends AbstractRecordReader
{
    private static final Logger logger;
    private static final String CONTAINER_NAME = "root";
    private static final DateFormats.FormatterAndType DATE_TIME_FORMATTER;
    private final ElasticsearchConf config;
    private final OperatorStats stats;
    private final ElasticsearchScanSpec spec;
    private final List<SchemaPath> columns;
    private final List<AggExpr> aggregates;
    private final List<CompleteType> groupByTypes;
    private final boolean hasGroupBys;
    private final String resource;
    private final ElasticReaderProto.ElasticSplitXattr splitAttributes;
    private final ElasticConnectionPool.ElasticConnection connection;
    private final long readTimeoutMillis;
    private final List<String> tableSchemaPath;
    private WorkingBuffer workingBuffer;
    private BaseWriter.ComplexWriter complexWriter;
    private boolean firstIteration;
    private int processedTotal;
    private int processed;
    private int fetch;
    private ListenableFuture<InputStream> futureResult;
    private FieldReader totalResultReader;
    private List<GroupByTermEntry> groupByTermEntries;
    private FieldReader aggregationsResultReader;
    private NonNullableStructVector structVector;
    
    public ElasticsearchAggregatorReader(final OperatorContext context, final ElasticsearchConf config, final ElasticsearchScanSpec spec, final SplitAndPartitionInfo split, final ElasticConnectionPool.ElasticConnection connection, final List<ElasticsearchAggExpr> aggregates, final List<SchemaPath> columns, final BatchSchema schema, final List<String> tableSchemaPath) throws InvalidProtocolBufferException {
        super(context, (List)columns);
        this.firstIteration = true;
        this.processedTotal = 0;
        this.processed = 0;
        this.fetch = 0;
        this.groupByTermEntries = new ArrayList<GroupByTermEntry>();
        this.config = config;
        this.spec = spec;
        this.stats = context.getStats();
        this.splitAttributes = ((split == null) ? null : ElasticReaderProto.ElasticSplitXattr.parseFrom(split.getDatasetSplitInfo().getExtendedProperty()));
        this.resource = ((split == null) ? spec.getResource() : this.splitAttributes.getResource());
        this.processed = 0;
        this.fetch = spec.getFetch();
        this.connection = connection;
        this.groupByTypes = this.deriveDimensionTypes(schema, (aggregates == null) ? 0 : aggregates.size());
        this.hasGroupBys = !this.groupByTypes.isEmpty();
        this.columns = columns;
        this.readTimeoutMillis = config.getReadTimeoutMillis();
        this.tableSchemaPath = tableSchemaPath;
        final List<Field> aggregateFields = schema.getFields().subList(schema.getFieldCount() - aggregates.size(), schema.getFieldCount());
        final ImmutableList.Builder<AggExpr> exprB = ImmutableList.builder();
        int i = 0;
        for (final ElasticsearchAggExpr expr : aggregates) {
            final Field field = aggregateFields.get(i);
            exprB.add(new AggExpr(CompleteType.fromField(field).toMinorType(), field.getName(), expr.getOperation(), expr.getType()));
            ++i;
        }
        this.aggregates = (List<AggExpr>)exprB.build();
    }
    
    private List<CompleteType> deriveDimensionTypes(final BatchSchema schema, final int measureCount) {
        return FluentIterable.from(schema.getFields().subList(0, schema.getFieldCount() - measureCount)).transform(new Function<Field, CompleteType>() {
            public CompleteType apply(final Field input) {
                return CompleteType.fromField(input);
            }
        }).toList();
    }
    
    private void startQuery() {
        final ElasticActions.Search<InputStream> search = new ElasticActions.SearchInputStream().setQuery(this.spec.getQuery()).setResource(this.resource);
        if (this.splitAttributes != null) {
            search.setParameter("preference", "_shards:" + this.splitAttributes.getShard());
        }
        this.futureResult = this.connection.executeAsync(search);
    }
    
    public void setup(final OutputMutator output) throws ExecutionSetupException {
        this.startQuery();
        this.complexWriter = (BaseWriter.ComplexWriter)new VectorContainerWriter(output);
        this.workingBuffer = new WorkingBuffer(this.context.getManagedBuffer());
    }
    
    public int next() {
        if (!this.firstIteration && this.groupByTermEntries.isEmpty()) {
            return 0;
        }
        try {
            if (this.firstIteration) {
                try {
                    this.stats.startWait();
                    final InputStream result = DremioFutures.getChecked(this.futureResult, UserException.class, this.readTimeoutMillis, TimeUnit.MILLISECONDS, cause -> {
                        if (cause instanceof UserException) {
                            return (UserException)cause;
                        }
                        else {
                            return UserException.systemError(cause).build(ElasticsearchAggregatorReader.logger);
                        }
                    });
                    final int sizeLimit = Math.toIntExact(this.context.getOptions().getOption((TypeValidators.LongValidator)ExecConstants.LIMIT_FIELD_SIZE_BYTES));
                    final int maxLeafLimit = Math.toIntExact(this.context.getOptions().getOption(CatalogOptions.METADATA_LEAF_COLUMN_MAX));
                    final JsonReader jsonReader = new JsonReader(this.context.getManagedBuffer(), sizeLimit, maxLeafLimit, true, false, false);
                    jsonReader.setSource(result);
                    (this.structVector = NonNullableStructVector.empty("root", this.context.getAllocator())).allocateNew();
                    final ComplexWriterImpl writer = new ComplexWriterImpl("root", this.structVector);
                    final JsonProcessor.ReadState readState = jsonReader.write((BaseWriter.ComplexWriter)writer);
                    Preconditions.checkState(readState == JsonProcessor.ReadState.WRITE_SUCCEED, "Failed to parse json response");
                    writer.setValueCount(1);
                    this.totalResultReader = (FieldReader)((StructVector)this.structVector.getChild("root", StructVector.class)).getReader();
                }
                catch (UserException e) {
                    if (e.getErrorType() == UserBitShared.DremioPBError.ErrorType.INVALID_DATASET_METADATA) {
                        ElasticsearchAggregatorReader.logger.trace("failed with invalid metadata, ", (Throwable)e);
                        throw UserException.invalidMetadataError().setAdditionalExceptionContext((AdditionalExceptionContext)new InvalidMetadataErrorContext((List)Collections.singletonList(this.tableSchemaPath))).build(ElasticsearchAggregatorReader.logger);
                    }
                    throw e;
                }
                catch (TimeoutException e2) {
                    throw UserException.dataReadError((Throwable)e2).message("Elastic aggregation didn't return within the configured timeout of %s milliseconds.", new Object[] { Long.toString(this.readTimeoutMillis) }).build(ElasticsearchAggregatorReader.logger);
                }
                finally {
                    this.stats.stopWait();
                }
            }
            int n = 0;
            n = this.process();
            this.ensureAtLeastOneField(this.complexWriter);
            return n;
        }
        catch (IOException e3) {
            throw UserException.dataReadError((Throwable)e3).message("Failure while consuming response from elastic.", new Object[] { e3 }).build(ElasticsearchAggregatorReader.logger);
        }
    }
    
    private int process() throws IOException {
        final int total = readAsInt(this.totalResultReader.reader("hits").reader("total").readText().toString());
        this.processed = 0;
        this.complexWriter.allocate();
        this.complexWriter.reset();
        if (this.firstIteration) {
            this.aggregationsResultReader = this.totalResultReader.reader("aggregations");
        }
        final BaseWriter.StructWriter structWriter = this.complexWriter.rootAsStruct();
        if (this.hasGroupBys) {
            this.processGroupbys(structWriter, (BaseReader.StructReader)this.aggregationsResultReader, 0);
        }
        else {
            this.processAggregates(structWriter, (BaseReader.StructReader)this.aggregationsResultReader, total);
        }
        final int count = (this.fetch == 0) ? 0 : this.processed;
        assert count <= this.numRowsPerBatch : "Fetched " + count + " rows, but batch size limit was " + this.numRowsPerBatch;
        this.complexWriter.setValueCount(count);
        this.firstIteration = false;
        return count;
    }
    
    public void ensureAtLeastOneField(final BaseWriter.ComplexWriter writer) {
        final SchemaPath sp = this.columns.get(0);
        PathSegment fieldPath = (PathSegment)sp.getRootSegment();
        BaseWriter.StructWriter fieldWriter = writer.rootAsStruct();
        while (fieldPath.getChild() != null && !fieldPath.getChild().isArray()) {
            fieldWriter = fieldWriter.struct(fieldPath.getNameSegment().getPath());
            fieldPath = fieldPath.getChild();
        }
        if (fieldWriter.isEmptyStruct()) {
            fieldWriter.integer(fieldPath.getNameSegment().getPath());
        }
    }
    
    private boolean fetchedEnough() {
        return this.processed >= this.numRowsPerBatch;
    }
    
    private void processAggregates(final BaseWriter.StructWriter structWriter, final BaseReader.StructReader aggElasticResults, final long docCountBucket) throws IOException {
        if (this.fetchedEnough()) {
            return;
        }
        this.complexWriter.setPosition(this.processed);
        if (this.hasGroupBys) {
            for (int i = 0; i < this.groupByTermEntries.size(); ++i) {
                final GroupByTermEntry groupByTermEntry = this.groupByTermEntries.get(i);
                final String key = groupByTermEntry.currentBucketKey;
                if (key != null) {
                    final CompleteType groupByType = this.groupByTypes.get(i);
                    final String outputName = groupByTermEntry.outputName;
                    switch (groupByType.toMinorType()) {
                        case BIGINT: {
                            try {
                                final long longValue = Long.valueOf(key);
                                structWriter.bigInt(outputName).writeBigInt(longValue);
                            }
                            catch (NumberFormatException e) {
                                ElasticsearchAggregatorReader.logger.debug("group by field (" + outputName + "), value (" + key + ") cannot be parsed as number");
                            }
                            break;
                        }
                        case BIT: {
                            final boolean boolValue = ElasticsearchJsonReader.parseElasticBoolean(key);
                            structWriter.bit(outputName).writeBit((int)(boolValue ? 1 : 0));
                            break;
                        }
                        case DATE: {
                            try {
                                final long longValue2 = parseDateTime(key);
                                structWriter.dateMilli(outputName).writeDateMilli(longValue2);
                            }
                            catch (IllegalArgumentException e2) {
                                ElasticsearchAggregatorReader.logger.debug("group by field (" + outputName + "), value (" + key + ") cannot be parsed as time");
                            }
                            break;
                        }
                        case FLOAT4: {
                            try {
                                final float floatValue = Float.valueOf(key);
                                structWriter.float4(outputName).writeFloat4(floatValue);
                            }
                            catch (NumberFormatException e3) {
                                ElasticsearchAggregatorReader.logger.debug("group by field (" + outputName + "), value (" + key + ") cannot be parsed as number");
                            }
                            break;
                        }
                        case FLOAT8: {
                            try {
                                final double doubleValue = Double.valueOf(key);
                                structWriter.float8(outputName).writeFloat8(doubleValue);
                            }
                            catch (NumberFormatException e3) {
                                ElasticsearchAggregatorReader.logger.debug("group by field (" + outputName + "), value (" + key + ") cannot be parsed as number");
                            }
                            break;
                        }
                        case INT: {
                            try {
                                final int intValue = Integer.valueOf(key);
                                structWriter.integer(outputName).writeInt(intValue);
                            }
                            catch (NumberFormatException e3) {
                                ElasticsearchAggregatorReader.logger.debug("group by field (" + outputName + "), value (" + key + ") cannot be parsed as number");
                            }
                            break;
                        }
                        case TIME: {
                            try {
                                final int intValue = (int)parseDateTime(key);
                                structWriter.timeMilli(outputName).writeTimeMilli(intValue);
                            }
                            catch (IllegalArgumentException e2) {
                                ElasticsearchAggregatorReader.logger.debug("group by field (" + outputName + "), value (" + key + ") cannot be parsed as time");
                            }
                            break;
                        }
                        case TIMESTAMP: {
                            try {
                                final long longValue2 = parseDateTime(key);
                                structWriter.timeStampMilli(outputName).writeTimeStampMilli(longValue2);
                            }
                            catch (IllegalArgumentException e2) {
                                ElasticsearchAggregatorReader.logger.debug("group by field (" + outputName + "), value (" + key + ") cannot be parsed as time");
                            }
                            break;
                        }
                        case VARBINARY: {
                            final byte[] bytes = DatatypeConverter.parseBase64Binary(key);
                            structWriter.varBinary(outputName).writeVarBinary(0, this.workingBuffer.prepareBinary(bytes), this.workingBuffer.getBuf());
                            break;
                        }
                        case VARCHAR: {
                            structWriter.varChar(outputName).writeVarChar(0, this.workingBuffer.prepareVarCharHolder(key), this.workingBuffer.getBuf());
                            break;
                        }
                        default: {
                            throw new UnsupportedOperationException("type(" + groupByType + ") " + outputName + " cannot be grouped");
                        }
                    }
                }
            }
        }
        for (final AggExpr agg : this.aggregates) {
            final String operation = agg.getOperation();
            final String fieldName = agg.getOutputName();
            boolean handledSpecial = false;
            final String s = operation;
            int n = -1;
            switch (s.hashCode()) {
                case 82475: {
                    if (s.equals("SUM")) {
                        n = 0;
                        break;
                    }
                    break;
                }
                case 35803529: {
                    if (s.equals("$SUM0")) {
                        n = 1;
                        break;
                    }
                    break;
                }
                case 64313583: {
                    if (s.equals("COUNT")) {
                        n = 2;
                        break;
                    }
                    break;
                }
                case -102600828: {
                    if (s.equals("STDDEV_POP")) {
                        n = 3;
                        break;
                    }
                    break;
                }
                case 955438329: {
                    if (s.equals("VAR_POP")) {
                        n = 4;
                        break;
                    }
                    break;
                }
                case -1839078638: {
                    if (s.equals("STDDEV")) {
                        n = 5;
                        break;
                    }
                    break;
                }
                case 1114417534: {
                    if (s.equals("STDDEV_SAMP")) {
                        n = 6;
                        break;
                    }
                    break;
                }
                case -466948495: {
                    if (s.equals("VARIANCE")) {
                        n = 7;
                        break;
                    }
                    break;
                }
                case -446106967: {
                    if (s.equals("VAR_SAMP")) {
                        n = 8;
                        break;
                    }
                    break;
                }
            }
            Label_1654: {
                switch (n) {
                    case 0:
                    case 1:
                    case 2: {
                        switch (agg.getType()) {
                            case COUNT_ALL: {
                                structWriter.bigInt(fieldName).writeBigInt(docCountBucket);
                                handledSpecial = true;
                                break Label_1654;
                            }
                            case COUNT_DISTINCT: {
                                final Double element = aggElasticResults.reader(fieldName).reader("value").readDouble();
                                if (element != null) {
                                    structWriter.bigInt(fieldName).writeBigInt(element.longValue());
                                }
                                handledSpecial = true;
                                break Label_1654;
                            }
                            case NORMAL: {
                                break Label_1654;
                            }
                            default: {
                                break Label_1654;
                            }
                        }
                        //-break;
                    }
                    case 3: {
                        final Double element = aggElasticResults.reader(fieldName).reader("std_deviation").readDouble();
                        if (element != null) {
                            structWriter.float8(fieldName).writeFloat8((double)element);
                        }
                        handledSpecial = true;
                        break;
                    }
                    case 4: {
                        final Double element = aggElasticResults.reader(fieldName).reader("variance").readDouble();
                        if (element != null) {
                            structWriter.float8(fieldName).writeFloat8((double)element);
                        }
                        handledSpecial = true;
                        break;
                    }
                    case 5:
                    case 6:
                    case 7:
                    case 8: {
                        final BaseReader.StructReader extendedStats = (BaseReader.StructReader)aggElasticResults.reader(fieldName);
                        if (extendedStats.reader("avg").isSet()) {
                            Preconditions.checkState(extendedStats.reader("sum_of_squares").isSet() && extendedStats.reader("sum").isSet() && extendedStats.reader("count").isSet(), "extended_stats returned invalid results");
                            final double mu = extendedStats.reader("avg").readDouble();
                            final double sumOfSquares = extendedStats.reader("sum_of_squares").readDouble();
                            final double sum = extendedStats.reader("sum").readDouble();
                            final double count = extendedStats.reader("count").readDouble();
                            final double var_sam = (sumOfSquares - 2.0 * mu * sum + mu * mu * count) / (count - 1.0);
                            if (operation.equals("VARIANCE") || operation.equals("VAR_SAMP")) {
                                structWriter.float8(fieldName).writeFloat8(var_sam);
                            }
                            else {
                                structWriter.float8(fieldName).writeFloat8(Math.sqrt(var_sam));
                            }
                        }
                        handledSpecial = true;
                        break;
                    }
                }
            }
            if (handledSpecial) {
                continue;
            }
            final TypeProtos.MinorType minorType = agg.getMinorType();
            if (minorType == TypeProtos.MinorType.VARCHAR || minorType == TypeProtos.MinorType.VARBINARY) {
                final Text element2 = aggElasticResults.reader(fieldName).reader("value_as_string").readText();
                if (element2 == null) {
                    continue;
                }
                final String stringValue = element2.toString();
                structWriter.varChar(fieldName).writeVarChar(0, this.workingBuffer.prepareVarCharHolder(stringValue), this.workingBuffer.getBuf());
            }
            else {
                final FieldReader fieldReader = aggElasticResults.reader(fieldName).reader("value");
                if (!fieldReader.isSet()) {
                    continue;
                }
                switch (minorType) {
                    case BIT: {
                        final boolean boolValue2 = fieldReader.readBoolean();
                        structWriter.bit(fieldName).writeBit((int)(boolValue2 ? 1 : 0));
                        continue;
                    }
                    case INT:
                    case TINYINT:
                    case SMALLINT:
                    case UINT1:
                    case UINT2:
                    case UINT4:
                    case UINT8: {
                        final int intValue2 = readAsInt(fieldReader.readText().toString());
                        structWriter.integer(fieldName).writeInt(intValue2);
                        continue;
                    }
                    case BIGINT: {
                        final long longValue3 = readAsLong(fieldReader.readText().toString());
                        structWriter.bigInt(fieldName).writeBigInt(longValue3);
                        continue;
                    }
                    case FLOAT4: {
                        final float floatValue2 = readAsFloat(fieldReader.readText().toString());
                        structWriter.float4(fieldName).writeFloat4(floatValue2);
                        continue;
                    }
                    case FLOAT8: {
                        final double doubleValue2 = readAsDouble(fieldReader.readText().toString());
                        structWriter.float8(fieldName).writeFloat8(doubleValue2);
                        continue;
                    }
                    case DATE: {
                        final long dateValue = readAsLong(aggElasticResults.reader(fieldName).reader("value").readText().toString());
                        structWriter.dateMilli(fieldName).writeDateMilli(dateValue);
                        continue;
                    }
                    case TIME: {
                        final int timeValue = readAsInt(aggElasticResults.reader(fieldName).reader("value").readText().toString());
                        structWriter.timeMilli(fieldName).writeTimeMilli(timeValue);
                        continue;
                    }
                    case TIMESTAMP: {
                        final long timestampValue = readAsLong(aggElasticResults.reader(fieldName).reader("value").readText().toString());
                        structWriter.timeStampMilli(fieldName).writeTimeStampMilli(timestampValue);
                        continue;
                    }
                    default: {
                        throw UserException.unsupportedError().message("Cannot pushdown aggregation that returns data type: %s, operator: %s", new Object[] { minorType, operation }).build(ElasticsearchAggregatorReader.logger);
                    }
                }
            }
        }
        ++this.processed;
        ++this.processedTotal;
    }
    
    private static long parseDateTime(final String dateTime) {
        try {
            return Long.parseLong(dateTime);
        }
        catch (NumberFormatException e) {
            return ElasticsearchAggregatorReader.DATE_TIME_FORMATTER.parseToLong(dateTime);
        }
    }
    
    private static long readAsLong(final String number) {
        return new LazilyParsedNumber(number).longValue();
    }
    
    private static int readAsInt(final String number) {
        return new LazilyParsedNumber(number).intValue();
    }
    
    private static float readAsFloat(final String number) {
        return new LazilyParsedNumber(number).floatValue();
    }
    
    private static double readAsDouble(final String number) {
        return new LazilyParsedNumber(number).doubleValue();
    }
    
    void processGroupbys(final BaseWriter.StructWriter structWriter, final BaseReader.StructReader aggregations, final int depth) throws IOException {
        GroupByTermEntry entry;
        boolean advanceBucket;
        if (depth < this.groupByTermEntries.size()) {
            entry = this.groupByTermEntries.get(depth);
            advanceBucket = (depth == this.groupByTermEntries.size() - 1);
        }
        else {
            entry = new GroupByTermEntry(aggregations, this.groupByTypes.get(depth));
            entry.getAggEntry(true);
            this.groupByTermEntries.add(entry);
            advanceBucket = true;
        }
        for (String aggEntry = entry.getAggEntry(false); aggEntry != null; aggEntry = entry.getAggEntry(true)) {
            for (BaseReader.StructReader bucket = entry.getBucket(advanceBucket); bucket != null; bucket = entry.getBucket(true)) {
                if (depth >= this.groupByTypes.size() - 1) {
                    this.processAggregates(structWriter, bucket, entry.currentBucketDocCount);
                }
                else {
                    this.processGroupbys(structWriter, bucket, depth + 1);
                }
                if (this.fetchedEnough()) {
                    return;
                }
            }
        }
        this.groupByTermEntries.remove(this.groupByTermEntries.size() - 1);
    }
    
    public void close() throws Exception {
        if (this.structVector != null) {
            this.structVector.close();
        }
    }
    
    static {
        logger = LoggerFactory.getLogger(ElasticsearchAggregatorReader.class);
        DATE_TIME_FORMATTER = DateFormats.getFormatterAndType("dateOptionalTime");
    }
    
    private static class AggExpr
    {
        private final TypeProtos.MinorType minorType;
        private final String outputName;
        private final String operation;
        private final ElasticsearchAggExpr.Type type;
        
        public AggExpr(final TypeProtos.MinorType minorType, final String outputName, final String operation, final ElasticsearchAggExpr.Type type) {
            this.minorType = minorType;
            this.outputName = outputName;
            this.operation = operation;
            this.type = type;
        }
        
        public TypeProtos.MinorType getMinorType() {
            return this.minorType;
        }
        
        public String getOutputName() {
            return this.outputName;
        }
        
        public String getOperation() {
            return this.operation;
        }
        
        public ElasticsearchAggExpr.Type getType() {
            return this.type;
        }
    }
    
    private final class GroupByTermEntry
    {
        private final BaseReader.StructReader aggregations;
        private final Iterator<String> aggEntryIterator;
        private String outputName;
        private FieldReader buckets;
        private int bucketsIndex;
        private BaseReader.StructReader currentBucket;
        private String currentBucketKey;
        private long currentBucketDocCount;
        private final CompleteType groupByType;
        private String aggEntryKey;
        
        GroupByTermEntry(final BaseReader.StructReader aggregations, final CompleteType groupByType) {
            this.outputName = null;
            this.buckets = null;
            this.bucketsIndex = -1;
            this.currentBucket = null;
            this.currentBucketKey = null;
            this.currentBucketDocCount = -1L;
            this.aggregations = aggregations;
            this.aggEntryIterator = (Iterator<String>)aggregations.iterator();
            this.groupByType = groupByType;
        }
        
        String getAggEntry(final boolean advance) {
            if (advance) {
                while (this.aggEntryIterator.hasNext()) {
                    this.aggEntryKey = this.aggEntryIterator.next();
                    if (!this.aggEntryKey.equalsIgnoreCase("key") && !this.aggEntryKey.equalsIgnoreCase("doc_count")) {
                        if (this.aggEntryKey.equalsIgnoreCase("key_as_string")) {
                            continue;
                        }
                        this.outputName = this.aggEntryKey;
                        this.buckets = this.aggregations.reader(this.aggEntryKey).reader("buckets");
                        this.bucketsIndex = 0;
                        return this.aggEntryKey;
                    }
                }
                return null;
            }
            assert this.aggEntryKey != null;
            return this.aggEntryKey;
        }
        
        BaseReader.StructReader getBucket(final boolean advance) {
            if (!advance) {
                assert this.currentBucket != null;
                return this.currentBucket;
            }
            else {
                if (this.buckets.next()) {
                    this.currentBucket = (BaseReader.StructReader)this.buckets.reader();
                    final String key = this.currentBucket.reader("key").readText().toString();
                    this.currentBucketDocCount = readAsLong(this.currentBucket.reader("doc_count").readText().toString());
                    boolean nullValue = false;
                    switch (this.groupByType.toMinorType()) {
                        case BIGINT: {
                            nullValue = key.equals(Long.toString(ElasticsearchConstants.NULL_LONG_TAG));
                            break;
                        }
                        case BIT: {
                            nullValue = key.equals("NULL_BOOLEAN_TAG");
                            break;
                        }
                        case DATE:
                        case TIME:
                        case TIMESTAMP: {
                            nullValue = key.equals(Long.toString(ElasticsearchConstants.NULL_TIME_TAG));
                            break;
                        }
                        case FLOAT4:
                        case FLOAT8: {
                            nullValue = key.equals(Double.toString(ElasticsearchConstants.NULL_DOUBLE_TAG));
                            break;
                        }
                        case INT: {
                            nullValue = key.equals(Integer.toString(ElasticsearchConstants.NULL_INTEGER_TAG));
                            break;
                        }
                        case VARBINARY: {
                            nullValue = key.equals(ElasticsearchConstants.NULL_BYTE_TAG);
                            break;
                        }
                        case VARCHAR: {
                            final Text ele = this.currentBucket.reader("key_as_string").readText();
                            if (ele != null) {
                                nullValue = ele.toString().equals("NULL_STRING_TAG");
                                break;
                            }
                            nullValue = key.equals("NULL_STRING_TAG");
                            break;
                        }
                        default: {
                            throw UserException.dataReadError().message("Group by not supported for " + this.groupByType, new Object[0]).build(ElasticsearchAggregatorReader.logger);
                        }
                    }
                    if (nullValue) {
                        this.currentBucketKey = null;
                    }
                    else {
                        this.currentBucketKey = key;
                    }
                    return this.currentBucket;
                }
                return null;
            }
        }
    }
}
