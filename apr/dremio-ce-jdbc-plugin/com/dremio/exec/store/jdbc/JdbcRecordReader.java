package com.dremio.exec.store.jdbc;

import com.google.common.annotations.*;
import javax.sql.*;
import java.util.concurrent.atomic.*;
import com.dremio.sabot.exec.context.*;
import com.dremio.service.namespace.capabilities.*;
import com.dremio.exec.*;
import com.dremio.options.*;
import com.google.common.util.concurrent.*;
import org.apache.arrow.vector.*;
import com.dremio.sabot.op.scan.*;
import com.google.common.primitives.*;
import java.util.concurrent.*;
import com.dremio.exec.store.*;
import com.dremio.exec.exception.*;
import com.dremio.exec.store.jdbc.dialect.*;
import com.google.common.collect.*;
import com.dremio.common.*;
import org.apache.parquet.*;
import java.util.stream.*;
import com.dremio.exec.expr.*;
import org.apache.arrow.vector.types.pojo.*;
import com.dremio.common.expression.*;
import java.util.function.*;
import java.math.*;
import org.slf4j.*;
import com.google.common.base.*;
import com.dremio.common.exceptions.*;
import java.util.*;
import java.sql.*;
import java.time.*;

@VisibleForTesting
public final class JdbcRecordReader extends AbstractRecordReader
{
    private static final boolean DISABLE_TRIM_ON_CHARS;
    private static final Logger logger;
    private final DataSource source;
    private ResultSet resultSet;
    private final String storagePluginName;
    private Connection connection;
    private volatile Statement statement;
    private final String sql;
    private ImmutableList<ValueVector> vectors;
    private ImmutableList<Copier<?>> copiers;
    private boolean hasNext;
    private final List<SchemaPath> columns;
    private final int fetchSize;
    private final AtomicBoolean cancelled;
    private final boolean needsTrailingPaddingTrim;
    private final boolean coerceTimesToUTC;
    private final boolean coerceTimestampsToUTC;
    private final boolean adjustDateTimezone;
    private final TypeMapper dialectTypeMapper;
    private final Collection<List<String>> tableList;
    private final Set<String> skippedColumns;
    private final int maxCellSize;
    private static final String CANCEL_ERROR_MESSAGE = "Call to cancel query on RDBMs source failed with error: {}";
    
    public JdbcRecordReader(final OperatorContext context, final DataSource source, final String sql, final String storagePluginName, final List<SchemaPath> columns, final int fetchSize, final ListenableFuture<Boolean> cancelled, final SourceCapabilities capabilities, final TypeMapper typeMapper, final Collection<List<String>> tableList, final Set<String> skippedColumns) {
        super(context, (List)columns);
        this.hasNext = true;
        this.source = source;
        this.sql = sql;
        this.storagePluginName = storagePluginName;
        this.columns = columns;
        this.fetchSize = fetchSize;
        this.needsTrailingPaddingTrim = capabilities.getCapability(JdbcStoragePlugin.REQUIRE_TRIMS_ON_CHARS);
        this.coerceTimesToUTC = capabilities.getCapability(JdbcStoragePlugin.COERCE_TIMES_TO_UTC);
        this.coerceTimestampsToUTC = capabilities.getCapability(JdbcStoragePlugin.COERCE_TIMESTAMPS_TO_UTC);
        this.adjustDateTimezone = capabilities.getCapability(JdbcStoragePlugin.ADJUST_DATE_TIMEZONE);
        this.dialectTypeMapper = typeMapper;
        this.tableList = tableList;
        this.skippedColumns = skippedColumns;
        this.cancelled = new AtomicBoolean(false);
        if (context != null) {
            this.maxCellSize = Math.toIntExact(context.getOptions().getOption((TypeValidators.LongValidator)ExecConstants.LIMIT_FIELD_SIZE_BYTES));
            Futures.addCallback((ListenableFuture)cancelled, (FutureCallback)new FutureCallback<Boolean>() {
                public void onFailure(final Throwable t) {
                }
                
                public void onSuccess(final Boolean result) {
                    JdbcRecordReader.this.cancel();
                }
            }, (Executor)context.getExecutor());
        }
        else {
            this.maxCellSize = Math.toIntExact(ExecConstants.LIMIT_FIELD_SIZE_BYTES.getDefault().getNumVal());
        }
    }
    
    private Copier<?> getCopier(final int offset, final ResultSet result, final ValueVector v) {
        if (v instanceof BigIntVector) {
            return new BigIntCopier(offset, result, (BigIntVector)v);
        }
        if (v instanceof Float4Vector) {
            return new Float4Copier(offset, result, (Float4Vector)v);
        }
        if (v instanceof Float8Vector) {
            return new Float8Copier(offset, result, (Float8Vector)v);
        }
        if (v instanceof IntVector) {
            return new IntCopier(offset, result, (IntVector)v);
        }
        if (v instanceof VarCharVector) {
            if (!JdbcRecordReader.DISABLE_TRIM_ON_CHARS && this.needsTrailingPaddingTrim) {
                return new VarCharTrimCopier(this.maxCellSize, offset, result, (VarCharVector)v);
            }
            return new VarCharCopier(this.maxCellSize, offset, result, (VarCharVector)v);
        }
        else {
            if (v instanceof VarBinaryVector) {
                return new VarBinaryCopier(this.maxCellSize, offset, result, (VarBinaryVector)v);
            }
            if (v instanceof DateMilliVector) {
                if (this.adjustDateTimezone) {
                    return new DateTimeZoneAdjustmentCopier(offset, result, (DateMilliVector)v);
                }
                return new DateCopier(offset, result, (DateMilliVector)v);
            }
            else if (v instanceof TimeMilliVector) {
                if (this.coerceTimesToUTC) {
                    return new TimeCopierCoerceToUTC(offset, result, (TimeMilliVector)v);
                }
                return new TimeCopier(offset, result, (TimeMilliVector)v);
            }
            else if (v instanceof TimeStampMilliVector) {
                if (this.coerceTimestampsToUTC) {
                    return new TimeStampCopierCoerceToUTC(offset, result, (TimeStampMilliVector)v);
                }
                return new TimeStampCopier(offset, result, (TimeStampMilliVector)v);
            }
            else {
                if (v instanceof BitVector) {
                    return new BitCopier(offset, result, (BitVector)v);
                }
                if (v instanceof DecimalVector) {
                    return new DecimalCopier(offset, result, (DecimalVector)v);
                }
                throw new IllegalArgumentException("Unknown how to handle vector.");
            }
        }
    }
    
    public void setup(final OutputMutator output) {
        final long longFetchSize = (this.fetchSize != 0) ? Math.min(this.fetchSize, this.numRowsPerBatch) : ((long)this.numRowsPerBatch);
        final int fetchSize = Ints.saturatedCast(longFetchSize);
        try {
            this.connection = this.source.getConnection();
            this.statement = this.connection.createStatement();
            if (this.cancelled.get()) {
                throw StoragePluginUtils.message(UserException.ioExceptionError((Throwable)new CancellationException()), this.storagePluginName, "Query was cancelled.", new Object[0]).addContext("sql", this.sql).build(JdbcRecordReader.logger);
            }
            this.statement.setFetchSize(fetchSize);
            this.resultSet = this.statement.executeQuery(this.sql);
            final ResultSetMetaData meta = this.resultSet.getMetaData();
            final ImmutableList.Builder<ValueVector> vectorBuilder = (ImmutableList.Builder<ValueVector>)ImmutableList.builder();
            final ImmutableList.Builder<Copier<?>> copierBuilder = (ImmutableList.Builder<Copier<?>>)ImmutableList.builder();
            final List<JdbcToFieldMapping> mappings = this.dialectTypeMapper.mapJdbcToArrowFields(null, null, this::throwInvalidMetadataError, this.connection, meta, this.skippedColumns, false);
            final ImmutableList.Builder<ValueVector> valueVectorBuilder;
            final ImmutableList.Builder<Copier<?>> copierBuilder2;
            mappings.forEach(mapping -> {
                if (!this.skippedColumns.contains(mapping.getField().getName().toLowerCase(Locale.ROOT))) {
                    this.addFieldToOutput(output, valueVectorBuilder, copierBuilder2, mapping);
                }
                return;
            });
            this.checkSchemaConsistency(mappings);
            this.vectors = (ImmutableList<ValueVector>)vectorBuilder.build();
            this.copiers = (ImmutableList<Copier<?>>)copierBuilder.build();
        }
        catch (SQLException e) {
            throw StoragePluginUtils.message(UserException.dataReadError((Throwable)e), this.storagePluginName, e.getMessage(), new Object[0]).addContext("sql", this.sql).build(JdbcRecordReader.logger);
        }
        catch (SchemaChangeException e2) {
            throw UserException.dataReadError((Throwable)e2).message("The JDBC storage plugin failed while trying setup the SQL query. ", new Object[0]).addContext("sql", this.sql).addContext("plugin", this.storagePluginName).build(JdbcRecordReader.logger);
        }
    }
    
    public int next() {
        int counter = 0;
        try {
            while (counter < this.numRowsPerBatch && this.hasNext && (this.hasNext = this.resultSet.next())) {
                for (final Copier<?> c : this.copiers) {
                    c.copy(counter);
                }
                ++counter;
            }
        }
        catch (SQLException e) {
            throw StoragePluginUtils.message(UserException.dataReadError((Throwable)e), this.storagePluginName, e.getMessage(), new Object[0]).addContext("sql", this.sql).build(JdbcRecordReader.logger);
        }
        catch (IllegalArgumentException e2) {
            throw UserException.validationError((Throwable)e2).message(e2.getMessage(), new Object[0]).addContext("sql", this.sql).build();
        }
        final int maxCounter = counter;
        this.vectors.forEach(vv -> vv.setValueCount(maxCounter));
        return maxCounter;
    }
    
    public void cancel() {
        final boolean alreadyCancelled = this.cancelled.getAndSet(true);
        if (alreadyCancelled) {
            return;
        }
        try {
            if (this.statement != null && !this.statement.isClosed()) {
                this.statement.cancel();
            }
        }
        catch (SQLException e) {
            JdbcRecordReader.logger.info("Call to cancel query on RDBMs source failed with error: {}", (Object)e.getMessage());
            JdbcRecordReader.logger.debug("Call to cancel query on RDBMs source failed with error: {}", (Object)e.getMessage(), (Object)e);
        }
    }
    
    public void close() throws Exception {
        try {
            this.cancel();
        }
        finally {
            AutoCloseables.close(new AutoCloseable[] { this.resultSet, this.statement, this.connection });
        }
    }
    
    public void checkSchemaConsistency(final List<JdbcToFieldMapping> mappings) {
        if (mappings.size() > this.columns.size()) {
            JdbcRecordReader.logger.debug("More columns returned from generated SQL than from query planning. Checking if these are all skipped columns.");
            final List<String> unexpectedColumns = this.identifyUnexpectedColumns(mappings);
            if (!unexpectedColumns.isEmpty()) {
                JdbcRecordReader.logger.debug("Unrecognized columns returned from generated SQL than from query planning. Throwing invalid metadata error to trigger refresh of columns.");
                throw UserException.invalidMetadataError().addContext("Unexpected columns", Strings.join((Iterable)unexpectedColumns, ",")).addContext("Expected skipped columns", Strings.join((Iterable)this.skippedColumns, ",")).setAdditionalExceptionContext((AdditionalExceptionContext)new InvalidMetadataErrorContext((List)ImmutableList.copyOf((Collection)this.tableList))).build(JdbcRecordReader.logger);
            }
        }
        else if (mappings.size() < this.columns.size()) {
            JdbcRecordReader.logger.debug("Fewer columns returned from generated SQL than from query planning. Throwing invalid metadata error to trigger refresh of columns.");
            final List<String> unexpectedColumns = this.identifyUnexpectedColumns(mappings);
            final UserException.Builder builder = UserException.invalidMetadataError();
            if (!unexpectedColumns.isEmpty()) {
                builder.addContext("Unexpected columns: %s", Strings.join((Iterable)unexpectedColumns, ","));
            }
            throw builder.addContext("Expected skipped columns: ", Strings.join((Iterable)this.skippedColumns, ",")).setAdditionalExceptionContext((AdditionalExceptionContext)new InvalidMetadataErrorContext((List)ImmutableList.copyOf((Collection)this.tableList))).build(JdbcRecordReader.logger);
        }
        final List<String> mappingsFieldNames = mappings.stream().map(m -> m.getField().getName().toLowerCase(Locale.ROOT)).collect((Collector<? super Object, ?, List<String>>)Collectors.toList());
        Preconditions.checkArgument(this.columns.size() == mappingsFieldNames.size());
        for (int i = 0; i < this.columns.size(); ++i) {
            Preconditions.checkArgument(this.columns.get(i).getAsUnescapedPath().toLowerCase(Locale.ROOT).equals(mappingsFieldNames.get(i)));
        }
    }
    
    private void addFieldToOutput(final OutputMutator output, final ImmutableList.Builder<ValueVector> valueVectorBuilder, final ImmutableList.Builder<Copier<?>> copierBuilder, final JdbcToFieldMapping mapping) {
        final Field field = mapping.getField();
        final Class<? extends ValueVector> clazz = (Class<? extends ValueVector>)TypeHelper.getValueVectorClass(field);
        final ValueVector vector = output.addField(field, (Class)clazz);
        valueVectorBuilder.add((Object)vector);
        copierBuilder.add((Object)this.getCopier(mapping.getJdbcOrdinal(), this.resultSet, vector));
    }
    
    private List<String> identifyUnexpectedColumns(final List<JdbcToFieldMapping> jdbcMappings) {
        final List<String> expectedColumns = this.columns.stream().map((Function<? super Object, ?>)BasePath::getAsUnescapedPath).map((Function<? super Object, ?>)String::toLowerCase).collect((Collector<? super Object, ?, List<String>>)Collectors.toList());
        final ImmutableList.Builder<String> unexpectedColumnListBuilder = (ImmutableList.Builder<String>)ImmutableList.builder();
        for (final JdbcToFieldMapping mapping : jdbcMappings) {
            final String columnName = mapping.getField().getName().toLowerCase(Locale.ROOT);
            if (!expectedColumns.contains(columnName) && !this.skippedColumns.contains(columnName)) {
                JdbcRecordReader.logger.debug("Unrecognized column name found: {}.", (Object)columnName);
                unexpectedColumnListBuilder.add((Object)columnName);
            }
        }
        return (List<String>)unexpectedColumnListBuilder.build();
    }
    
    @VisibleForTesting
    public void throwInvalidMetadataError(final String message) {
        throw UserException.invalidMetadataError().addContext(message).setAdditionalExceptionContext((AdditionalExceptionContext)new InvalidMetadataErrorContext((List)ImmutableList.copyOf((Collection)this.tableList))).buildSilently();
    }
    
    @VisibleForTesting
    static BigDecimal processDecimal(final BigDecimal originalValue, final int vectorScale, final int originalValueScale) {
        final BigDecimal scaledDecimalValue = getScaledDecimalValue(originalValue, vectorScale, originalValueScale);
        final int finalValuePrecision = scaledDecimalValue.precision();
        if (finalValuePrecision > 38) {
            throw new IllegalArgumentException(String.format("Received a Decimal value with precision %d that exceeds the maximum supported precision of %d. Please try adding an explicit cast or reducing the volume of data processed in the query.", finalValuePrecision, 38));
        }
        return scaledDecimalValue;
    }
    
    private static BigDecimal getScaledDecimalValue(final BigDecimal originalValue, final int vectorScale, final int originalValueScale) {
        if (originalValueScale < vectorScale) {
            return originalValue.setScale(vectorScale);
        }
        if (originalValueScale > vectorScale) {
            return originalValue.setScale(vectorScale, 5);
        }
        return originalValue;
    }
    
    private static int getTrimmedSize(final byte[] record, final byte b) {
        for (int i = record.length; i > 0; --i) {
            if (record[i - 1] != b) {
                return i;
            }
        }
        return 0;
    }
    
    @VisibleForTesting
    static ZonedDateTime treatAsUTC(final Timestamp timestamp) {
        final ZoneId zoneSystemDefault = ZoneId.systemDefault();
        final LocalDateTime localDateTime = timestamp.toLocalDateTime();
        final ZonedDateTime zonedUTC = ZonedDateTime.ofLocal(localDateTime, ZoneId.of("UTC"), null);
        return zonedUTC.withZoneSameInstant(zoneSystemDefault);
    }
    
    static {
        DISABLE_TRIM_ON_CHARS = Boolean.getBoolean("dremio.jdbc.mssql.trim-on-chars.disable");
        logger = LoggerFactory.getLogger((Class)JdbcRecordReader.class);
    }
    
    private abstract static class Copier<T extends ValueVector>
    {
        private final int columnIndex;
        private final ResultSet result;
        private final T valueVector;
        
        Copier(final int columnIndex, final ResultSet result, final T valueVector) {
            this.columnIndex = columnIndex;
            this.result = result;
            this.valueVector = valueVector;
        }
        
        abstract void copy(final int p0) throws SQLException;
        
        protected int getColumnIndex() {
            return this.columnIndex;
        }
        
        protected ResultSet getResult() {
            return this.result;
        }
        
        protected T getValueVector() {
            return this.valueVector;
        }
    }
    
    private static class IntCopier extends Copier<IntVector>
    {
        IntCopier(final int offset, final ResultSet set, final IntVector vector) {
            super(offset, set, (ValueVector)vector);
        }
        
        @Override
        void copy(final int index) throws SQLException {
            this.getValueVector().setSafe(index, this.getResult().getInt(this.getColumnIndex()));
            if (this.getResult().wasNull()) {
                this.getValueVector().setNull(index);
            }
        }
    }
    
    private static class BigIntCopier extends Copier<BigIntVector>
    {
        BigIntCopier(final int offset, final ResultSet set, final BigIntVector vector) {
            super(offset, set, (ValueVector)vector);
        }
        
        @Override
        void copy(final int index) throws SQLException {
            this.getValueVector().setSafe(index, this.getResult().getLong(this.getColumnIndex()));
            if (this.getResult().wasNull()) {
                this.getValueVector().setNull(index);
            }
        }
    }
    
    private static class Float4Copier extends Copier<Float4Vector>
    {
        Float4Copier(final int columnIndex, final ResultSet result, final Float4Vector vector) {
            super(columnIndex, result, (ValueVector)vector);
        }
        
        @Override
        void copy(final int index) throws SQLException {
            this.getValueVector().setSafe(index, this.getResult().getFloat(this.getColumnIndex()));
            if (this.getResult().wasNull()) {
                this.getValueVector().setNull(index);
            }
        }
    }
    
    private static class Float8Copier extends Copier<Float8Vector>
    {
        Float8Copier(final int columnIndex, final ResultSet result, final Float8Vector vector) {
            super(columnIndex, result, (ValueVector)vector);
        }
        
        @Override
        void copy(final int index) throws SQLException {
            this.getValueVector().setSafe(index, this.getResult().getDouble(this.getColumnIndex()));
            if (this.getResult().wasNull()) {
                this.getValueVector().setNull(index);
            }
        }
    }
    
    private static class DecimalCopier extends Copier<DecimalVector>
    {
        DecimalCopier(final int columnIndex, final ResultSet result, final DecimalVector vector) {
            super(columnIndex, result, (ValueVector)vector);
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final BigDecimal originalValue = this.getResult().getBigDecimal(this.getColumnIndex());
            if (originalValue != null) {
                final int vectorScale = this.getValueVector().getScale();
                final int originalValueScale = originalValue.scale();
                try {
                    this.getValueVector().setSafe(index, JdbcRecordReader.processDecimal(originalValue, vectorScale, originalValueScale));
                }
                catch (UnsupportedOperationException e) {
                    throw new IllegalArgumentException(String.format("Expected a Decimal value with precision %d and scale %d but received a value with precision %d and scale %d. Please try adding an explicit cast to your query.", this.getValueVector().getPrecision(), vectorScale, originalValue.precision(), originalValueScale), e);
                }
            }
        }
    }
    
    private static class VarCharCopier extends Copier<VarCharVector>
    {
        private final int maxCellSize;
        
        VarCharCopier(final int maxCellSize, final int columnIndex, final ResultSet result, final VarCharVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.maxCellSize = maxCellSize;
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final String val = this.getResult().getString(this.getColumnIndex());
            if (val != null) {
                final byte[] record = val.getBytes(Charsets.UTF_8);
                FieldSizeLimitExceptionHelper.checkSizeLimit(record.length, this.maxCellSize, this.getColumnIndex(), JdbcRecordReader.logger);
                this.getValueVector().setSafe(index, record, 0, record.length);
            }
        }
    }
    
    private static class VarCharTrimCopier extends Copier<VarCharVector>
    {
        private final int maxCellSize;
        
        VarCharTrimCopier(final int maxCellSize, final int columnIndex, final ResultSet result, final VarCharVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.maxCellSize = maxCellSize;
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final String val = this.getResult().getString(this.getColumnIndex());
            if (val != null) {
                final byte[] record = val.getBytes(Charsets.UTF_8);
                final int trimmedSize = getTrimmedSize(record, (byte)32);
                FieldSizeLimitExceptionHelper.checkSizeLimit(trimmedSize, this.maxCellSize, this.getColumnIndex(), JdbcRecordReader.logger);
                this.getValueVector().setSafe(index, record, 0, trimmedSize);
            }
        }
    }
    
    private static class VarBinaryCopier extends Copier<VarBinaryVector>
    {
        private final int maxCellSize;
        
        VarBinaryCopier(final int maxCellSize, final int columnIndex, final ResultSet result, final VarBinaryVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.maxCellSize = maxCellSize;
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final byte[] record = this.getResult().getBytes(this.getColumnIndex());
            if (record != null) {
                FieldSizeLimitExceptionHelper.checkSizeLimit(record.length, this.maxCellSize, this.getColumnIndex(), JdbcRecordReader.logger);
                this.getValueVector().setSafe(index, record, 0, record.length);
            }
        }
    }
    
    private static class DateCopier extends Copier<DateMilliVector>
    {
        private final Calendar calendar;
        
        DateCopier(final int columnIndex, final ResultSet result, final DateMilliVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final Date date = this.getResult().getDate(this.getColumnIndex(), this.calendar);
            if (date != null) {
                this.getValueVector().setSafe(index, date.getTime());
            }
        }
    }
    
    private static class DateTimeZoneAdjustmentCopier extends Copier<DateMilliVector>
    {
        private final Calendar calendar;
        
        DateTimeZoneAdjustmentCopier(final int columnIndex, final ResultSet result, final DateMilliVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final Date date = this.getResult().getDate(this.getColumnIndex(), this.calendar);
            if (date != null) {
                this.getValueVector().setSafe(index, date.getTime() + TimeZone.getDefault().getOffset(date.getTime()));
            }
        }
    }
    
    private static class TimeCopier extends Copier<TimeMilliVector>
    {
        private final Calendar calendar;
        
        TimeCopier(final int columnIndex, final ResultSet result, final TimeMilliVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final Time time = this.getResult().getTime(this.getColumnIndex(), this.calendar);
            if (time != null) {
                this.getValueVector().setSafe(index, (int)time.getTime());
            }
        }
    }
    
    private static class TimeCopierCoerceToUTC extends Copier<TimeMilliVector>
    {
        private final Calendar calendar;
        
        TimeCopierCoerceToUTC(final int columnIndex, final ResultSet result, final TimeMilliVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final Timestamp stamp = this.getResult().getTimestamp(this.getColumnIndex(), this.calendar);
            if (stamp != null) {
                final LocalTime localTime = JdbcRecordReader.treatAsUTC(stamp).toLocalTime();
                this.getValueVector().setSafe(index, (int)(Time.valueOf(localTime).getTime() + localTime.getNano() / 1000000));
            }
        }
    }
    
    private static class TimeStampCopier extends Copier<TimeStampMilliVector>
    {
        private final Calendar calendar;
        
        TimeStampCopier(final int columnIndex, final ResultSet result, final TimeStampMilliVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final Timestamp stamp = this.getResult().getTimestamp(this.getColumnIndex(), this.calendar);
            if (stamp != null) {
                this.getValueVector().setSafe(index, stamp.getTime());
            }
        }
    }
    
    private static class TimeStampCopierCoerceToUTC extends Copier<TimeStampMilliVector>
    {
        private final Calendar calendar;
        
        TimeStampCopierCoerceToUTC(final int columnIndex, final ResultSet result, final TimeStampMilliVector vector) {
            super(columnIndex, result, (ValueVector)vector);
            this.calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        }
        
        @Override
        void copy(final int index) throws SQLException {
            final Timestamp stamp = this.getResult().getTimestamp(this.getColumnIndex(), this.calendar);
            if (stamp != null) {
                final Timestamp newTimestamp = Timestamp.valueOf(JdbcRecordReader.treatAsUTC(stamp).toLocalDateTime());
                this.getValueVector().setSafe(index, newTimestamp.getTime());
            }
        }
    }
    
    private static class BitCopier extends Copier<BitVector>
    {
        BitCopier(final int columnIndex, final ResultSet result, final BitVector vector) {
            super(columnIndex, result, (ValueVector)vector);
        }
        
        @Override
        void copy(final int index) throws SQLException {
            this.getValueVector().setSafe(index, (int)(this.getResult().getBoolean(this.getColumnIndex()) ? 1 : 0));
            if (this.getResult().wasNull()) {
                this.getValueVector().setNull(index);
            }
        }
    }
}
