package com.dremio.plugins.mongo.execution;

import com.dremio.sabot.op.spi.*;
import com.dremio.sabot.exec.fragment.*;
import com.dremio.sabot.exec.context.*;
import com.dremio.plugins.mongo.*;
import com.google.common.collect.*;
import com.google.common.base.*;
import com.dremio.plugins.mongo.planning.*;
import com.dremio.plugins.mongo.connection.*;
import com.dremio.common.expression.*;
import com.dremio.exec.store.*;
import com.dremio.sabot.op.scan.*;
import java.util.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.physical.base.*;

public class MongoScanCreator implements ProducerOperator.Creator<MongoSubScan>
{
    public ProducerOperator create(final FragmentExecutionContext fec, final OperatorContext context, final MongoSubScan subScan) throws ExecutionSetupException {
        final List<SchemaPath> columns = (List<SchemaPath>)((subScan.getColumns() == null) ? GroupScan.ALL_COLUMNS : subScan.getColumns());
        final int batchSize = context.getTargetBatchSize();
        Preconditions.checkArgument(!subScan.isSingleFragment() || subScan.getChunkScanSpecList().size() == 1);
        final boolean isSingleFragment = subScan.isSingleFragment();
        final MongoStoragePlugin plugin = (MongoStoragePlugin)fec.getStoragePlugin(subScan.getPluginId());
        final MongoConnectionManager manager = plugin.getManager();
        final Iterable<RecordReader> readers = (Iterable<RecordReader>)FluentIterable.from((Iterable)subScan.getChunkScanSpecList()).transform((Function)new Function<MongoSubScanSpec, RecordReader>() {
            public RecordReader apply(final MongoSubScanSpec scanSpec) {
                final MongoRecordReader innerReader = new MongoRecordReader(manager, context, scanSpec, columns, subScan.getSanitizedColumns(), context.getManagedBuffer(), isSingleFragment, batchSize, subScan.getFullSchema());
                if (!scanSpec.getPipeline().needsCoercion()) {
                    return (RecordReader)innerReader;
                }
                return (RecordReader)new CoercionReader(context, columns, (RecordReader)innerReader, subScan.getFullSchema());
            }
        });
        return (ProducerOperator)new ScanOperator((SubScan)subScan, context, (Iterator)readers.iterator());
    }
}
