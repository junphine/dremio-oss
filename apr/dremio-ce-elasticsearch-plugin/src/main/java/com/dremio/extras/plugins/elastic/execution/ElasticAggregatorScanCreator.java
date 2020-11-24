package com.dremio.extras.plugins.elastic.execution;

import com.dremio.sabot.op.spi.*;
import com.dremio.extras.plugins.elastic.planning.*;
import com.dremio.sabot.exec.fragment.*;
import com.dremio.sabot.exec.context.*;
import com.dremio.exec.store.*;
import com.dremio.common.expression.*;
import com.google.common.collect.*;
import com.google.common.base.*;
import com.dremio.service.namespace.dataset.proto.*;
import com.dremio.sabot.op.scan.*;
import java.util.*;
import com.dremio.common.exceptions.*;
import com.google.protobuf.*;
import com.dremio.plugins.elastic.planning.*;
import com.dremio.plugins.elastic.*;
import com.dremio.exec.physical.base.*;

public class ElasticAggregatorScanCreator implements ProducerOperator.Creator<ElasticsearchAggregatorSubScan>
{
    public ProducerOperator create(final FragmentExecutionContext fec, final OperatorContext context, final ElasticsearchAggregatorSubScan subScan) throws ExecutionSetupException {
        try {
            final ElasticsearchStoragePlugin plugin = fec.getStoragePlugin(subScan.getPluginId());
            final List<RecordReader> readers = new ArrayList<RecordReader>();
            final ElasticsearchScanSpec spec = subScan.getScanSpec();
            if (subScan.getSplits().isEmpty()) {
                readers.add(new ElasticsearchAggregatorReader(context, plugin.getConfig(), spec, null, plugin.getConnection(ImmutableList.of()), subScan.getAggregates(), subScan.getColumns(), subScan.getFullSchema(), Iterables.getOnlyElement(subScan.getReferencedTables())));
            }
            else {
                for (final SplitAndPartitionInfo split : subScan.getSplits()) {
                    final ElasticConnectionPool.ElasticConnection connection = plugin.getConnection(FluentIterable.from(split.getDatasetSplitInfo().getAffinitiesList()).transform(new Function<PartitionProtobuf.Affinity, String>() {
                        public String apply(final PartitionProtobuf.Affinity input) {
                            return input.getHost();
                        }
                    }));
                    readers.add((RecordReader)new ElasticsearchAggregatorReader(context, plugin.getConfig(), spec, split, connection, subScan.getAggregates(), subScan.getColumns(), subScan.getFullSchema(), Iterables.getOnlyElement(subScan.getReferencedTables())));
                }
            }
            return new ScanOperator(subScan, context, readers.iterator());
        }
        catch (InvalidProtocolBufferException e) {
            throw new ExecutionSetupException(e);
        }
    }
}
