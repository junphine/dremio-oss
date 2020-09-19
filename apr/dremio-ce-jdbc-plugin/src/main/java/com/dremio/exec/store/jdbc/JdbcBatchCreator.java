package com.dremio.exec.store.jdbc;

import com.dremio.sabot.op.spi.*;
import com.dremio.sabot.exec.fragment.*;
import com.dremio.sabot.exec.context.*;
import com.google.common.util.concurrent.*;
import com.dremio.exec.store.*;
import com.dremio.sabot.op.scan.*;
import java.util.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.physical.base.*;

public class JdbcBatchCreator implements ProducerOperator.Creator<JdbcSubScan>
{
    public ProducerOperator create(final FragmentExecutionContext fragmentExecContext, final OperatorContext context, final JdbcSubScan subScan) throws ExecutionSetupException {
        final JdbcStoragePlugin plugin = (JdbcStoragePlugin)fragmentExecContext.getStoragePlugin(subScan.getPluginId());
        final JdbcRecordReader innerReader = new JdbcRecordReader(context, plugin.getSource(), subScan.getSql(), plugin.getName(), subScan.getColumns(), plugin.getConfig().getFetchSize(), (ListenableFuture<Boolean>)fragmentExecContext.cancelled(), subScan.getPluginId().getCapabilities(), plugin.getDialect().getDataTypeMapper(), subScan.getReferencedTables(), subScan.getSkippedColumns());
        final CoercionReader reader = new CoercionReader(context, (List)subScan.getColumns(), (RecordReader)innerReader, subScan.getFullSchema());
        return (ProducerOperator)new ScanOperator((SubScan)subScan, context, (Iterator)Collections.singletonList(reader).iterator());
    }
}
