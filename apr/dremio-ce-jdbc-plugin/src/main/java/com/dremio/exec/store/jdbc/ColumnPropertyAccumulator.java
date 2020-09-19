package com.dremio.exec.store.jdbc;

import com.dremio.exec.planner.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.store.jdbc.proto.*;
import com.google.protobuf.*;
import com.dremio.service.namespace.dataset.proto.*;
import java.util.*;
import org.apache.calcite.plan.hep.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.dremio.exec.store.jdbc.rules.*;
import org.slf4j.*;

public class ColumnPropertyAccumulator extends StatelessRelShuttleImpl
{
    private static final Logger logger;
    private final Map<String, Map<String, String>> columnProperties;
    
    public ColumnPropertyAccumulator() {
        this.columnProperties = new HashMap<String, Map<String, String>>();
    }
    
    public RelNode visit(final TableScan scan) {
        final JdbcTableScan tableScan = (JdbcTableScan)scan;
        final ReadDefinition readDefinition = tableScan.getTableMetadata().getReadDefinition();
        if (readDefinition.getExtendedProperty() != null) {
            try {
                final JdbcReaderProto.JdbcTableXattr attrs = JdbcReaderProto.JdbcTableXattr.parseFrom(readDefinition.getExtendedProperty().asReadOnlyByteBuffer());
                for (final JdbcReaderProto.ColumnProperties colProps : attrs.getColumnPropertiesList()) {
                    final Map<String, String> properties = new HashMap<String, String>();
                    for (final JdbcReaderProto.ColumnProperty colProp : colProps.getPropertiesList()) {
                        properties.put(colProp.getKey(), colProp.getValue());
                    }
                    this.columnProperties.put(colProps.getColumnName(), properties);
                }
            }
            catch (InvalidProtocolBufferException ex) {
                ColumnPropertyAccumulator.logger.warn("Unable to get extended properties for table {}.", tableScan.getTableName(), ex);
            }
        }
        return super.visit(scan);
    }
    
    public RelNode visit(RelNode other) {
        if (other instanceof HepRelVertex) {
            other = ((HepRelVertex)other).getCurrentRel();
        }
        if (other instanceof JdbcTableScan) {
            return this.visit((TableScan)other);
        }
        if (other instanceof JdbcIntermediatePrel) {
            return this.visit(((JdbcIntermediatePrel)other).getSubTree());
        }
        return super.visit(other);
    }
    
    public Map<String, Map<String, String>> getColumnProperties() {
        return this.columnProperties;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)UnpushableTypeVisitor.class);
    }
}
