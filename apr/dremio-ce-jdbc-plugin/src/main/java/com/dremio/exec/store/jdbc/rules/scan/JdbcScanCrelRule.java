package com.dremio.exec.store.jdbc.rules.scan;

import com.dremio.exec.store.common.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.store.jdbc.rel.*;
import java.util.*;
import com.dremio.common.expression.*;
import com.dremio.exec.calcite.logical.*;
import org.apache.calcite.rel.*;

public class JdbcScanCrelRule extends SourceLogicalConverter
{
    public JdbcScanCrelRule(final SourceType type) {
        super(type);
    }
    
    public Rel convertScan(final ScanCrel scan) {
        final JdbcTableScan tableScan = new JdbcTableScan(scan.getCluster(), scan.getTraitSet().replace((RelTrait)Rel.LOGICAL), scan.getTable(), scan.getPluginId(), scan.getTableMetadata(), scan.getProjectedColumns(), scan.getObservedRowcountAdjustment(), scan.isDirectNamespaceDescendent());
        return (Rel)new JdbcCrel(tableScan.getCluster(), scan.getTraitSet().replace((RelTrait)Rel.LOGICAL), (RelNode)tableScan, scan.getPluginId());
    }
}
