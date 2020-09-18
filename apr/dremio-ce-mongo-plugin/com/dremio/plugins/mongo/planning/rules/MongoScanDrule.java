package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.store.common.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.plugins.mongo.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;
import com.dremio.plugins.mongo.planning.rels.*;
import java.util.*;
import com.dremio.common.expression.*;

public class MongoScanDrule extends SourceLogicalConverter
{
    public static final MongoScanDrule INSTANCE;
    
    private MongoScanDrule() {
        super((SourceType)MongoConf.class.getAnnotation(SourceType.class));
    }
    
    public Rel convertScan(final ScanCrel scan) {
        return (Rel)new MongoScanDrel(scan.getCluster(), scan.getTraitSet().plus((RelTrait)Rel.LOGICAL), scan.getTable(), scan.getPluginId(), scan.getTableMetadata(), scan.getProjectedColumns(), MongoColumnNameSanitizer.sanitizeColumnNames(scan.getRowType()).getFieldNames(), scan.getObservedRowcountAdjustment());
    }
    
    static {
        INSTANCE = new MongoScanDrule();
    }
}
