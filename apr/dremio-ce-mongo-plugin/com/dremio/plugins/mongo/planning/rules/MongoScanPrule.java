package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.expr.fn.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.plan.*;
import java.util.*;
import com.dremio.common.expression.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.plugins.mongo.planning.rels.*;
import org.apache.calcite.rel.*;

public class MongoScanPrule extends RelOptRule
{
    private final FunctionLookupContext lookupContext;
    
    public MongoScanPrule(final FunctionLookupContext lookupContext) {
        super(RelOptHelper.any((Class)MongoScanDrel.class), "MongoScanPrule");
        this.lookupContext = lookupContext;
    }
    
    public void onMatch(final RelOptRuleCall call) {
        final MongoScanDrel logicalScan = (MongoScanDrel)call.rel(0);
        final MongoIntermediateScanPrel physicalScan = new MongoIntermediateScanPrel(logicalScan.getCluster(), logicalScan.getTraitSet().replace((RelTrait)MongoConvention.INSTANCE), logicalScan.getTable(), logicalScan.getTableMetadata(), logicalScan.getProjectedColumns(), logicalScan.getObservedRowcountAdjustment());
        final RelNode converted = (RelNode)new MongoIntermediatePrel(physicalScan.getTraitSet().replace((RelTrait)Prel.PHYSICAL), (RelNode)physicalScan, this.lookupContext, physicalScan, physicalScan.getPluginId());
        call.transformTo(converted);
    }
}
