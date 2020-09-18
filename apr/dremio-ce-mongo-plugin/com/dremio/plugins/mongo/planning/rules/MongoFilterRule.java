package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.planner.physical.*;
import com.dremio.exec.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rel.*;
import com.dremio.plugins.mongo.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.plan.*;
import com.dremio.plugins.mongo.planning.rels.*;
import org.apache.calcite.rex.*;
import com.dremio.service.namespace.capabilities.*;
import com.dremio.exec.record.*;
import com.dremio.options.*;

public class MongoFilterRule extends AbstractMongoConverterRule<FilterPrel>
{
    public static final MongoFilterRule INSTANCE;
    
    private MongoFilterRule() {
        super(FilterPrel.class, "MongoFilterRule", ExecConstants.MONGO_RULES_FILTER, false);
    }
    
    @Override
    public MongoRel convert(final RelOptRuleCall call, final FilterPrel filter, final StoragePluginId pluginId, final RelNode inputToFilterRel) {
        final SourceCapabilities capabilities = pluginId.getCapabilities();
        final boolean isMongo3_6Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_6_FEATURES);
        final BatchSchema schema = ((MongoIntermediatePrel)call.rel(1)).getScan().getBatchSchema();
        if (isMongo3_6Enabled) {
            try {
                final FindQueryGenerator generator = new FindQueryGenerator(schema, filter.getRowType());
                filter.getCondition().accept((RexVisitor)generator);
                return new MongoFilter(filter.getTraitSet().replace((RelTrait)MongoConvention.INSTANCE), inputToFilterRel, filter.getCondition(), generator.needsCollation);
            }
            catch (Exception e) {
                return null;
            }
        }
        final boolean isMongo3_2Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
        final boolean isMongo3_4Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);
        final ProjectExtractor projectExtractor = new ProjectExtractor(filter.getRowType(), filter.getCluster().getTypeFactory(), isMongo3_2Enabled, isMongo3_4Enabled);
        final RexNode newFilterCond = (RexNode)filter.getCondition().accept((RexVisitor)projectExtractor);
        final MatchExpressionConverter expressionConverter = new MatchExpressionConverter(schema, projectExtractor.getNewRecordType(), isMongo3_2Enabled, isMongo3_4Enabled);
        newFilterCond.accept((RexVisitor)expressionConverter);
        final boolean needsCollation = isMongo3_4Enabled && expressionConverter.needsCollation();
        if (projectExtractor.hasNewProjects()) {
            return null;
        }
        return new MongoFilter(filter.getTraitSet().replace((RelTrait)MongoConvention.INSTANCE), inputToFilterRel, newFilterCond, needsCollation);
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final MongoIntermediatePrel prel = (MongoIntermediatePrel)call.rel(1);
        final SourceCapabilities capabilities = prel.getPluginId().getCapabilities();
        final boolean isMongo3_6Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_6_FEATURES);
        final FilterPrel filter = (FilterPrel)call.rel(0);
        final BatchSchema schema = prel.getScan().getBatchSchema();
        if (isMongo3_6Enabled) {
            try {
                filter.getCondition().accept((RexVisitor)new FindQueryGenerator(schema, filter.getRowType()));
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }
        final boolean isMongo3_2Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_2_FEATURES);
        final boolean isMongo3_4Enabled = capabilities.getCapability(MongoStoragePlugin.MONGO_3_4_FEATURES);
        try {
            final ProjectExtractor projectExtractor = new ProjectExtractor(filter.getRowType(), filter.getCluster().getTypeFactory(), isMongo3_2Enabled, isMongo3_4Enabled);
            if (projectExtractor.hasNewProjects()) {
                return false;
            }
            final RexNode rexNode = (RexNode)filter.getCondition().accept((RexVisitor)projectExtractor);
            rexNode.accept((RexVisitor)new MatchExpressionConverter(schema, projectExtractor.getNewRecordType(), isMongo3_2Enabled, isMongo3_4Enabled));
        }
        catch (Exception e2) {
            return false;
        }
        return true;
    }
    
    static {
        INSTANCE = new MongoFilterRule();
    }
}
