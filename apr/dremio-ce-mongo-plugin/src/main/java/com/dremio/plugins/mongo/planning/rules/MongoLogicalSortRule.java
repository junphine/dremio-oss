package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.planner.logical.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.plugins.mongo.planning.rels.*;
import com.dremio.options.*;
import com.dremio.exec.*;

public class MongoLogicalSortRule extends RuleWithOption
{
    public static final MongoLogicalSortRule INSTANCE;
    
    public MongoLogicalSortRule() {
        super(operand(SortRel.class, any()), "MongoLogicalSort");
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final SortRel sort = (SortRel)call.rel(0);
        return !CollationFilterChecker.hasCollationFilter((RelNode)sort) && AbstractMongoConverterRule.sortAllowed(sort.getCollation());
    }
    
    public void onMatch(final RelOptRuleCall call) {
        final SortRel sort = (SortRel)call.rel(0);
        final RelNode newInput = convert(sort.getInput(), sort.getInput().getTraitSet().replace((RelTrait)Prel.PHYSICAL).simplify());
        final MongoLSortPrel newSort = new MongoLSortPrel(sort.getCluster(), newInput.getTraitSet().replace((RelTrait)sort.getCollation()).replace((RelTrait)DistributionTrait.SINGLETON), newInput, sort.getCollation());
        call.transformTo((RelNode)newSort);
    }
    
    @Override
    public boolean isEnabled(final OptionManager options) {
        return options.getOption(ExecConstants.MONGO_RULES_SORT);
    }
    
    static {
        INSTANCE = new MongoLogicalSortRule();
    }
}
