package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.planner.physical.*;
import org.slf4j.*;
import org.apache.calcite.rel.*;
import java.util.*;
import com.dremio.plugins.mongo.planning.*;
import org.apache.calcite.plan.*;
import com.dremio.plugins.mongo.planning.rels.*;
import com.dremio.exec.catalog.*;
import com.dremio.options.*;

abstract class AbstractMongoConverterRule<R extends RelNode> extends RuleWithOption
{
    protected final Logger logger;
    private final TypeValidators.BooleanValidator isRuleEnabledValidator;
    private final boolean checkCollationFilter;
    
    protected AbstractMongoConverterRule(final Class<R> clazz, final String description, final TypeValidators.BooleanValidator isRuleEnabledValidator, final boolean checkCollationFilter) {
        this(clazz, Prel.PHYSICAL, description, isRuleEnabledValidator, checkCollationFilter);
    }
    
    protected AbstractMongoConverterRule(final Class<R> clazz, final Convention inputConvention, final String description, final TypeValidators.BooleanValidator isRuleEnabledValidator, final boolean checkCollationFilter) {
        super(operand((Class)clazz, (RelTrait)inputConvention, some(operand((Class)MongoIntermediatePrel.class, any()), new RelOptRuleOperand[0])), description);
        this.logger = LoggerFactory.getLogger((Class)this.getClass());
        this.isRuleEnabledValidator = isRuleEnabledValidator;
        this.checkCollationFilter = checkCollationFilter;
    }
    
    static boolean sortAllowed(final RelCollation collation) {
        for (final RelFieldCollation c : collation.getFieldCollations()) {
            if ((c.direction == RelFieldCollation.Direction.ASCENDING && c.nullDirection == RelFieldCollation.NullDirection.LAST) || (c.direction == RelFieldCollation.Direction.DESCENDING && c.nullDirection == RelFieldCollation.NullDirection.FIRST) || c.direction == RelFieldCollation.Direction.CLUSTERED) {
                return false;
            }
        }
        return true;
    }
    
    public static RelTraitSet withMongo(final RelNode node) {
        return node.getTraitSet().replace((RelTrait)MongoConvention.INSTANCE);
    }
    
    public void onMatch(final RelOptRuleCall call) {
        final R main = (R)call.rel(0);
        if (this.checkCollationFilter && CollationFilterChecker.hasCollationFilter(main)) {
            return;
        }
        final MongoIntermediatePrel child = (MongoIntermediatePrel)call.rel(1);
        final MongoRel rel = this.convert(call, main, child.getPluginId(), child.getInput());
        if (rel != null) {
            call.transformTo((RelNode)child.withNewInput(rel));
        }
    }
    
    public abstract MongoRel convert(final RelOptRuleCall p0, final R p1, final StoragePluginId p2, final RelNode p3);
    
    @Override
    public boolean isEnabled(final OptionManager options) {
        return options == null || options.getOption(this.isRuleEnabledValidator);
    }
}
