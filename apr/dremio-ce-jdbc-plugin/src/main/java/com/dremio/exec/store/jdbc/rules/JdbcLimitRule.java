package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.core.*;
import com.dremio.exec.catalog.*;
import java.util.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.slf4j.*;

public final class JdbcLimitRule extends JdbcUnaryConverterRule
{
    private static final Logger logger;
    public static final JdbcLimitRule INSTANCE;
    
    private JdbcLimitRule() {
        super((Class<? extends RelNode>)LimitRel.class, "JdbcLimitRuleDrel");
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final LimitRel limit = (LimitRel)call.rel(0);
        final JdbcCrel crel = (JdbcCrel)call.rel(1);
        final StoragePluginId pluginId = crel.getPluginId();
        if (pluginId == null) {
            return true;
        }
        JdbcLimitRule.logger.debug("Checking if Limit node {} is supported using supportsSort().", limit);
        final List<RelCollation> collationList = (List<RelCollation>)limit.getTraitSet().getTraits((RelTraitDef)RelCollationTraitDef.INSTANCE);
        final RelCollation collation = (collationList.size() == 0) ? RelCollations.EMPTY : collationList.get(0);
        final boolean isSortSupported = JdbcConverterRule.getDialect(pluginId).supportsSort((Sort)LogicalSort.create((RelNode)crel, collation, limit.getOffset(), limit.getFetch()));
        if (!isSortSupported) {
            JdbcLimitRule.logger.debug("Limit '{}' is unsupported. Aborting pushdown.", limit);
            return false;
        }
        JdbcLimitRule.logger.debug("Limit '{}' is supported.", limit);
        return true;
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final LimitRel limit = (LimitRel)rel;
        return (RelNode)new JdbcSort(rel.getCluster(), limit.getTraitSet().replace((RelTrait)Rel.LOGICAL).replace((RelTrait)RelCollations.EMPTY), crel.getInput(), RelCollations.EMPTY, limit.getOffset(), limit.getFetch(), pluginId);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcLimitRule.class);
        INSTANCE = new JdbcLimitRule();
    }
}
