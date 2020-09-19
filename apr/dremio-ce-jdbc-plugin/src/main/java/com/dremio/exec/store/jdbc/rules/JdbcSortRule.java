package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.slf4j.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;

public final class JdbcSortRule extends JdbcUnaryConverterRule
{
    private static final Logger logger;
    public static final JdbcSortRule CALCITE_INSTANCE;
    public static final JdbcSortRule LOGICAL_INSTANCE;
    
    private JdbcSortRule(final Class<? extends Sort> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final Sort sort = (Sort)call.rel(0);
        final JdbcCrel crel = (JdbcCrel)call.rel(1);
        final StoragePluginId pluginId = crel.getPluginId();
        if (pluginId == null) {
            return true;
        }
        JdbcSortRule.logger.debug("Checking if Sort node {} is supported using supportsSort().", sort);
        final boolean isSortSupported = JdbcConverterRule.getDialect(pluginId).supportsSort(sort);
        if (!isSortSupported) {
            JdbcSortRule.logger.debug("Sort '{}' is unsupported. Aborting pushdown.", sort);
            return false;
        }
        JdbcSortRule.logger.debug("Sort '{}' is supported.", sort);
        return true;
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final Sort sort = (Sort)rel;
        return (RelNode)new JdbcSort(rel.getCluster(), sort.getTraitSet().replace((RelTrait)Rel.LOGICAL), crel.getInput(), sort.getCollation(), sort.offset, sort.fetch, pluginId);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcSortRule.class);
        CALCITE_INSTANCE = new JdbcSortRule((Class<? extends Sort>)LogicalSort.class, "JdbcSortRuleCrel");
        LOGICAL_INSTANCE = new JdbcSortRule((Class<? extends Sort>)SortRel.class, "JdbcSortRuleDrel");
    }
}
