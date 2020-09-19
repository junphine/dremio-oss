package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.planner.logical.*;
import com.google.common.collect.*;
import com.dremio.exec.store.jdbc.rel.*;
import java.util.*;
import org.apache.calcite.plan.*;
import org.slf4j.*;

public final class JdbcMinusRule extends JdbcBinaryConverterRule
{
    private static final Logger logger;
    public static final JdbcMinusRule INSTANCE;
    
    private JdbcMinusRule() {
        super((Class<? extends RelNode>)LogicalMinus.class, "JdbcMinusRule");
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel left, final JdbcCrel right, final StoragePluginId pluginId) {
        final LogicalMinus minus = (LogicalMinus)rel;
        if (minus.all) {
            JdbcMinusRule.logger.debug("EXCEPT All used but is not permitted to be pushed down. Aborting pushdown.");
            return null;
        }
        return (RelNode)new JdbcMinus(rel.getCluster(), rel.getTraitSet().replace((RelTrait)Rel.LOGICAL), (List<RelNode>)ImmutableList.of(left.getInput(), right.getInput()), false, pluginId);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcMinusRule.class);
        INSTANCE = new JdbcMinusRule();
    }
}
