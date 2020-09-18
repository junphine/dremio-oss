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

public final class JdbcIntersectRule extends JdbcBinaryConverterRule
{
    private static final Logger logger;
    public static final JdbcIntersectRule INSTANCE;
    
    private JdbcIntersectRule() {
        super((Class<? extends RelNode>)LogicalIntersect.class, "JdbcIntersectRule");
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel left, final JdbcCrel right, final StoragePluginId pluginId) {
        final LogicalIntersect intersect = (LogicalIntersect)rel;
        if (intersect.all) {
            JdbcIntersectRule.logger.debug("Intersect All used but is not permitted to be pushed down. Aborting pushdown.");
            return null;
        }
        return (RelNode)new JdbcIntersect(rel.getCluster(), intersect.getTraitSet().replace((RelTrait)Rel.LOGICAL), (List<RelNode>)ImmutableList.of((Object)left.getInput(), (Object)right.getInput()), false, pluginId);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcIntersectRule.class);
        INSTANCE = new JdbcIntersectRule();
    }
}
