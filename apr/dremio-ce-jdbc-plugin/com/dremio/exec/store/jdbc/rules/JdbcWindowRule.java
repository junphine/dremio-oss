package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.rel.*;
import java.util.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.store.jdbc.legacy.*;
import org.slf4j.*;
import com.dremio.exec.planner.logical.*;

public final class JdbcWindowRule extends JdbcUnaryConverterRule
{
    private static final Logger logger;
    public static final JdbcWindowRule INSTANCE;
    
    private JdbcWindowRule(final Class<? extends Window> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final Window window = (Window)rel;
        return (RelNode)new JdbcWindow(rel.getCluster(), rel.getTraitSet().replace((RelTrait)Rel.LOGICAL), crel.getInput(), window.getConstants(), window.getRowType(), (List<Window.Group>)window.groups, pluginId);
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final Window window = (Window)call.rel(0);
        final JdbcCrel jdbcCrel = (JdbcCrel)call.rel(1);
        final StoragePluginId pluginId = jdbcCrel.getPluginId();
        if (pluginId == null) {
            return true;
        }
        final JdbcDremioSqlDialect dialect = JdbcConverterRule.getDialect(pluginId);
        if (!dialect.supportsOver(window)) {
            JdbcWindowRule.logger.debug("Encountered unsupported OVER clause in window function. Aborting pushdown.");
            return false;
        }
        return true;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcWindowRule.class);
        INSTANCE = new JdbcWindowRule((Class<? extends Window>)WindowRel.class, "JdbcWindowRuleDrel");
    }
}
