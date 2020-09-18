package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.plan.*;
import com.dremio.common.expression.*;
import org.apache.calcite.sql.type.*;
import java.util.concurrent.*;
import com.dremio.exec.store.jdbc.legacy.*;
import java.util.*;
import org.slf4j.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;

public final class JdbcProjectRule extends JdbcUnaryConverterRule
{
    private static final Logger logger;
    public static final JdbcProjectRule CALCITE_INSTANCE;
    public static final JdbcProjectRule LOGICAL_INSTANCE;
    
    private JdbcProjectRule(final Class<? extends Project> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final Project project = (Project)rel;
        return (RelNode)new JdbcProject(rel.getCluster(), rel.getTraitSet().replace((RelTrait)Rel.LOGICAL), crel.getInput(), project.getProjects(), project.getRowType(), pluginId);
    }
    
    public boolean matches(final RelOptRuleCall call) {
        try {
            final Project project = (Project)call.rel(0);
            final JdbcCrel crel = (JdbcCrel)call.rel(1);
            final StoragePluginId pluginId = crel.getPluginId();
            if (pluginId == null) {
                return true;
            }
            if (UnpushableTypeVisitor.hasUnpushableTypes((RelNode)project, project.getChildExps())) {
                JdbcProjectRule.logger.debug("Project has expressions with types that are not pushable. Aborting pushdown.");
                return false;
            }
            final RuleContext ruleContext = JdbcConverterRule.getRuleContext(pluginId, crel.getCluster().getRexBuilder());
            final JdbcDremioSqlDialect dialect = JdbcConverterRule.getDialect(pluginId);
            final boolean supportsBitLiteral = dialect.supportsLiteral(CompleteType.BIT);
            for (final RexNode node : project.getChildExps()) {
                if (!(boolean)ruleContext.getSupportedExpressions().get((Object)node)) {
                    return false;
                }
                if (!supportsBitLiteral && dialect.hasBooleanLiteralOrRexCallReturnsBoolean(node, false)) {
                    JdbcProjectRule.logger.debug("Dialect does not support booleans, and an expression which returned a boolean was projected.Aborting pushdown.");
                    return false;
                }
                if (SqlTypeName.DAY_INTERVAL_TYPES.contains(node.getType().getSqlTypeName()) || SqlTypeName.YEAR_INTERVAL_TYPES.contains(node.getType().getSqlTypeName())) {
                    JdbcProjectRule.logger.debug("Intervals are currently unsupported for projection by the JDBC plugin.");
                    return false;
                }
            }
            for (final RexNode node : project.getChildExps()) {
                if (!(boolean)ruleContext.getOverCheckedExpressions().get((Object)node)) {
                    JdbcProjectRule.logger.debug("Encountered unsupported OVER clause in window function. Aborting pushdown.");
                    return false;
                }
            }
            return true;
        }
        catch (ExecutionException e) {
            throw new IllegalStateException("Failure while trying to evaluate pushdown.", e);
        }
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcProjectRule.class);
        CALCITE_INSTANCE = new JdbcProjectRule((Class<? extends Project>)LogicalProject.class, "JdbcProjectRuleCrel");
        LOGICAL_INSTANCE = new JdbcProjectRule((Class<? extends Project>)ProjectRel.class, "JdbcProjectRuleDrel");
    }
}
