package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.catalog.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.google.common.collect.*;
import com.dremio.exec.store.jdbc.rel.*;
import java.util.*;
import org.apache.calcite.plan.*;
import org.slf4j.*;
import org.apache.calcite.rel.logical.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.exec.planner.*;
import com.dremio.exec.planner.common.*;
import org.apache.calcite.rel.core.*;

public final class JdbcUnionRule extends JdbcBinaryConverterRule
{
    private static final Logger logger;
    public static final JdbcUnionRule CALCITE_INSTANCE;
    public static final JdbcUnionRule LOGICAL_INSTANCE;
    
    private JdbcUnionRule(final Class<? extends Union> clazz, final String name) {
        super((Class<? extends RelNode>)clazz, name);
    }
    
    public boolean matchImpl(final RelOptRuleCall call) {
        final Union union = (Union)call.rel(0);
        final JdbcCrel leftChild = (JdbcCrel)call.rel(1);
        final StoragePluginId pluginId = leftChild.getPluginId();
        if (pluginId == null) {
            return true;
        }
        final JdbcDremioSqlDialect dialect = JdbcConverterRule.getDialect(pluginId);
        JdbcUnionRule.logger.debug("Checking if Union is supported on dialect using supportsUnion{}", (Object)(union.all ? "All" : ""));
        final boolean supportsUnionClause = (union.all && dialect.supportsUnionAll()) || dialect.supportsUnion();
        if (!supportsUnionClause) {
            JdbcUnionRule.logger.debug("Union operation wasn't supported. Aborting pushdown.");
            return false;
        }
        if (dialect.supportsFetchOffsetInSetOperand()) {
            JdbcUnionRule.logger.debug("Union operation supported. Pushing down Union.");
            return true;
        }
        JdbcUnionRule.logger.debug("Dialect does not support using Sort in set operands. Scanning for a Sort.");
        final RelNode subtree = union.accept((RelShuttle)new MoreRelOptUtil.SubsetRemover());
        final SortDetector sortDetector = new SortDetector((Union)subtree);
        subtree.accept((RelShuttle)sortDetector);
        return !sortDetector.hasFoundSort();
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel left, final JdbcCrel right, final StoragePluginId pluginId) {
        final Union union = (Union)rel;
        final RelTraitSet traitSet = union.getTraitSet();
        return (RelNode)new JdbcUnion(rel.getCluster(), traitSet.replace((RelTrait)Rel.LOGICAL), (List<RelNode>)ImmutableList.of((Object)left.getInput(), (Object)right.getInput()), union.all, pluginId);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcUnionRule.class);
        CALCITE_INSTANCE = new JdbcUnionRule((Class<? extends Union>)LogicalUnion.class, "JdbcUnionRuleCrel");
        LOGICAL_INSTANCE = new JdbcUnionRule((Class<? extends Union>)UnionRel.class, "JdbcUnionRuleDrel");
    }
    
    private static final class SortDetector extends StatelessRelShuttleImpl
    {
        private boolean hasSort;
        private final Union rootUnion;
        
        public SortDetector(final Union union) {
            this.hasSort = false;
            this.rootUnion = union;
        }
        
        protected RelNode visitChildren(final RelNode node) {
            if (node == this.rootUnion) {
                return super.visitChildren(node);
            }
            if (this.hasSort) {
                return node;
            }
            if (node instanceof Sort) {
                final Sort sortNode = (Sort)node;
                if (sortNode.fetch != null || sortNode.offset != null) {
                    this.hasSort = true;
                    return node;
                }
                return super.visitChildren(node);
            }
            else {
                if (node instanceof SampleRelBase) {
                    this.hasSort = true;
                    return node;
                }
                if (node instanceof TableScan || node instanceof SetOp || node instanceof Join || node instanceof com.dremio.common.logical.data.Join) {
                    return node;
                }
                return super.visitChildren(node);
            }
        }
        
        public boolean hasFoundSort() {
            return this.hasSort;
        }
    }
}
