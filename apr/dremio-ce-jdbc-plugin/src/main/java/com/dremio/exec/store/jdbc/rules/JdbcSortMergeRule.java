package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.catalog.*;
import org.slf4j.*;

public final class JdbcSortMergeRule extends RelOptRule
{
    private static final Logger logger;
    public static final JdbcSortMergeRule INSTANCE;
    
    private JdbcSortMergeRule() {
        super(operand((Class)JdbcSort.class, operand((Class)JdbcSort.class, any()), new RelOptRuleOperand[0]), "JdbcSortMergeRule");
    }
    
    public boolean matches(final RelOptRuleCall call) {
        return true;
    }
    
    public void onMatch(final RelOptRuleCall call) {
        final JdbcSort sort1 = (JdbcSort)call.rel(0);
        final JdbcSort sort2 = (JdbcSort)call.rel(1);
        final JdbcSort sorted = (sort1.getCollation() != RelCollations.EMPTY) ? sort1 : sort2;
        final RexNode offset = (sort1.offset == null) ? sort2.offset : sort1.offset;
        final RexNode fetch = (sort1.fetch == null) ? sort2.fetch : sort1.fetch;
        final StoragePluginId pluginId = sort1.getPluginId();
        final JdbcSort mergedSort = new JdbcSort(sort1.getCluster(), sorted.getTraitSet(), sort2.getInput(), sorted.getCollation(), offset, fetch, pluginId);
        call.transformTo((RelNode)mergedSort);
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)JdbcSortMergeRule.class);
        INSTANCE = new JdbcSortMergeRule();
    }
}
