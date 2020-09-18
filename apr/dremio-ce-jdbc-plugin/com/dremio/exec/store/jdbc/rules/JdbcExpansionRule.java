package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.planner.acceleration.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.plan.*;

public final class JdbcExpansionRule extends JdbcConverterRule
{
    public static final RelOptRule INSTANCE;
    
    private JdbcExpansionRule() {
        super(RelOptHelper.some((Class)ExpansionNode.class, RelOptHelper.any((Class)JdbcCrel.class), new RelOptRuleOperand[0]), "jdbc-expansion-removal");
    }
    
    public void onMatch(final RelOptRuleCall call) {
        call.transformTo(call.rel(1));
    }
    
    static {
        INSTANCE = new JdbcExpansionRule();
    }
}
