package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.rules.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.plan.*;

public final class JdbcFilterSetOpTransposeRule extends FilterSetOpTransposeRule
{
    public static final JdbcFilterSetOpTransposeRule INSTANCE;
    
    private JdbcFilterSetOpTransposeRule() {
        super(DremioRelFactories.CALCITE_LOGICAL_BUILDER);
    }
    
    public boolean matches(final RelOptRuleCall call) {
        final Filter filterRel = (Filter)call.rel(0);
        final SetOp setOp = (SetOp)call.rel(1);
        return filterRel.getConvention() == Convention.NONE && setOp.getConvention() == Convention.NONE;
    }
    
    static {
        INSTANCE = new JdbcFilterSetOpTransposeRule();
    }
}
