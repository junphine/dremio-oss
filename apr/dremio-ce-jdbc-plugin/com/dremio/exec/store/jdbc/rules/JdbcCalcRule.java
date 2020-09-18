package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.plan.*;

public final class JdbcCalcRule extends JdbcUnaryConverterRule
{
    public static final JdbcCalcRule INSTANCE;
    
    private JdbcCalcRule() {
        super((Class<? extends RelNode>)LogicalCalc.class, "JdbcCalcRule");
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final LogicalCalc calc = (LogicalCalc)rel;
        if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
            return null;
        }
        return (RelNode)new JdbcCalc(rel.getCluster(), rel.getTraitSet().replace((RelTrait)Rel.LOGICAL), crel.getInput(), calc.getProgram(), pluginId);
    }
    
    static {
        INSTANCE = new JdbcCalcRule();
    }
}
