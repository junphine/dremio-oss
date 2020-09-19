package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.convert.*;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.store.jdbc.rel.*;
import com.google.common.collect.*;
import org.apache.calcite.rex.*;
import com.dremio.exec.calcite.logical.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.plan.*;
import com.dremio.exec.planner.logical.*;

public final class JdbcValuesRule extends ConverterRule
{
    public static final JdbcValuesRule CALCITE_INSTANCE;
    public static final JdbcValuesRule LOGICAL_INSTANCE;
    
    private JdbcValuesRule(final Class<? extends Values> clazz, final RelTrait in, final String name) {
        super((Class)clazz, in, (RelTrait)Rel.LOGICAL, name);
    }
    
    public RelNode convert(final RelNode rel) {
        final Values values = (Values)rel;
        final JdbcValues jdbcValues = new JdbcValues(values.getCluster(), values.getRowType(), (ImmutableList<ImmutableList<RexLiteral>>)values.getTuples(), values.getTraitSet().replace((RelTrait)Rel.LOGICAL));
        return (RelNode)new JdbcCrel(values.getCluster(), values.getTraitSet().replace((RelTrait)Rel.LOGICAL), (RelNode)jdbcValues, jdbcValues.getPluginId());
    }
    
    static {
        CALCITE_INSTANCE = new JdbcValuesRule((Class<? extends Values>)LogicalValues.class, (RelTrait)Convention.NONE, "JdbcValuesRuleCrel");
        LOGICAL_INSTANCE = new JdbcValuesRule((Class<? extends Values>)ValuesRel.class, (RelTrait)Rel.LOGICAL, "JdbcValuesRuleDrel");
    }
}
