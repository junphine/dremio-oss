package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.convert.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.exec.planner.logical.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.planner.physical.*;
import com.dremio.exec.planner.common.*;
import com.dremio.exec.store.jdbc.rel.*;
import org.apache.calcite.plan.*;

public final class JdbcPrule extends ConverterRule
{
    private final FunctionLookupContext context;
    
    public JdbcPrule(final FunctionLookupContext context) {
        super((Class)JdbcRel.class, (RelTrait)Rel.LOGICAL, (RelTrait)Prel.PHYSICAL, "JDBC_PREL_Converter_");
        this.context = context;
    }
    
    public RelNode convert(final RelNode in) {
        final JdbcRel drel = (JdbcRel)in;
        RelTraitSet physicalTraits = drel.getTraitSet().replace(this.getOutTrait());
        physicalTraits = physicalTraits.replace((RelTrait)DistributionTrait.SINGLETON);
        return (RelNode)new JdbcIntermediatePrel(drel.getCluster(), physicalTraits, drel.getSubTree(), this.context, ((JdbcRelImpl)drel.getSubTree()).getPluginId());
    }
}
