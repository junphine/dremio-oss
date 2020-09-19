package com.dremio.exec.store.jdbc.rules;

import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.*;
import com.dremio.exec.calcite.logical.*;
import com.dremio.exec.catalog.*;
import org.apache.calcite.schema.*;
import com.dremio.exec.planner.logical.*;
import com.dremio.exec.store.jdbc.rel.*;
import java.util.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.plan.*;

public final class JdbcTableModificationRule extends JdbcUnaryConverterRule
{
    public static final JdbcTableModificationRule INSTANCE;
    
    private JdbcTableModificationRule() {
        super((Class<? extends RelNode>)LogicalTableModify.class, "JdbcTableModificationRule");
    }
    
    public RelNode convert(final RelNode rel, final JdbcCrel crel, final StoragePluginId pluginId) {
        final LogicalTableModify modify = (LogicalTableModify)rel;
        final ModifiableTable modifiableTable = (ModifiableTable)modify.getTable().unwrap((Class)ModifiableTable.class);
        if (modifiableTable == null) {
            return null;
        }
        return (RelNode)new JdbcTableModify(modify.getCluster(), modify.getTraitSet().replace((RelTrait)Rel.LOGICAL), modify.getTable(), modify.getCatalogReader(), crel.getInput(), modify.getOperation(), modify.getUpdateColumnList(), modify.getSourceExpressionList(), modify.isFlattened(), pluginId);
    }
    
    static {
        INSTANCE = new JdbcTableModificationRule();
    }
}
