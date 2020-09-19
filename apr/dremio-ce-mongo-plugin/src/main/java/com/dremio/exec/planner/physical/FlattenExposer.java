package com.dremio.exec.planner.physical;

import org.apache.calcite.rex.*;

public class FlattenExposer
{
    public static Integer getInputRef(final FlattenPrel prel) {
        final RexNode node = prel.toFlatten;
        if (node instanceof RexInputRef) {
            return ((RexInputRef)node).getIndex();
        }
        return null;
    }
}
