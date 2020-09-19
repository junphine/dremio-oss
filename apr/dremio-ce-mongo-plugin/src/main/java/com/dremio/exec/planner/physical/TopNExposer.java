package com.dremio.exec.planner.physical;

import org.apache.calcite.rel.*;

public class TopNExposer
{
    public static RelCollation getCollation(final TopNPrel prel) {
        return prel.collation;
    }
    
    public static int getLimit(final TopNPrel prel) {
        return prel.limit;
    }
}
