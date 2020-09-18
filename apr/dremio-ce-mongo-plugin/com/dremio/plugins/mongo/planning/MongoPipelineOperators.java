package com.dremio.plugins.mongo.planning;

import java.util.*;
import com.google.common.collect.*;

public enum MongoPipelineOperators
{
    PROJECT("$project"), 
    UNWIND("$unwind"), 
    SORT("$sort"), 
    MATCH("$match");
    
    public static final Set<MongoPipelineOperators> PROJECT_ONLY;
    public static final Set<MongoPipelineOperators> MATCH_ONLY;
    public static final Set<MongoPipelineOperators> PROJECT_MATCH;
    private final String mongoOperator;
    
    private MongoPipelineOperators(final String mongoOperator) {
        this.mongoOperator = mongoOperator;
    }
    
    public String getOperator() {
        return this.mongoOperator;
    }
    
    static {
        PROJECT_ONLY = Sets.newHashSet((Object[])new MongoPipelineOperators[] { MongoPipelineOperators.PROJECT });
        MATCH_ONLY = Sets.newHashSet((Object[])new MongoPipelineOperators[] { MongoPipelineOperators.MATCH });
        PROJECT_MATCH = Sets.newHashSet((Object[])new MongoPipelineOperators[] { MongoPipelineOperators.PROJECT, MongoPipelineOperators.MATCH });
    }
}
