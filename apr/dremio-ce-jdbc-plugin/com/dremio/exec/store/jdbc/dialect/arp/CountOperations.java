package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class CountOperations
{
    private final CountOperation countStar;
    private final CountOperation countOperation;
    private final CountOperation countMultiOperation;
    private final CountOperation countDistinctOperation;
    private final CountOperation countDistinctMultiOperation;
    
    CountOperations(@JsonProperty("count_star") final CountOperation countStar, @JsonProperty("count") final CountOperation count, @JsonProperty("count_multi") final CountOperation countMulti, @JsonProperty("count_distinct") final CountOperation countDistinct, @JsonProperty("count_distinct_multi") final CountOperation countDistinctMulti) {
        this.countOperation = count;
        this.countMultiOperation = countMulti;
        this.countDistinctOperation = countDistinct;
        this.countDistinctMultiOperation = countDistinctMulti;
        this.countStar = countStar;
    }
    
    public CountOperation getCountOperation(final CountOperationType type) {
        switch (type) {
            case COUNT_STAR: {
                return this.countStar;
            }
            case COUNT_DISTINCT: {
                return this.countDistinctOperation;
            }
            case COUNT_DISTINCT_MULTI: {
                return this.countDistinctMultiOperation;
            }
            case COUNT_MULTI: {
                return this.countMultiOperation;
            }
            default: {
                return this.countOperation;
            }
        }
    }
    
    public enum CountOperationType
    {
        COUNT_STAR, 
        COUNT, 
        COUNT_MULTI, 
        COUNT_DISTINCT, 
        COUNT_DISTINCT_MULTI;
    }
}
