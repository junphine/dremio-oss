package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import java.util.*;

public class Aggregation extends RelationalAlgebraOperation
{
    @JsonIgnore
    private final boolean supportsDistinct;
    @JsonIgnore
    private final Map<OperatorDescriptor, Signature> functionMap;
    @JsonIgnore
    private final CountOperations countOperations;
    
    Aggregation(@JsonProperty("enable") final boolean enable, @JsonProperty("count_functions") final CountOperations countOperations, @JsonProperty("distinct") final boolean supportsDistinct, @JsonProperty("functions") final List<OperatorDefinition> operators) {
        super(enable);
        this.supportsDistinct = supportsDistinct;
        this.functionMap = OperatorDefinition.buildOperatorMap(operators);
        this.countOperations = countOperations;
    }
    
    public boolean supportsDistinct() {
        return this.supportsDistinct;
    }
    
    public Map<OperatorDescriptor, Signature> getFunctionMap() {
        return this.functionMap;
    }
    
    CountOperation getCountOperation(final CountOperations.CountOperationType operationType) {
        return this.countOperations.getCountOperation(operationType);
    }
}
