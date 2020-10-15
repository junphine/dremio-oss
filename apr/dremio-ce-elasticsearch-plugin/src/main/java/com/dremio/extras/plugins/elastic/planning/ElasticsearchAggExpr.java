package com.dremio.extras.plugins.elastic.planning;

import com.fasterxml.jackson.annotation.*;
import com.google.common.base.*;

public class ElasticsearchAggExpr
{
    private final String operation;
    private final Type type;
    
    public ElasticsearchAggExpr(@JsonProperty("operation") final String operation, @JsonProperty("type") final Type type) {
        this.operation = operation;
        this.type = type;
    }
    
    public String getOperation() {
        return this.operation;
    }
    
    public Type getType() {
        return this.type;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ElasticsearchAggExpr)) {
            return false;
        }
        final ElasticsearchAggExpr castOther = (ElasticsearchAggExpr)other;
        return Objects.equal(this.operation, castOther.operation) && Objects.equal(this.type, castOther.type);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(new Object[] { this.operation, this.type });
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("operation", this.operation).add("type", this.type).toString();
    }
    
    public enum Type
    {
        NORMAL, 
        COUNT_DISTINCT, 
        COUNT_ALL;
    }
}
