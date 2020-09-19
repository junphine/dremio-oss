package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class SubQuerySupport
{
    private final boolean supportsSubQuery;
    private final boolean supportsCorrelated;
    private final boolean supportsScalar;
    private final boolean supportsInClause;
    
    SubQuerySupport(@JsonProperty("enable") final boolean enable, @JsonProperty("correlated") final boolean correlated, @JsonProperty("scalar") final boolean scalar, @JsonProperty("in_clause") final boolean inClause) {
        this.supportsSubQuery = enable;
        this.supportsCorrelated = correlated;
        this.supportsScalar = scalar;
        this.supportsInClause = inClause;
    }
    
    public boolean isEnabled() {
        return this.supportsSubQuery;
    }
    
    public boolean getCorrelatedSubQuerySupport() {
        return this.supportsCorrelated;
    }
    
    public boolean getScalarSupport() {
        return this.supportsScalar;
    }
    
    public boolean getInClauseSupport() {
        return this.supportsInClause;
    }
}
