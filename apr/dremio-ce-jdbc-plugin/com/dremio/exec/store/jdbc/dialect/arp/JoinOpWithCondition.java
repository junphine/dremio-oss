package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class JoinOpWithCondition extends JoinOp
{
    private final boolean inequality;
    
    JoinOpWithCondition(@JsonProperty("enable") final boolean enable, @JsonProperty("rewrite") final String rewrite, @JsonProperty("inequality") final boolean inequality) {
        super(enable, rewrite);
        this.inequality = inequality;
    }
    
    public boolean isInequality() {
        return this.inequality;
    }
    
    @Override
    public String toString() {
        return super.toString() + "Supports inequality: " + this.inequality + "\n";
    }
}
