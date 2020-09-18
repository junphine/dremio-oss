package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class CountOperation
{
    private final boolean enable;
    private final VarArgsRewritingSignature signature;
    
    CountOperation(@JsonProperty("enable") final boolean enable, @JsonProperty("variable_rewrite") final VariableRewrite rewrite) {
        this.enable = enable;
        if (rewrite != null) {
            this.signature = new VarArgsRewritingSignature("bigint", null, rewrite);
        }
        else {
            this.signature = null;
        }
    }
    
    public boolean isEnable() {
        return this.enable;
    }
    
    public VarArgsRewritingSignature getSignature() {
        return this.signature;
    }
}
