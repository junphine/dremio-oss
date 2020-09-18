package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class JoinOp
{
    private final boolean enable;
    protected final String rewrite;
    
    JoinOp(@JsonProperty("enable") final boolean enable, @JsonProperty("rewrite") final String rewrite) {
        this.enable = enable;
        this.rewrite = rewrite;
    }
    
    public boolean isEnable() {
        return this.enable;
    }
    
    public String getRewrite() {
        return this.rewrite;
    }
    
    public <T> T unwrap(final Class<T> iface) {
        if (iface.isAssignableFrom(this.getClass())) {
            return (T)this;
        }
        return null;
    }
    
    @Override
    public String toString() {
        return "Is enabled: '" + this.enable + "'\nRewrite: " + this.rewrite + "\n";
    }
}
