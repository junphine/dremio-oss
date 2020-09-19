package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class RelationalAlgebraOperation
{
    private final boolean enable;
    
    RelationalAlgebraOperation(@JsonProperty("enable") final boolean enable) {
        this.enable = enable;
    }
    
    public boolean isEnabled() {
        return this.enable;
    }
}
