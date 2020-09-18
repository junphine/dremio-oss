package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import java.util.*;

public class OrderBy
{
    private final boolean enable;
    private final Ordering defaultNullsOrdering;
    
    OrderBy(@JsonProperty("enable") final boolean enable, @JsonProperty("default_nulls_ordering") final String ordering) {
        this.enable = enable;
        this.defaultNullsOrdering = Ordering.valueOf(ordering.toUpperCase(Locale.ROOT));
    }
    
    public boolean isEnabled() {
        return this.enable;
    }
    
    public Ordering getDefaultNullsOrdering() {
        return this.defaultNullsOrdering;
    }
    
    enum Ordering
    {
        FIRST, 
        HIGH, 
        LAST, 
        LOW;
    }
}
