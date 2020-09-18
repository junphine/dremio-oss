package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;
import java.util.*;

public class Values extends RelationalAlgebraOperation
{
    @JsonIgnore
    private final Method method;
    @JsonIgnore
    private final String dummyTable;
    
    public Values(@JsonProperty("enable") final boolean enable, @JsonProperty("method") final String method, @JsonProperty("dummy_table") final String dummyTable) {
        super(enable);
        this.method = Method.valueOf(method.toUpperCase(Locale.ROOT));
        this.dummyTable = dummyTable;
    }
    
    public Method getMethod() {
        return this.method;
    }
    
    public String getDummyTable() {
        return this.dummyTable;
    }
    
    enum Method
    {
        VALUES, 
        DUMMY_TABLE;
    }
}
