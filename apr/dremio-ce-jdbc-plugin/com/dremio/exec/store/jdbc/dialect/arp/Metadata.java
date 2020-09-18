package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

class Metadata
{
    private final String name;
    private final String apiname;
    private final String version;
    
    public String getName() {
        return this.name;
    }
    
    public String getApiname() {
        return this.apiname;
    }
    
    public String getVersion() {
        return this.version;
    }
    
    Metadata(@JsonProperty("name") final String name, @JsonProperty("apiname") final String apiname, @JsonProperty("version") final String version) {
        this.name = name;
        this.apiname = apiname;
        this.version = version;
    }
}
