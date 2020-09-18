package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class DataType
{
    private final String name;
    private final Integer maxScale;
    private final Integer maxPrecision;
    
    DataType(@JsonProperty("name") final String name, @JsonProperty("max_scale") final Integer maxScale, @JsonProperty("max_precision") final Integer maxPrecision) {
        this.name = name;
        this.maxScale = maxScale;
        this.maxPrecision = maxPrecision;
    }
    
    public String getName() {
        return this.name;
    }
    
    public Integer getMaxScale() {
        return this.maxScale;
    }
    
    public Integer getMaxPrecision() {
        return this.maxPrecision;
    }
}
