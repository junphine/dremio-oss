package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class DateTimeFormatDescriptor
{
    private final boolean enable;
    private final String format;
    
    DateTimeFormatDescriptor(@JsonProperty("enable") final boolean enable, @JsonProperty("format") final String format) {
        this.enable = enable;
        this.format = format;
    }
    
    public boolean isEnable() {
        return this.enable;
    }
    
    public String getFormat() {
        return this.format;
    }
}
