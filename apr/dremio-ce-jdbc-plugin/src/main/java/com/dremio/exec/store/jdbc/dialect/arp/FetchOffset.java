package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.*;

public class FetchOffset
{
    private final FetchOffsetDescriptor offsetFetch;
    private final FetchOffsetDescriptor fetchOnly;
    private final FetchOffsetDescriptor offsetOnly;
    
    FetchOffset(@JsonProperty("offset_fetch") final FetchOffsetDescriptor offsetFetch, @JsonProperty("fetch_only") final FetchOffsetDescriptor fetchOnly, @JsonProperty("offset_only") final FetchOffsetDescriptor offsetOnly) {
        if (offsetFetch.getFormat() == null) {
            this.offsetFetch = new FetchOffsetDescriptor(offsetFetch.isEnable(), "OFFSET {0} ROWS FETCH {1} ROWS ONLY");
        }
        else {
            this.offsetFetch = offsetFetch;
        }
        if (fetchOnly.getFormat() == null) {
            this.fetchOnly = new FetchOffsetDescriptor(fetchOnly.isEnable(), "FETCH {0} ROWS ONLY");
        }
        else {
            this.fetchOnly = fetchOnly;
        }
        if (offsetOnly.getFormat() == null) {
            this.offsetOnly = new FetchOffsetDescriptor(offsetOnly.isEnable(), "OFFSET {0} ROWS");
        }
        else {
            this.offsetOnly = offsetOnly;
        }
    }
    
    public FetchOffsetDescriptor getOffsetFetch() {
        return this.offsetFetch;
    }
    
    public FetchOffsetDescriptor getFetchOnly() {
        return this.fetchOnly;
    }
    
    public FetchOffsetDescriptor getOffsetOnly() {
        return this.offsetOnly;
    }
}
