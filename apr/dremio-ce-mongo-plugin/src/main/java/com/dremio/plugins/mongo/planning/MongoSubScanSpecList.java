package com.dremio.plugins.mongo.planning;

import java.util.*;
import com.fasterxml.jackson.annotation.*;

public class MongoSubScanSpecList
{
    private List<MongoSubScanSpec> specs;
    
    public MongoSubScanSpecList(@JsonProperty("specs") final List<MongoSubScanSpec> specs) {
        this.specs = specs;
    }
    
    public List<MongoSubScanSpec> getSpecs() {
        return this.specs;
    }
}
