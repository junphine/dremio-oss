package com.dremio.plugins.mongo.planning.rules;

import com.dremio.exec.catalog.*;
import org.apache.calcite.rel.*;
import com.dremio.plugins.mongo.planning.*;
import com.dremio.plugins.mongo.planning.rels.*;

public class MongoImplementor
{
    private final StoragePluginId pluginId;
    private boolean needsLimitZero;
    private boolean hasSample;
    private long limitSize;
    private boolean hasLimit;
    
    public MongoImplementor(final StoragePluginId pluginId) {
        this.needsLimitZero = false;
        this.hasSample = false;
        this.limitSize = Long.MAX_VALUE;
        this.hasLimit = false;
        this.pluginId = pluginId;
    }
    
    public StoragePluginId getPluginId() {
        return this.pluginId;
    }
    
    public MongoScanSpec visitChild(final int i, final RelNode e) {
        return ((MongoRel)e).implement(this);
    }
    
    public void markAsNeedsLimitZero() {
        this.needsLimitZero = true;
    }
    
    public boolean needsLimitZero() {
        return this.needsLimitZero;
    }
    
    public void setHasSample() {
        this.hasSample = true;
    }
    
    public boolean hasSample() {
        return this.hasSample;
    }
    
    public void setHasLimit(final long limitSize) {
        this.hasLimit = true;
        if (this.limitSize > limitSize) {
            this.limitSize = limitSize;
        }
    }
    
    public boolean hasLimit() {
        return this.hasLimit;
    }
    
    public long getLimitSize() {
        return this.limitSize;
    }
}
