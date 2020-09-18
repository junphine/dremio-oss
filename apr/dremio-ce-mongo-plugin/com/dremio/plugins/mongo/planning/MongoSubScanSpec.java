package com.dremio.plugins.mongo.planning;

import java.util.*;
import org.bson.*;
import com.dremio.exec.store.*;
import com.dremio.mongo.proto.*;
import com.google.common.base.*;
import com.google.protobuf.*;
import com.fasterxml.jackson.annotation.*;

public class MongoSubScanSpec
{
    private String dbName;
    private String collectionName;
    private List<String> hosts;
    private Document minFilters;
    private Document maxFilters;
    private MongoPipeline pipeline;
    
    public MongoSubScanSpec(@JsonProperty("dbName") final String dbName, @JsonProperty("collectionName") final String collectionName, @JsonProperty("hosts") final List<String> hosts, @JsonProperty("minFilters") final String minFilters, @JsonProperty("maxFilters") final String maxFilters, @JsonProperty("pipeline") final MongoPipeline pipeline) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.hosts = hosts;
        this.minFilters = ((minFilters == null || minFilters.isEmpty()) ? null : Document.parse(minFilters));
        this.maxFilters = ((maxFilters == null || maxFilters.isEmpty()) ? null : Document.parse(maxFilters));
        this.pipeline = pipeline;
    }
    
    public static MongoSubScanSpec of(final MongoScanSpec scanSpec, final SplitAndPartitionInfo split) {
        MongoReaderProto.MongoSplitXattr splitAttributes;
        try {
            splitAttributes = MongoReaderProto.MongoSplitXattr.parseFrom(split.getDatasetSplitInfo().getExtendedProperty());
        }
        catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate((Throwable)e);
        }
        final MongoSubScanSpec subScanSpec = new MongoSubScanSpec(scanSpec.getDbName(), scanSpec.getCollectionName(), (List<String>)splitAttributes.getHostsList(), splitAttributes.getMinFilter(), splitAttributes.getMaxFilter(), scanSpec.getPipeline().copy());
        return subScanSpec;
    }
    
    MongoSubScanSpec() {
    }
    
    public String getDbName() {
        return this.dbName;
    }
    
    public MongoSubScanSpec setDbName(final String dbName) {
        this.dbName = dbName;
        return this;
    }
    
    public String getCollectionName() {
        return this.collectionName;
    }
    
    public MongoSubScanSpec setCollectionName(final String collectionName) {
        this.collectionName = collectionName;
        return this;
    }
    
    public List<String> getHosts() {
        return this.hosts;
    }
    
    public MongoSubScanSpec setHosts(final List<String> hosts) {
        this.hosts = hosts;
        return this;
    }
    
    @JsonProperty
    public String getMinFilters() {
        return (this.minFilters == null) ? null : this.minFilters.toJson();
    }
    
    @JsonIgnore
    public Document getMinFiltersAsDocument() {
        return this.minFilters;
    }
    
    public MongoSubScanSpec setMinFilters(final Document minFilters) {
        this.minFilters = minFilters;
        return this;
    }
    
    @JsonProperty
    public String getMaxFilters() {
        return (this.maxFilters == null) ? null : this.maxFilters.toJson();
    }
    
    @JsonIgnore
    public Document getMaxFiltersAsDocument() {
        return this.maxFilters;
    }
    
    public MongoSubScanSpec setMaxFilters(final Document maxFilters) {
        this.maxFilters = maxFilters;
        return this;
    }
    
    public MongoPipeline getPipeline() {
        return this.pipeline;
    }
    
    public MongoSubScanSpec setPipeline(final MongoPipeline pipeline) {
        this.pipeline = pipeline;
        return this;
    }
    
    @Override
    public String toString() {
        return "MongoSubScanSpec [dbName=" + this.dbName + ", collectionName=" + this.collectionName + ", hosts=" + this.hosts + ", minFilters=" + this.minFilters + ", maxFilters=" + this.maxFilters + ", pipeline=" + this.pipeline + "]";
    }
}
