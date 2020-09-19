package com.dremio.plugins.mongo.planning;

import com.fasterxml.jackson.annotation.*;
import org.bson.*;
import java.util.*;

public class MongoScanSpec
{
    private final String dbName;
    private final String collectionName;
    private final MongoPipeline pipeline;
    
    public MongoScanSpec(@JsonProperty("dbName") final String dbName, @JsonProperty("collectionName") final String collectionName, @JsonProperty("pipeline") final MongoPipeline pipeline) {
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.pipeline = pipeline;
    }
    
    public String getDbName() {
        return this.dbName;
    }
    
    public String getCollectionName() {
        return this.collectionName;
    }
    
    @JsonProperty("pipeline")
    public MongoPipeline getPipeline() {
        return this.pipeline;
    }
    
    @JsonIgnore
    public String getMongoQuery() {
        final StringBuilder sb = new StringBuilder();
        sb.append("use ");
        sb.append(this.dbName);
        sb.append("; ");
        sb.append("db.");
        sb.append(this.collectionName);
        sb.append(".");
        sb.append(this.pipeline.toString());
        return sb.toString();
    }
    
    @Override
    public String toString() {
        return "MongoScanSpec [dbName=" + this.dbName + ", collectionName=" + this.collectionName + ", pipeline=" + this.pipeline + "]";
    }
    
    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof MongoScanSpec)) {
            return false;
        }
        final MongoScanSpec that = (MongoScanSpec)other;
        final boolean equals = Objects.equals(this.dbName, that.dbName) && Objects.equals(this.collectionName, that.collectionName) && Objects.equals(this.pipeline, that.pipeline);
        return equals;
    }
    
    public MongoScanSpec plusPipeline(final List<Document> operations, final boolean needsCollation) {
        final List<Document> pipes = new ArrayList<Document>(this.pipeline.getPipelines());
        pipes.addAll(operations);
        final MongoPipeline newPipeline = MongoPipeline.createMongoPipeline(pipes, this.pipeline.needsCollation() || needsCollation);
        return new MongoScanSpec(this.dbName, this.collectionName, newPipeline);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.dbName, this.collectionName, this.pipeline);
    }
}
