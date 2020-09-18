package com.dremio.plugins.mongo.metadata;

import org.bson.*;
import com.mongodb.*;
import com.google.common.base.*;
import com.dremio.mongo.proto.*;
import com.dremio.connector.metadata.*;
import java.util.*;
import com.dremio.exec.planner.fragment.*;
import com.dremio.exec.physical.*;
import com.dremio.exec.proto.*;
import com.fasterxml.jackson.annotation.*;
import com.dremio.exec.store.schedule.*;

public class MongoChunk
{
    private static final Joiner SPLIT_KEY_JOINER;
    private static final double SPLIT_DEFAULT_SIZE = 100000.0;
    private final List<String> chunkLocList;
    private Document minFilters;
    private Document maxFilters;
    
    public MongoChunk(final List<String> chunkLocList) {
        this.chunkLocList = chunkLocList;
    }
    
    public static MongoChunk newWithAddress(final ServerAddress chunkLoc) {
        Preconditions.checkNotNull((Object)chunkLoc);
        return new MongoChunk(Collections.singletonList(chunkLoc.toString()));
    }
    
    public static MongoChunk newWithAddresses(final Collection<ServerAddress> chunkLocList) {
        final List<String> list = new ArrayList<String>();
        for (final ServerAddress a : chunkLocList) {
            list.add(a.toString());
        }
        return new MongoChunk(list);
    }
    
    public PartitionChunk toSplit() {
        final MongoReaderProto.MongoSplitXattr.Builder splitAttributes = MongoReaderProto.MongoSplitXattr.newBuilder();
        if (this.minFilters != null) {
            splitAttributes.setMinFilter(this.minFilters.toJson());
        }
        if (this.maxFilters != null) {
            splitAttributes.setMaxFilter(this.maxFilters.toJson());
        }
        if (this.chunkLocList != null && !this.chunkLocList.isEmpty()) {
            splitAttributes.addAllHosts(this.chunkLocList);
        }
        final List<DatasetSplitAffinity> affinities = new ArrayList<DatasetSplitAffinity>();
        for (final String host : this.chunkLocList) {
            affinities.add(DatasetSplitAffinity.of(host, 100000.0));
        }
        final MongoReaderProto.MongoSplitXattr extended = splitAttributes.build();
        final DatasetSplit[] array = { null };
        final int n = 0;
        final List<DatasetSplitAffinity> list = affinities;
        final long n2 = 100000L;
        final long n3 = 100000L;
        final MongoReaderProto.MongoSplitXattr mongoSplitXattr = extended;
        Objects.requireNonNull(mongoSplitXattr);
        array[n] = DatasetSplit.of((List)list, n2, n3, mongoSplitXattr::writeTo);
        return PartitionChunk.of(array);
    }
    
    public List<String> getChunkLocList() {
        return this.chunkLocList;
    }
    
    public void setMinFilters(final Document minFilters) {
        this.minFilters = minFilters;
    }
    
    public Map<String, Object> getMinFilters() {
        return (Map<String, Object>)this.minFilters;
    }
    
    public void setMaxFilters(final Document maxFilters) {
        this.maxFilters = maxFilters;
    }
    
    public Map<String, Object> getMaxFilters() {
        return (Map<String, Object>)this.maxFilters;
    }
    
    @Override
    public String toString() {
        return "ChunkInfo [chunkLocList=" + this.chunkLocList + ", minFilters=" + this.minFilters + ", maxFilters=" + this.maxFilters + "]";
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final MongoChunk that = (MongoChunk)obj;
        return Objects.equals(this.chunkLocList, that.chunkLocList) && Objects.equals(this.minFilters, that.minFilters) && Objects.equals(this.maxFilters, that.maxFilters);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.chunkLocList, this.minFilters, this.maxFilters);
    }
    
    @JsonIgnore
    public MongoCompleteWork newCompleteWork(final ExecutionNodeMap executionNodes) {
        final List<EndpointAffinity> affinityList = new ArrayList<EndpointAffinity>();
        for (final String loc : this.chunkLocList) {
            final CoordinationProtos.NodeEndpoint ep = executionNodes.getEndpoint(loc);
            if (ep != null) {
                affinityList.add(new EndpointAffinity(ep, 1.0));
            }
        }
        return new MongoCompleteWork(affinityList);
    }
    
    static {
        SPLIT_KEY_JOINER = Joiner.on('-');
    }
    
    public class MongoCompleteWork extends SimpleCompleteWork
    {
        public MongoCompleteWork(final List<EndpointAffinity> affinity) {
            super(1000000L, (List)affinity);
        }
        
        public MongoChunk getChunk() {
            return MongoChunk.this;
        }
    }
}
