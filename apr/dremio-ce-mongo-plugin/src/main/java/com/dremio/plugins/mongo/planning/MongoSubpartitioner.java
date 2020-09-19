package com.dremio.plugins.mongo.planning;

import java.util.*;
import com.google.common.collect.*;
import org.bson.types.*;
import com.mongodb.*;
import com.mongodb.client.MongoDatabase;

import org.bson.*;
import org.bson.conversions.*;

import com.dremio.plugins.mongo.connection.MongoConnection;
import com.dremio.plugins.mongo.metadata.*;

public class MongoSubpartitioner
{
    private final MongoClient client;
    
    public MongoSubpartitioner(final MongoConnection connection) {
        this.client = connection.getClient();
    }
    
    public List<Range> getPartitions(final MongoCollection collection, final int recordsPerCollection) {
        final MongoDatabase database = this.client.getDatabase(collection.getDatabase());
        final com.mongodb.client.MongoCollection<Document> documents = database.getCollection(collection.getCollection());
        final long totalDocs = documents.count();
        if (totalDocs < recordsPerCollection) {
            final List<Range> oneRange = Lists.newArrayList();
            oneRange.add(new Range(new MinKey(), new MaxKey()));
            return oneRange;
        }
        final List<Range> ranges = Lists.newArrayList();
        Object previous = null;
        int i = 1;
        while (true) {
            final Object val = this.getValueCondition(documents, i * recordsPerCollection);
            final Range r = new Range((previous == null) ? new MinKey() : previous, val);
            previous = val;
            ranges.add(r);
            if (val instanceof MaxKey) {
                break;
            }
            ++i;
        }
        return ranges;
    }
    
    private Object getValueCondition(final com.mongodb.client.MongoCollection<Document> collection, final int skip) {
        final Document o = (Document)collection.find().projection((Bson)new Document("_id", 1)).sort((Bson)new Document("_id", 1)).skip(skip).first();
        if (o == null) {
            return new MaxKey();
        }
        return o.get("_id");
    }
    
    public class Range
    {
        private final Object min;
        private final Object max;
        
        public Range(final Object min, final Object max) {
            this.min = min;
            this.max = max;
        }
        
        @Override
        public String toString() {
            return "Range [min=" + this.min + ", max=" + this.max + "]";
        }
        
        public MongoChunk getAsChunk(final MongoChunk baseChunk) {
            final MongoChunk c = new MongoChunk(baseChunk.getChunkLocList());
            if (!(this.min instanceof MinKey)) {
                c.setMinFilters(new Document("_id", this.min));
            }
            if (!(this.max instanceof MaxKey)) {
                c.setMaxFilters(new Document("_id", this.max));
            }
            return c;
        }
    }
}
