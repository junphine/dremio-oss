package com.dremio.plugins.mongo.metadata;

import com.dremio.mongo.proto.*;
import com.dremio.plugins.mongo.connection.*;
import com.google.common.base.*;
import com.dremio.common.exceptions.*;
import com.dremio.exec.store.*;
import java.util.concurrent.*;
import org.bson.*;
import com.dremio.plugins.mongo.planning.*;
import java.util.*;
import com.dremio.plugins.mongo.*;
import org.bson.conversions.*;
import com.mongodb.client.*;
import com.google.common.collect.*;
import org.slf4j.*;

public class MongoChunks implements Iterable<MongoChunk>
{
    private static final Integer SELECT;
    private static final Logger logger;
    private final MongoTopology topology;
    private final MongoCollection collection;
    private List<MongoChunk> chunks;
    private final MongoReaderProto.CollectionType type;
    
    public MongoChunks(final MongoCollection collection, final MongoConnection connection, final MongoTopology topology, final int subpartitionSize, final String pluginName) {
        this.collection = collection;
        this.topology = topology;
        MongoChunks.logger.debug("Start init");
        final Stopwatch watch = Stopwatch.createStarted();
        try {
            final String collectionName = collection.getCollection();
            final MongoDatabase database = connection.getDatabase(collection.getDatabase());
            final com.mongodb.client.MongoCollection<Document> collections = (com.mongodb.client.MongoCollection<Document>)database.getCollection(collection.getCollection());
            if (topology.isSharded()) {
                if (!isShardedCollection(database, collectionName) || !topology.canConnectToMongods()) {
                    this.type = MongoReaderProto.CollectionType.SINGLE_PARTITION;
                    this.chunks = handleSimpleCase(connection);
                    return;
                }
                final boolean assignmentPerShard = !topology.canGetChunks() || isHashed(collections) || this.isCompoundKey(connection);
                if (assignmentPerShard) {
                    final List<MongoChunk> chunks = new ArrayList<MongoChunk>();
                    for (final MongoTopology.ReplicaSet replicaSet : topology) {
                        this.addChunk(chunks, connection, subpartitionSize, replicaSet);
                    }
                    this.type = MongoReaderProto.CollectionType.NODE_PARTITION;
                    this.chunks = chunks;
                    return;
                }
                this.chunks = this.handleShardedCollection(connection);
                this.type = MongoReaderProto.CollectionType.RANGE_PARTITION;
            }
            else {
                final List<MongoChunk> chunks2 = new ArrayList<MongoChunk>();
                this.addChunk(chunks2, connection, subpartitionSize, topology.iterator().next());
                this.chunks = chunks2;
                this.type = MongoReaderProto.CollectionType.SUB_PARTITIONED;
            }
        }
        catch (RuntimeException e) {
            throw StoragePluginUtils.message(UserException.dataReadError((Throwable)e), pluginName, "Failure while attempting to retrieve Mongo read information for collection %s.", new Object[] { collection }).build(MongoChunks.logger);
        }
        finally {
            MongoChunks.logger.debug("Mongo chunks retrieved in {} ms.", watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }
    
    public MongoReaderProto.CollectionType getCollectionType() {
        return this.type;
    }
    
    private void addChunk(final List<MongoChunk> listToFill, final MongoConnection client, final int subpartitionSize, final MongoTopology.ReplicaSet replicaSet) {
        if (subpartitionSize > 0) {
            final MongoSubpartitioner p = new MongoSubpartitioner(client);
            int index = 0;
            for (final MongoSubpartitioner.Range r : p.getPartitions(this.collection, subpartitionSize)) {
                final MongoChunk prototypeChunk = replicaSet.getAsChunk(index++);
                listToFill.add(r.getAsChunk(prototypeChunk));
            }
        }
        else {
            listToFill.add(replicaSet.getAsChunk(0));
        }
    }
    
    private static List<MongoChunk> handleSimpleCase(final MongoConnection connection) {
        final List<MongoChunk> chunks = new ArrayList<MongoChunk>();
        chunks.add(MongoChunk.newWithAddress(connection.getAddress()));
        return chunks;
    }
    
    private static boolean isHashed(final com.mongodb.client.MongoCollection<Document> collection) {
        try {
            for (final Document oneIndex : collection.listIndexes()) {
                for (final Map.Entry<String, Object> oneKey : ((Document)oneIndex.get("key")).entrySet()) {
                    if (oneKey.getValue() instanceof String && "hashed".equalsIgnoreCase(oneKey.getValue().toString())) {
                        return true;
                    }
                }
            }
        }
        catch (Exception e) {
            MongoChunks.logger.error("Could not get list of indices for collection, defaulting to hashed mode");
            return true;
        }
        return false;
    }
    
    private static boolean isShardedCollection(final MongoDatabase db, final String collectionName) {
        final Document stats = MongoDocumentHelper.runMongoCommand(db, (Bson)new Document("collStats", collectionName));
        return stats != null && stats.getBoolean("sharded", false);
    }
    
    private boolean isCompoundKey(final MongoConnection client) {
        final FindIterable<Document> chunks = this.getChunkCollection(client);
        final Document firstChunk = (Document)chunks.first();
        final Document minMap = (Document)firstChunk.get("min");
        final Document maxMap = (Document)firstChunk.get("max");
        return minMap.size() > 1 || maxMap.size() > 1;
    }
    
    private FindIterable<Document> getChunkCollection(final MongoConnection client) {
        assert this.topology.canGetChunks() && this.topology.canConnectToMongods();
        final MongoDatabase db = client.getDatabase("config");
        final com.mongodb.client.MongoCollection<Document> chunksCollection = (com.mongodb.client.MongoCollection<Document>)db.getCollection("chunks");
        final Document filter = new Document("ns", this.collection.toName());
        final Document projection = new Document().append("shard", MongoChunks.SELECT).append("min", MongoChunks.SELECT).append("max", MongoChunks.SELECT);
        return (FindIterable<Document>)chunksCollection.find((Bson)filter).projection((Bson)projection);
    }
    
    private List<MongoChunk> handleShardedCollection(final MongoConnection connection) {
        final List<MongoChunk> chunks = new ArrayList<MongoChunk>();
        final MongoCursor<Document> iterator = (MongoCursor<Document>)this.getChunkCollection(connection).iterator();
        final Map<String, MongoChunk> addresses = Maps.newHashMap();
        for (final MongoTopology.ReplicaSet sets : this.topology) {
            final MongoChunk chunk = sets.getAsChunk(0);
            addresses.put(sets.getName(), chunk);
        }
        while (iterator.hasNext()) {
            final Document chunkObj = (Document)iterator.next();
            final String shardName = (String)chunkObj.get("shard");
            final MongoChunk chunk = new MongoChunk(addresses.get(shardName).getChunkLocList());
            final Document minMap = (Document)chunkObj.get("min");
            chunk.setMinFilters(minMap);
            final Document maxMap = (Document)chunkObj.get("max");
            chunk.setMaxFilters(maxMap);
            chunks.add(chunk);
        }
        if (chunks.isEmpty()) {
            return handleSimpleCase(connection);
        }
        return chunks;
    }
    
    @Override
    public Iterator<MongoChunk> iterator() {
        return (Iterator<MongoChunk>)Iterators.unmodifiableIterator((Iterator)this.chunks.iterator());
    }
    
    public int size() {
        return this.chunks.size();
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MongoChunks for ");
        sb.append(this.collection.toName());
        sb.append("\n");
        for (final MongoChunk c : this) {
            sb.append("\t");
            sb.append(c.toString());
            sb.append("\n");
        }
        return sb.toString();
    }
    
    static {
        SELECT = 1;
        logger = LoggerFactory.getLogger(MongoChunks.class);
    }
}
