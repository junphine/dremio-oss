package com.dremio.plugins.mongo.metadata;

import com.dremio.plugins.mongo.connection.*;
import org.bson.*;
import org.bson.conversions.*;
import com.dremio.common.exceptions.*;
import com.google.common.collect.*;
import com.dremio.plugins.*;
import java.util.*;
import com.dremio.plugins.mongo.*;
import com.mongodb.*;
import com.mongodb.client.*;
import org.apache.commons.lang3.*;
import com.google.common.base.*;
import org.slf4j.*;

public class MongoTopology implements Iterable<ReplicaSet>
{
    private static final Logger logger;
    private final ImmutableList<ReplicaSet> replicaSets;
    private final boolean isSharded;
    private final boolean canGetChunks;
    private final boolean canConnectToMongods;
    private final MongoVersion minimumClusterVersion;
    
    public MongoTopology(final List<ReplicaSet> replicaSets, final boolean isSharded, final boolean canGetChunks, final boolean canConnectToMongods, final MongoVersion minimumClusterVersion) {
        this.isSharded = isSharded;
        this.canGetChunks = canGetChunks;
        this.canConnectToMongods = canConnectToMongods;
        this.replicaSets = (ImmutableList<ReplicaSet>)ImmutableList.copyOf((Collection)replicaSets);
        this.minimumClusterVersion = minimumClusterVersion;
    }
    
    public boolean isSharded() {
        return this.isSharded;
    }
    
    public boolean canGetChunks() {
        return this.canGetChunks;
    }
    
    public boolean canConnectToMongods() {
        return this.canConnectToMongods;
    }
    
    public MongoVersion getClusterVersion() {
        return this.minimumClusterVersion;
    }
    
    @Override
    public Iterator<ReplicaSet> iterator() {
        return (Iterator<ReplicaSet>)this.replicaSets.iterator();
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MongoTopology\n");
        sb.append('\n');
        sb.append("\t-Is Sharded Cluster: ");
        sb.append(this.isSharded);
        sb.append("\n\t-Can Get Chunks (applicable only to sharded cluster)?: ");
        sb.append(this.canGetChunks);
        sb.append("\n\t-Can Connect to MongoDs (applicable only to sharded cluster)?: ");
        sb.append(this.canConnectToMongods);
        sb.append("\n\n\tReplicas:\n");
        for (final ReplicaSet r : this.replicaSets) {
            sb.append("\t\t");
            sb.append(r.getName());
            sb.append(": ");
            for (final MongoServer server : r.getNodes()) {
                sb.append(server.address.toString());
                sb.append(" (");
                sb.append(server.role.name().toLowerCase());
                sb.append("), ");
            }
            sb.append("\n");
        }
        return sb.toString();
    }
    
    public static MongoTopology getClusterTopology(final MongoConnectionManager manager, final MongoConnection client, final boolean canDbStats, final String dbStatsDatabase, final String authDatabase, final boolean readOnlySecondary) {
        final MongoDatabase db = client.getDatabase(authDatabase);
        final Document isMasterMsg = MongoDocumentHelper.runMongoCommand(db, (Bson)new Document("isMaster", (Object)1));
        if (!MongoDocumentHelper.checkResultOkay(isMasterMsg, "Failed to run mongo command, isMaster")) {
            throw UserException.dataReadError().message("Unable to retrieve mongo cluster information", new Object[0]).build(MongoTopology.logger);
        }
        final boolean isMongos = "isdbgrid".equalsIgnoreCase(isMasterMsg.getString((Object)"msg"));
        if (!isMongos) {
            MongoTopology.logger.debug("Detected that we are working with mongod according to isMaster response {}", (Object)isMasterMsg);
            final ReplicaSet replica = getReplicaSet("singlenode", client.getAddress(), isMasterMsg, readOnlySecondary);
            final MongoVersion minVersion = getMinVersion(manager, authDatabase, Lists.newArrayList((Object[])new ReplicaSet[] { replica }));
            return new MongoTopology(Collections.singletonList(replica), isMongos, false, false, minVersion);
        }
        MongoTopology.logger.debug("Detected that we are working with mongos according to isMaster response {}", (Object)isMasterMsg);
        if (!canDbStats) {
            MongoTopology.logger.warn("Detected that we are working with mongos, but cannot dbStats any database.  Cannot get mongo shard information");
            return new MongoTopology(Lists.newArrayList(), isMongos, false, false, MongoVersion.getVersionForConnection(db));
        }
        boolean singleThreaded = false;
        final MongoDatabase dbForStats = client.getDatabase(dbStatsDatabase);
        final List<Shard> shards = getShards(dbForStats);
        if (shards.isEmpty()) {
            MongoTopology.logger.warn("Unable to retrieve shards from mongo server, connecting to mongos only");
            singleThreaded = true;
        }
        final List<ReplicaSet> replicas = new ArrayList<ReplicaSet>();
        for (final Shard shard : shards) {
            final ServerAddress randomNode = shard.addresses.get(0);
            final MongoConnection randomClient = manager.getMetadataClient(randomNode);
            final MongoDatabase randomDB = randomClient.getDatabase(authDatabase);
            if (!canConnectTo(randomDB, randomNode)) {
                singleThreaded = true;
                replicas.clear();
                break;
            }
            final Document randomIsMaster = MongoDocumentHelper.runMongoCommand(randomDB, (Bson)new Document("isMaster", (Object)1));
            if (!MongoDocumentHelper.checkResultOkay(randomIsMaster, "Failed to run mongo command, isMaster")) {
                throw UserException.dataReadError().message("Unable to retrieve mongo cluster information", new Object[0]).build(MongoTopology.logger);
            }
            replicas.add(getReplicaSet(shard.name, randomNode, randomIsMaster, readOnlySecondary));
        }
        if (singleThreaded) {
            replicas.add(getReplicaSet("mongosnode", client.getAddress(), isMasterMsg, readOnlySecondary));
        }
        final MongoVersion version = getMinVersion(manager, authDatabase, replicas);
        return new MongoTopology(replicas, isMongos, canGetChunks(client), !singleThreaded, version);
    }
    
    private static MongoVersion getMinVersion(final MongoConnectionManager clientProvider, final String authDb, final List<ReplicaSet> replicaSets) {
        MongoVersion minVersionFoundInCluster = new MongoVersion(Integer.MAX_VALUE, 0, 0);
        final Set<String> hostsCheckedForVersion = new HashSet<String>();
        for (final ReplicaSet replSet : replicaSets) {
            for (final MongoServer server : replSet.getNodes()) {
                final String addr = server.getAddress().toString();
                if (hostsCheckedForVersion.contains(addr)) {
                    continue;
                }
                final MongoDatabase database = clientProvider.getMetadataClient(new ServerAddress(addr)).getDatabase(authDb);
                final MongoVersion version = MongoVersion.getVersionForConnection(database);
                if (version.compareTo((Version)minVersionFoundInCluster) >= 0) {
                    continue;
                }
                minVersionFoundInCluster = version;
            }
        }
        return minVersionFoundInCluster;
    }
    
    private static boolean canConnectTo(final MongoDatabase db, final ServerAddress hostAddr) {
        final String errorMsg = "User does not have access to MongoD instance '{}' in a replica set, this will slow down performance. Entering single thread mode";
        try {
            final Document result = MongoDocumentHelper.runMongoCommand(db, (Bson)MongoConstants.PING_REQ);
            if (MongoDocumentHelper.checkResultOkay(result, "Failed to ping mongo cluster")) {
                return true;
            }
            MongoTopology.logger.warn("User does not have access to MongoD instance '{}' in a replica set, this will slow down performance. Entering single thread mode", (Object)hostAddr);
        }
        catch (MongoClientException | MongoServerException ex2) {
            final MongoException ex;
            final MongoException e = ex;
            MongoTopology.logger.warn("User does not have access to MongoD instance '{}' in a replica set, this will slow down performance. Entering single thread mode", (Object)hostAddr, (Object)e);
        }
        return false;
    }
    
    private static boolean canGetChunks(final MongoConnection client) {
        try {
            final MongoDatabase configDB = client.getDatabase("config");
            final MongoCollection<Document> chunksCollection = (MongoCollection<Document>)configDB.getCollection("chunks");
            final FindIterable<Document> chunkCursor = (FindIterable<Document>)chunksCollection.find().batchSize(1).limit(1);
            if (chunkCursor.iterator().hasNext()) {
                chunkCursor.iterator().next();
                return true;
            }
            MongoTopology.logger.info("Connected to unsharded mongo database, found zero chunks");
            return false;
        }
        catch (Exception e) {
            MongoTopology.logger.warn("User does not have access to config.chunks, this will slow down performance.Entering single threaded mode : " + e);
            return false;
        }
    }
    
    private static ReplicaSet getReplicaSet(final String name, final ServerAddress host, final Document isMasterMsg, final boolean readOnlySecondary) {
        final ArrayList<String> hosts = (ArrayList<String>)isMasterMsg.get((Object)"hosts");
        if (hosts == null) {
            return new ReplicaSet(name, Collections.singletonList(new MongoServer(host, ServerRole.PRIMARY)));
        }
        final String primary = isMasterMsg.getString((Object)"primary");
        final List<MongoServer> nodes = new ArrayList<MongoServer>(hosts.size());
        for (final Object o : hosts) {
            final String hostString = (String)o;
            final ServerRole role = primary.equals(hostString) ? ServerRole.PRIMARY : ServerRole.SECONDARY;
            if (role == ServerRole.SECONDARY || !readOnlySecondary) {
                final String[] hostStringParts = hostString.split(":");
                final MongoServer n = new MongoServer(new ServerAddress(hostStringParts[0], Integer.parseInt(hostStringParts[1])), role);
                nodes.add(n);
            }
        }
        return new ReplicaSet(name, nodes);
    }
    
    private static Shard getShard(final String shardString) {
        final String[] tagAndHost = StringUtils.split(shardString, '/');
        if (tagAndHost.length < 1 || tagAndHost.length > 2) {
            return null;
        }
        final String[] hosts = (tagAndHost.length > 1) ? StringUtils.split(tagAndHost[1], ',') : StringUtils.split(tagAndHost[0], ',');
        final String tag = tagAndHost[0];
        final List<ServerAddress> addresses = (List<ServerAddress>)Lists.newArrayList();
        for (final String host : hosts) {
            final String[] hostAndPort = host.split(":");
            switch (hostAndPort.length) {
                case 1: {
                    addresses.add(new ServerAddress(hostAndPort[0]));
                    break;
                }
                case 2: {
                    try {
                        addresses.add(new ServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
                    }
                    catch (NumberFormatException ex) {}
                    break;
                }
                default: {
                    return null;
                }
            }
        }
        if (addresses.isEmpty()) {
            return null;
        }
        return new Shard(tag, addresses);
    }
    
    private static List<Shard> getShards(final MongoDatabase db) {
        final Document dbStats = MongoDocumentHelper.runMongoCommand(db, (Bson)new Document("dbStats", (Object)1));
        MongoDocumentHelper.checkResultOkay(dbStats, "Mongo command, dbStats, returned error");
        final Document raw = (Document)MongoDocumentHelper.getObjectFromDocumentWithKey(dbStats, "raw");
        if (raw == null) {
            MongoTopology.logger.warn("Attempted to get shards from mongod. Shards will only be available when asking mongos.");
            return Collections.emptyList();
        }
        Preconditions.checkNotNull((Object)raw, (Object)"Tried to get shard info on a non-sharded cluster.");
        final List<Shard> shards = new ArrayList<Shard>();
        for (final String hostEntry : raw.keySet()) {
            final Shard shard = getShard(hostEntry);
            if (shard == null) {
                MongoTopology.logger.warn("Failure trying to get shard names from dbStats. String {} didn't match expected pattern of [name]/host:port,host:port...", (Object)shard);
                return Collections.emptyList();
            }
            shards.add(shard);
        }
        return shards;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)MongoTopology.class);
    }
    
    public enum ServerRole
    {
        PRIMARY, 
        SECONDARY;
    }
    
    public static final class MongoServer
    {
        private final ServerAddress address;
        private final ServerRole role;
        
        public MongoServer(final ServerAddress address, final ServerRole role) {
            this.address = address;
            this.role = role;
        }
        
        public ServerAddress getAddress() {
            return this.address;
        }
        
        public ServerRole getRole() {
            return this.role;
        }
    }
    
    public static final class ReplicaSet
    {
        private final String name;
        private final List<MongoServer> nodes;
        
        public ReplicaSet(final String name, final List<MongoServer> nodes) {
            this.name = name;
            this.nodes = nodes;
        }
        
        public String getName() {
            return this.name;
        }
        
        public List<MongoServer> getNodes() {
            return this.nodes;
        }
        
        public MongoChunk getAsChunk(final int index) {
            final List<ServerAddress> addresses = (List<ServerAddress>)Lists.newArrayList();
            for (final MongoServer s : this.nodes) {
                addresses.add(s.address);
            }
            if (addresses.isEmpty()) {
                throw UserException.dataReadError().message("Unable to find valid nodes to read from. This typically happens when you configure Dremio to avoid reading from primary and no secondary is available.", new Object[0]).build(MongoTopology.logger);
            }
            return MongoChunk.newWithAddresses(addresses);
        }
    }
    
    private static final class Shard
    {
        private final String name;
        private final List<ServerAddress> addresses;
        
        public Shard(final String name, final List<ServerAddress> addresses) {
            this.name = name;
            this.addresses = addresses;
        }
    }
}
