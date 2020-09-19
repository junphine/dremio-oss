package com.dremio.plugins.mongo.connection;

import com.dremio.plugins.mongo.*;
import com.dremio.exec.catalog.conf.*;
import com.google.common.base.*;
import com.google.common.base.Function;
import com.dremio.common.exceptions.*;
import java.util.concurrent.*;
import java.nio.charset.*;
import java.net.*;
import java.io.*;
import com.google.common.collect.*;
import com.google.common.annotations.*;
import java.util.*;
import com.mongodb.*;
import com.dremio.exec.store.*;
import com.dremio.plugins.mongo.metadata.*;
import org.slf4j.*;
import com.google.common.cache.*;

public class MongoConnectionManager implements AutoCloseable
{
    private static final Logger logger;
    private final Cache<MongoConnectionKey, MongoClient> addressClientMap;
    private final MongoClientURI clientURI;
    private final MongoClientOptions clientOptions;
    private final String authDatabase;
    private final String user;
    private final String pluginName;
    private final boolean canReadOnlyOnSecondary;
    private final long authTimeoutInMillis;
    private final long readTimeoutInMillis;
    
    public MongoConnectionManager(final MongoConf config, final String pluginName) {
        final StringBuilder connection = new StringBuilder("mongodb://");
        if (AuthenticationType.MASTER.equals(config.authenticationType)) {
            final String username = config.username;
            if (username != null) {
                connection.append(urlEncode(username));
            }
            final String password = config.password;
            if (password != null) {
                connection.append(":").append(urlEncode(password));
            }
            connection.append("@");
            appendHosts(connection, (Iterable<Host>)Preconditions.checkNotNull(config.hostList, "hostList missing"), ',').append("/");
            final String database = config.authDatabase;
            if (database != null) {
                connection.append(database);
            }
        }
        else {
            appendHosts(connection, (Iterable<Host>)Preconditions.checkNotNull(config.hostList, "hostList missing"), ',').append("/");
        }
        connection.append("?");
        final List<Property> properties = new ArrayList<Property>();
        if (config.propertyList != null) {
            properties.addAll(config.propertyList);
        }
        if (config.useSsl) {
            properties.add(new Property("ssl", "true"));
        }
        connection.append(FluentIterable.from((Iterable)properties).transform(new Function<Property, String>() {
            public String apply(final Property input) {
                if (input.value != null) {
                    return input.name + "=" + input.value;
                }
                return input.name;
            }
        }).join(Joiner.on('&')));
        this.pluginName = pluginName;
        this.authTimeoutInMillis = config.authenticationTimeoutMillis;
        this.canReadOnlyOnSecondary = config.secondaryReadsOnly;
        this.clientURI = new MongoClientURI(connection.toString());
        if (this.clientURI.getHosts().isEmpty()) {
            throw UserException.dataReadError().message("Unable to configure Mongo with empty host list.", new Object[0]).build(MongoConnectionManager.logger);
        }
        this.readTimeoutInMillis = 0L;
        this.user = this.clientURI.getUsername();
        this.authDatabase = ((this.clientURI.getDatabase() == null || this.clientURI.getDatabase().isEmpty()) ? "admin" : this.clientURI.getDatabase());
        this.clientOptions = MongoClientOptionsHelper.newMongoClientOptions(this.clientURI);
        this.addressClientMap = (Cache<MongoConnectionKey, MongoClient>)CacheBuilder.newBuilder().expireAfterAccess(24L, TimeUnit.HOURS).removalListener((RemovalListener)new AddressCloser()).build();
    }
    
    private static StringBuilder appendHosts(final StringBuilder sb, final Iterable<Host> hostList, final char delimiter) {
        final Iterator<Host> iterator = hostList.iterator();
        while (iterator.hasNext()) {
            final Host h = iterator.next();
            sb.append((String)Preconditions.checkNotNull(h.hostname, "hostname missing")).append(":").append(Preconditions.checkNotNull(h.port, "port missing"));
            if (iterator.hasNext()) {
                sb.append(delimiter);
            }
        }
        return sb;
    }
    
    static String getHosts(final Iterable<Host> hostList, final char delimiter) {
        return appendHosts(new StringBuilder(), hostList, delimiter).toString();
    }
    
    public MongoVersion connect() {
        return this.validateConnection();
    }
    
    private static String urlEncode(final String fragment) {
        try {
            return URLEncoder.encode(fragment, StandardCharsets.UTF_8.name());
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError("Expecting UTF_8 to be a supported charset", e);
        }
    }
    
    public MongoConnection getFirstConnection() {
        final String firstHost = this.clientURI.getHosts().iterator().next();
        final String[] firstHostAndPort = firstHost.split(":");
        final ServerAddress address = (firstHostAndPort.length == 2) ? new ServerAddress(firstHostAndPort[0], Integer.parseInt(firstHostAndPort[1])) : new ServerAddress(firstHost);
        return this.getClient((List<ServerAddress>)ImmutableList.of(address), this.authTimeoutInMillis);
    }
    
    public MongoTopology getTopology(final boolean canDbStats, final String dbStatsDatabase) {
        final List<String> hosts = (List<String>)this.clientURI.getHosts();
        final List<ServerAddress> addresses = Lists.newArrayList();
        for (final String host : hosts) {
            addresses.add(new ServerAddress(host));
        }
        final MongoConnection connect = this.getClient(addresses, this.authTimeoutInMillis);
        return MongoTopology.getClusterTopology(this, connect, canDbStats, dbStatsDatabase, this.authDatabase, this.canReadOnlyOnSecondary);
    }
    
    @VisibleForTesting
    MongoClientURI getClientURI() {
        return this.clientURI;
    }
    
    @VisibleForTesting
    boolean getCanReadOnlyOnSecondary() {
        return this.canReadOnlyOnSecondary;
    }
    
    @VisibleForTesting
    long getAuthTimeoutInMillis() {
        return this.authTimeoutInMillis;
    }
    
    private MongoConnection getHost(final long timeout) {
        final List<String> hosts = (List<String>)this.clientURI.getHosts();
        final List<ServerAddress> addresses = Lists.newArrayList();
        for (final String host : hosts) {
            addresses.add(new ServerAddress(host));
        }
        final MongoConnection connect = this.getClient(addresses, timeout);
        return connect;
    }
    
    public MongoConnection getMetadataClient() {
        return this.getHost(this.authTimeoutInMillis);
    }
    
    public MongoConnection getMetadataClient(final ServerAddress host) {
        return this.getClient(Collections.singletonList(host), this.authTimeoutInMillis);
    }
    
    public MongoConnection getReadClient() {
        return this.getHost(this.readTimeoutInMillis);
    }
    
    public MongoConnection getReadClients(final List<ServerAddress> hosts) {
        return this.getClient(hosts, this.readTimeoutInMillis);
    }
    
    private synchronized MongoConnection getClient(final List<ServerAddress> addresses, final long serverSelectionTimeout) {
        if (addresses.isEmpty()) {
            throw UserException.dataReadError().message("Failure while attempting to connect to server without addresses.", new Object[0]).build(MongoConnectionManager.logger);
        }
        final ServerAddress serverAddress = addresses.get(0);
        final MongoCredential credential = this.clientURI.getCredentials();
        final String userName = (credential == null) ? null : credential.getUserName();
        final MongoConnectionKey key = new MongoConnectionKey(serverAddress, userName);
        final List<MongoCredential> credentials = (credential != null) ? Collections.singletonList(credential) : Collections.emptyList();
        MongoClient client = (MongoClient)this.addressClientMap.getIfPresent(key);
        if (client == null) {
            MongoClientOptions localClientOptions = this.clientOptions;
            if (serverSelectionTimeout > 0L) {
                localClientOptions = MongoClientOptions.builder(localClientOptions).serverSelectionTimeout((int)serverSelectionTimeout).build();
            }
            if (addresses.size() > 1) {
                client = new MongoClient(addresses, credentials, localClientOptions);
            }
            else {
                client = new MongoClient((ServerAddress)addresses.get(0), credentials, localClientOptions);
            }
            this.addressClientMap.put(key, client);
            MongoConnectionManager.logger.info("Created connection to {}.", key.toString());
            MongoConnectionManager.logger.info("Number of open connections {}.", this.addressClientMap.size());
            MongoConnectionManager.logger.info("MongoClientOptions:\n" + localClientOptions);
            MongoConnectionManager.logger.info("MongoCredential:" + credential);
        }
        return new MongoConnection(client, this.authDatabase, this.user);
    }
    
    public MongoVersion validateConnection() {
        final MongoConnection client = this.getMetadataClient();
        client.authenticate();
        final MongoCollections fetchResult = client.getCollections();
        if (fetchResult != null && !fetchResult.isEmpty()) {
            final MongoTopology topology = this.getTopology(fetchResult.canDBStats(), fetchResult.getDatabaseToDBStats());
            if (this.authDatabase != null) {
                MongoConnectionManager.logger.debug("Connected as user [{}], using authenticationDatabase [{}]. Found {} collections. {}", new Object[] { this.user, this.authDatabase, fetchResult.getNumDatabases(), topology });
            }
            else {
                MongoConnectionManager.logger.debug("Connected as anonymous user. Found {} collections. {}", fetchResult.getNumDatabases(), topology);
            }
            return topology.getClusterVersion();
        }
        if (this.authDatabase == null) {
            throw StoragePluginUtils.message(UserException.dataReadError(), this.pluginName, "Connection to Mongo failed. System either had no user collections or user %s was unable to access them.", new Object[] { this.user }).build(MongoConnectionManager.logger);
        }
        throw StoragePluginUtils.message(UserException.dataReadError(), this.pluginName, "Connection to Mongo failed. No collections were visible.", new Object[0]).build(MongoConnectionManager.logger);
    }
    
    @Override
    public void close() throws Exception {
        this.addressClientMap.invalidateAll();
    }
    
    static {
        logger = LoggerFactory.getLogger(MongoConnectionManager.class);
    }
    
    private class AddressCloser implements RemovalListener<MongoConnectionKey, MongoClient>
    {
        public synchronized void onRemoval(final RemovalNotification<MongoConnectionKey, MongoClient> removal) {
            ((MongoClient)removal.getValue()).close();
            MongoConnectionManager.logger.debug("Closed connection to {}.", ((MongoConnectionKey)removal.getKey()).toString());
        }
    }
}
