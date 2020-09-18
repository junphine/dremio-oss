package com.dremio.plugins.mongo.metadata;

import org.bson.*;
import com.dremio.plugins.mongo.*;
import org.bson.conversions.*;
import com.google.common.collect.*;
import java.util.*;
import com.mongodb.client.*;
import com.mongodb.*;
import org.slf4j.*;

public class MongoCollections implements Iterable<MongoCollection>
{
    private static final Logger logger;
    public static final String DB = "db";
    public static final String USER = "user";
    private final ImmutableList<MongoCollection> allCollections;
    private final boolean canDBStats;
    private final String databaseToDBStats;
    private final String authenticationDB;
    static final /* synthetic */ boolean $assertionsDisabled;
    
    public MongoCollections(final ImmutableList<MongoCollection> databasesAndCollections, final boolean canDBStats, final String databaseToDBStats, final String authenticationDB) {
        if (databasesAndCollections == null) {
            this.allCollections = (ImmutableList<MongoCollection>)ImmutableList.of();
        }
        else {
            this.allCollections = databasesAndCollections;
        }
        this.canDBStats = canDBStats;
        this.databaseToDBStats = databaseToDBStats;
        this.authenticationDB = authenticationDB;
        if (this.canDBStats && !MongoCollections.$assertionsDisabled && (databaseToDBStats == null || databaseToDBStats.isEmpty())) {
            throw new AssertionError((Object)"Database to run dbStats command cannot be null");
        }
    }
    
    public List<MongoCollection> getDatabasesAndCollections() {
        return (List<MongoCollection>)this.allCollections;
    }
    
    public boolean canDBStats() {
        return this.canDBStats;
    }
    
    public String getDatabaseToDBStats() {
        return this.databaseToDBStats;
    }
    
    public String getAuthenticationDB() {
        return this.authenticationDB;
    }
    
    public boolean isEmpty() {
        return this.allCollections.isEmpty();
    }
    
    public int getNumDatabases() {
        return this.allCollections.size();
    }
    
    public ImmutableList<MongoCollection> getCollections() {
        return this.allCollections;
    }
    
    public static MongoCollections fetchDatabaseAndCollectionNames(final MongoClient client, final String user, final String authenticationDatabase) {
        List<String> listAllNonSystemDatabases = null;
        final List<String> listAllSystemDatabases = new ArrayList<String>(2);
        String authDatabase;
        if (authenticationDatabase == null || authenticationDatabase.isEmpty()) {
            authDatabase = "admin";
        }
        else {
            authDatabase = authenticationDatabase;
        }
        if (user == null || user.isEmpty()) {
            final List<String> databases = getDatabasesFromListDatabases(client, listAllSystemDatabases);
            final ImmutableList<MongoCollection> collections = getAllCollections(client, databases, null, null);
            return new MongoCollections(collections, true, authDatabase, authDatabase);
        }
        final MongoDatabase authDB = client.getDatabase(authDatabase);
        final Document userDetails = new Document();
        userDetails.put("user", (Object)user);
        userDetails.put("db", (Object)authDatabase);
        final Document getUserInfo = new Document();
        getUserInfo.put("usersInfo", (Object)userDetails);
        getUserInfo.put("showCredentials", (Object)false);
        getUserInfo.put("showPrivileges", (Object)true);
        final Document result = MongoDocumentHelper.runMongoCommand(authDB, (Bson)getUserInfo);
        if (!MongoDocumentHelper.checkResultOkay(result, "Could not get user information for `" + user + "`, authDB `" + authDatabase + "`")) {
            return new MongoCollections((ImmutableList<MongoCollection>)ImmutableList.of(), false, null, authDatabase);
        }
        final List<Document> users = (List<Document>)MongoDocumentHelper.getObjectFromDocumentWithKey(result, "users");
        assert users.size() == 1 : "UserInfo should return a list of size 1 since we only provided one user";
        final Document thisUser = users.get(0);
        final List<Document> privileges = (List<Document>)MongoDocumentHelper.getObjectFromDocumentWithKey(thisUser, "inheritedPrivileges");
        final boolean canListDatabases = canListDatabases(privileges);
        final ImmutableList.Builder<MongoCollection> collections2 = (ImmutableList.Builder<MongoCollection>)ImmutableList.builder();
        boolean canDBStats = false;
        String databaseToDBStats = null;
        for (final Document privDoc : privileges) {
            final Document resource = (Document)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "resource");
            final List<String> actions = (List<String>)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "actions");
            if (resource.get((Object)"db") == null) {
                continue;
            }
            final String resourceDB = resource.getString((Object)"db");
            final String resourceCollection = resource.getString((Object)"collection");
            boolean thisDBCanDBStats = false;
            if (resourceCollection == null || resourceDB == null) {
                continue;
            }
            if (isSystemCollection(resourceCollection)) {
                continue;
            }
            if (resourceCollection.isEmpty() && !canDBStats) {
                thisDBCanDBStats = actions.contains("dbStats");
                if (thisDBCanDBStats) {
                    canDBStats = true;
                    databaseToDBStats = resourceDB;
                }
            }
            if (isSystemDatabase(resourceDB)) {
                continue;
            }
            final boolean canFind = actions.contains("find");
            if (!canFind) {
                continue;
            }
            List<String> databases2 = null;
            if (resourceDB.isEmpty()) {
                if (canListDatabases) {
                    if (listAllNonSystemDatabases == null) {
                        listAllNonSystemDatabases = getDatabasesFromListDatabases(client, listAllSystemDatabases);
                    }
                    databases2 = listAllNonSystemDatabases;
                }
            }
            else {
                databases2 = (List<String>)Lists.newArrayList((Object[])new String[] { resourceDB });
            }
            if (thisDBCanDBStats && (databaseToDBStats == null || databaseToDBStats.isEmpty())) {
                databaseToDBStats = getDBStatsDatabaseName(databases2, listAllSystemDatabases);
                if (databaseToDBStats == null) {
                    canDBStats = false;
                    databaseToDBStats = null;
                }
            }
            collections2.addAll((Iterable)getAllCollections(client, databases2, resourceCollection, actions));
        }
        return new MongoCollections((ImmutableList<MongoCollection>)collections2.build(), canDBStats, databaseToDBStats, authDatabase);
    }
    
    private static String getDBStatsDatabaseName(final List<String> currentDatabases, final List<String> sysDatabases) {
        if (!currentDatabases.isEmpty()) {
            return currentDatabases.get(0);
        }
        if (!sysDatabases.isEmpty()) {
            return sysDatabases.get(0);
        }
        return null;
    }
    
    private static boolean canListDatabases(final List<Document> privileges) {
        for (final Document privDoc : privileges) {
            final Document resource = (Document)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "resource");
            final List<String> actions = (List<String>)MongoDocumentHelper.getObjectFromDocumentWithKey(privDoc, "actions");
            if (resource.get((Object)"cluster") != null) {
                return actions.contains("listDatabases");
            }
        }
        return false;
    }
    
    private static List<String> getDatabasesFromListDatabases(final MongoClient client, final List<String> systemDBs) {
        final List<String> dbNames = new ArrayList<String>();
        for (final String dbName : client.listDatabaseNames()) {
            if (!isSystemDatabase(dbName)) {
                dbNames.add(dbName);
            }
            else {
                systemDBs.add(dbName);
            }
        }
        return dbNames;
    }
    
    private static ImmutableList<MongoCollection> getAllCollections(final MongoClient client, final List<String> databases, final String resourceCollection, final List<String> actions) {
        if (databases == null || databases.isEmpty()) {
            return (ImmutableList<MongoCollection>)ImmutableList.of();
        }
        final ImmutableList.Builder<MongoCollection> collections = (ImmutableList.Builder<MongoCollection>)ImmutableList.builder();
        for (final String dbToAdd : databases) {
            final List<MongoCollection> collectionsToAdd = getCollections(client, dbToAdd, resourceCollection, actions);
            if (collectionsToAdd != null) {
                if (collectionsToAdd.size() == 0) {
                    continue;
                }
                collections.addAll((Iterable)collectionsToAdd);
            }
        }
        return (ImmutableList<MongoCollection>)collections.build();
    }
    
    @Override
    public Iterator<MongoCollection> iterator() {
        return (Iterator<MongoCollection>)this.allCollections.iterator();
    }
    
    private static List<MongoCollection> getCollections(final MongoClient client, final String db, final String collection, final List<String> actions) {
        if (collection == null || collection.isEmpty()) {
            final boolean canListCollection = actions == null || actions.contains("listCollections");
            final MongoDatabase mongoDb = client.getDatabase(db);
            if (!canListCollection) {
                MongoCollections.logger.warn("listCollection privilege was not explicitly given to user for database, " + db);
            }
            final List<MongoCollection> collectionsToReturn = new ArrayList<MongoCollection>();
            try {
                for (final String colName : mongoDb.listCollectionNames()) {
                    if (!isSystemCollection(colName)) {
                        collectionsToReturn.add(new MongoCollection(db, colName));
                    }
                }
            }
            catch (MongoClientException | MongoServerException ex2) {
                final MongoException ex;
                final MongoException e = ex;
                MongoCollections.logger.warn("listCollection failed for database, " + db);
            }
            return collectionsToReturn;
        }
        if (isSystemCollection(collection)) {
            return (List<MongoCollection>)Lists.newArrayList();
        }
        return (List<MongoCollection>)Lists.newArrayList((Object[])new MongoCollection[] { new MongoCollection(db, collection) });
    }
    
    private static boolean isSystemDatabase(final String database) {
        return "local".equalsIgnoreCase(database) || "config".equalsIgnoreCase(database);
    }
    
    private static boolean isSystemCollection(final String collection) {
        return collection != null && collection.startsWith("system.");
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)MongoCollections.class);
    }
}
