package com.dremio.plugins.mongo.connection;

import com.dremio.plugins.mongo.metadata.*;
import com.mongodb.client.*;
import com.dremio.common.exceptions.*;
import com.dremio.plugins.mongo.*;
import org.bson.conversions.*;
import org.bson.*;
import com.mongodb.*;
import org.slf4j.*;

public class MongoConnection
{
    private static final Logger logger;
    private final MongoClient client;
    private final String authDatabase;
    private final String user;
    
    public MongoConnection(final MongoClient client, final String authDatabase, final String user) {
        this.client = client;
        this.authDatabase = authDatabase;
        this.user = user;
    }
    
    public MongoCollections getCollections() {
        return MongoCollections.fetchDatabaseAndCollectionNames(this.client, this.user, this.authDatabase);
    }
    
    public ServerAddress getAddress() {
        return this.client.getAddress();
    }
    
    public MongoClient getClient() {
        return this.client;
    }
    
    public MongoDatabase getDatabase(final String name) {
        return this.client.getDatabase(name);
    }
    
    public void authenticate() {
        if (this.user == null || this.user.isEmpty()) {
            return;
        }
        final MongoDatabase db = this.client.getDatabase(this.authDatabase);
        if (db == null) {
            throw UserException.connectionError().message("Could not find/access authentication database %s.", new Object[] { this.authDatabase }).build(MongoConnection.logger);
        }
        final String errorMessage = "Could not authenticate user `" + this.user + "` for database `" + this.authDatabase + "`";
        try {
            final Document result = MongoDocumentHelper.runMongoCommand(db, (Bson)MongoConstants.PING_REQ);
            if (!MongoDocumentHelper.checkResultOkay(result, errorMessage)) {
                throw UserException.connectionError().message("Could not find/access authentication database %s.", new Object[] { this.authDatabase }).build(MongoConnection.logger);
            }
        }
        catch (MongoClientException | MongoServerException ex2) {
            final MongoException ex;
            final MongoException e = ex;
            throw UserException.connectionError().message("Could not find/access authentication database %s.", new Object[] { this.authDatabase }).build(MongoConnection.logger);
        }
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)MongoConnection.class);
    }
}
