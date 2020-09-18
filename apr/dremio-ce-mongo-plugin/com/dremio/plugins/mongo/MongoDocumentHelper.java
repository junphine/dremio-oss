package com.dremio.plugins.mongo;

import com.mongodb.client.*;
import org.bson.conversions.*;
import org.bson.*;
import com.mongodb.*;
import org.slf4j.*;

public class MongoDocumentHelper
{
    private static final Logger logger;
    
    public static Document runMongoCommand(final MongoDatabase db, final Bson command) {
        Document result = null;
        try {
            result = db.runCommand(command);
        }
        catch (MongoClientException | MongoServerException ex2) {
            final MongoException ex;
            final MongoException e = ex;
            MongoDocumentHelper.logger.error("Failed to run command `" + command.toString() + "`, exception : " + e);
            return new Document("Exception", (Object)e.toString());
        }
        return result;
    }
    
    public static boolean checkResultOkay(final Document result, final String errorMsg) {
        if (result == null || !result.containsKey((Object)"ok") || !Double.valueOf(1.0).equals(result.getDouble((Object)"ok"))) {
            MongoDocumentHelper.logger.error("Mongo command returned with invalid return code (not OK), " + errorMsg + " : " + ((result == null) ? "null" : result.toJson().toString()));
            return false;
        }
        return true;
    }
    
    public static Object getObjectFromDocumentWithKey(final Document result, final String key) {
        if (result == null || result.isEmpty()) {
            MongoDocumentHelper.logger.error("Result document is empty, cannot search for subdocument with key, " + key);
            return null;
        }
        if (key == null || key.length() == 0) {
            MongoDocumentHelper.logger.error("Key is empty, cannot search for subdocument with empty key");
            return null;
        }
        final Object subResult = result.get((Object)key);
        if (subResult == null) {
            MongoDocumentHelper.logger.error("Cannot search for subdocument with key, " + key + " in document " + result.toJson().toString());
        }
        return subResult;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)MongoDocumentHelper.class);
    }
}
