package com.dremio.plugins.mongo.metadata;

import com.dremio.plugins.*;
import com.mongodb.client.*;
import org.bson.conversions.*;
import java.util.*;
import org.bson.*;
import com.dremio.service.namespace.*;
import org.slf4j.*;

public class MongoVersion extends Version
{
    private static final Logger logger;
    public static final MongoVersion MIN_MONGO_VERSION;
    public static final MongoVersion MAX_MONGO_VERSION;
    public static final MongoVersion MIN_VERSION_TO_ENABLE_NEW_FEATURES;
    public static final Version NEW_FEATURE_CUTOFF_VERSION;
    private final int compatibilityMajor;
    private final int compatibilityMinor;
    
    public MongoVersion(final int major, final int minor, final int patch) {
        super(major, minor, patch);
        this.compatibilityMajor = major;
        this.compatibilityMinor = minor;
    }
    
    public MongoVersion(final int major, final int minor, final int patch, final int compatibilityMajor, final int compatibilityMinor) {
        super(major, minor, patch);
        this.compatibilityMajor = compatibilityMajor;
        this.compatibilityMinor = compatibilityMinor;
    }
    
    public boolean enableNewFeatures() {
        return this.compareTo(MongoVersion.NEW_FEATURE_CUTOFF_VERSION) >= 0;
    }
    
    static MongoVersion getVersionForConnection(final MongoDatabase database) {
        Version version;
        try {
            final Document versionResult = database.runCommand((Bson)new BsonDocument("buildInfo", (BsonValue)new BsonBoolean(true)));
            version = Version.parse(versionResult.getString((Object)"version"));
        }
        catch (Exception e) {
            MongoVersion.logger.warn("Could not get the mongo version from the server. Defaulting to version: {}", (Object)MongoVersion.MAX_MONGO_VERSION, (Object)e);
            return MongoVersion.MAX_MONGO_VERSION;
        }
        if (version.getMajor() != 3 || version.getMinor() < 4) {
            if (version.getMajor() < 4) {
                return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch());
            }
        }
        Document compatibilityResult;
        try {
            compatibilityResult = database.runCommand((Bson)new BsonDocument((List)Arrays.asList(new BsonElement("getParameter", (BsonValue)new BsonBoolean(true)), new BsonElement("featureCompatibilityVersion", (BsonValue)new BsonBoolean(true)))));
        }
        catch (Exception e2) {
            MongoVersion.logger.warn("Could not get the mongo compatibility version from the server. Defaulting to detected version: {}.{}", new Object[] { version.getMajor(), version.getMinor(), e2 });
            return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch(), version.getMajor(), version.getMinor());
        }
        if (version.getMajor() == 3 && version.getMinor() == 4) {
            final Version compatibility = Version.parse(compatibilityResult.getString((Object)"featureCompatibilityVersion"));
            return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch(), compatibility.getMajor(), compatibility.getMinor());
        }
        if ((version.getMajor() == 3 && version.getMinor() > 4) || version.getMajor() >= 4) {
            final Document featureCompatibilityVersion = (Document)compatibilityResult.get((Object)"featureCompatibilityVersion");
            final Version compatibility2 = Version.parse(featureCompatibilityVersion.getString((Object)"version"));
            return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch(), compatibility2.getMajor(), compatibility2.getMinor());
        }
        return new MongoVersion(version.getMajor(), version.getMinor(), version.getPatch());
    }
    
    public SourceState.Message getVersionInfo() {
        final String message = String.format("MongoDB version %s.", this);
        return new SourceState.Message(SourceState.MessageLevel.INFO, message);
    }
    
    public SourceState.Message getVersionWarning() {
        final String message = String.format("Detected MongoDB version %s. Full query pushdown in Dremio requires version %s", this, MongoVersion.NEW_FEATURE_CUTOFF_VERSION);
        return new SourceState.Message(SourceState.MessageLevel.WARN, message);
    }
    
    public int getCompatibilityMajor() {
        return this.compatibilityMajor;
    }
    
    public int getCompatibilityMinor() {
        return this.compatibilityMinor;
    }
    
    static {
        logger = LoggerFactory.getLogger((Class)MongoVersion.class);
        MIN_MONGO_VERSION = new MongoVersion(1, 0, 0);
        MAX_MONGO_VERSION = new MongoVersion(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        MIN_VERSION_TO_ENABLE_NEW_FEATURES = new MongoVersion(3, 2, 0);
        NEW_FEATURE_CUTOFF_VERSION = new Version(3, 2, 0);
    }
}
