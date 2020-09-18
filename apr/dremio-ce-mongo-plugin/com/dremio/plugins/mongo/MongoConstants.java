package com.dremio.plugins.mongo;

import com.dremio.plugins.*;
import org.bson.*;

public interface MongoConstants
{
    public static final String ID = "_id";
    public static final String NS = "ns";
    public static final String SHARD = "shard";
    public static final String HOSTS = "hosts";
    public static final String CHUNKS = "chunks";
    public static final String KEY = "key";
    public static final String HASHED = "hashed";
    public static final String CONFIG_DB = "config";
    public static final String LOCAL_DB = "local";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String PRIMARY = "primary";
    public static final String DATABASES = "databases";
    public static final String ADMIN_DB = "admin";
    public static final String OK = "ok";
    public static final String SYSCOLLECTION_PREFIX = "system.";
    public static final Version VERSION_0_0 = new Version(0, 0, 0);
    public static final Version VERSION_3_2 = new Version(3, 2, 0);
    public static final Document PING_REQ = new Document("ping", (Object)1);
}
