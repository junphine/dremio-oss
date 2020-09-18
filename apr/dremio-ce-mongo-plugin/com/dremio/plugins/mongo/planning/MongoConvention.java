package com.dremio.plugins.mongo.planning;

import org.apache.calcite.plan.*;
import com.dremio.plugins.mongo.planning.rels.*;

public class MongoConvention extends Convention.Impl
{
    public static final MongoConvention INSTANCE;
    
    private MongoConvention() {
        super("MONGO", (Class)MongoRel.class);
    }
    
    static {
        INSTANCE = new MongoConvention();
    }
}
