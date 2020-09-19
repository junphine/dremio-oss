package com.dremio.plugins.mongo.planning.rels;

import com.dremio.exec.planner.physical.*;
import com.dremio.plugins.mongo.planning.rules.*;
import com.dremio.plugins.mongo.planning.*;
import com.dremio.exec.expr.fn.*;
import com.dremio.exec.record.*;

public interface MongoRel extends Prel
{
    MongoScanSpec implement(final MongoImplementor p0);
    
    BatchSchema getSchema(final FunctionLookupContext p0);
}
