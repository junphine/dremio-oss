package com.dremio.plugins.mongo.planning;

import org.bson.*;
import com.google.common.collect.*;
import java.util.*;

public class MongoFilterMerger
{
    public static Map<String, List<Document>> mergeFilters(final Document minFilters, final Document maxFilters) {
        final Map<String, List<Document>> filters = (Map<String, List<Document>>)Maps.newHashMap();
        if (minFilters != null) {
            for (final Map.Entry<String, Object> entry : minFilters.entrySet()) {
                List<Document> list = filters.get(entry.getKey());
                if (list == null) {
                    list = (List<Document>)Lists.newArrayList();
                    filters.put(entry.getKey(), list);
                }
                list.add(new Document((String)entry.getKey(), (Object)new Document(MongoFunctions.GREATER_OR_EQUAL.getMongoOperator(), entry.getValue())));
            }
        }
        if (maxFilters != null) {
            for (final Map.Entry<String, Object> entry : maxFilters.entrySet()) {
                List<Document> list = filters.get(entry.getKey());
                if (list == null) {
                    list = (List<Document>)Lists.newArrayList();
                    filters.put(entry.getKey(), list);
                }
                list.add(new Document((String)entry.getKey(), (Object)new Document(MongoFunctions.LESS.getMongoOperator(), entry.getValue())));
            }
        }
        return filters;
    }
}
