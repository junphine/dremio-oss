package com.dremio.plugins.mongo.metadata;

import java.util.*;
import java.util.Objects;

import com.google.common.base.*;

public class MongoCollection
{
    private final String database;
    private final String collection;
    
    public MongoCollection(final String database, final String collection) {
        this.database = database;
        this.collection = collection;
    }
    
    public String getDatabase() {
        return this.database;
    }
    
    public String getCollection() {
        return this.collection;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(this.database, this.collection);
    }
    
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final MongoCollection other = (MongoCollection)obj;
        return Objects.equals(this.database, other.database) && Objects.equals(this.collection, other.collection);
    }
    
    public boolean equalsIgnoreCase(final MongoCollection other) {
        return this.database.equalsIgnoreCase(other.database) && this.collection.equalsIgnoreCase(other.collection);
    }
    
    public String toName() {
        return this.database + '.' + this.collection;
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("database", this.database).add("collection", this.collection).toString();
    }
}
