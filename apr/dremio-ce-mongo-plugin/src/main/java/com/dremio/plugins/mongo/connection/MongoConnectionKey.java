package com.dremio.plugins.mongo.connection;

import com.mongodb.*;

public class MongoConnectionKey
{
    private final ServerAddress address;
    private final String user;
    
    public MongoConnectionKey(final ServerAddress address, final String user) {
        this.address = address;
        this.user = user;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = 31 * result + ((this.address == null) ? 0 : this.address.hashCode());
        result = 31 * result + ((this.user == null) ? 0 : this.user.hashCode());
        return result;
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
        final MongoConnectionKey other = (MongoConnectionKey)obj;
        if (this.address == null) {
            if (other.address != null) {
                return false;
            }
        }
        else if (!this.address.equals(other.address)) {
            return false;
        }
        if (this.user == null) {
            if (other.user != null) {
                return false;
            }
        }
        else if (!this.user.equals(other.user)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "[address=" + this.address.toString() + ", user=" + this.user + "]";
    }
}
