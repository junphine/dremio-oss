package com.dremio.exec.store.jdbc;

import java.util.*;

import com.dremio.exec.store.jdbc.CloseableDataSource.DatasourceWrapper;
import com.google.common.base.*;
import java.sql.*;
import org.apache.commons.dbcp2.*;
import javax.sql.*;
import org.apache.commons.dbcp2.datasources.*;

public final class DataSources
{
    public static CloseableDataSource newGenericConnectionPoolDataSource(final String driver, final String url, final String username, final String password, final Properties properties, final CommitMode commitMode) {
        Preconditions.checkNotNull(url);
        try {
            Class.forName((String)Preconditions.checkNotNull(driver)).asSubclass(Driver.class);
        }
        catch (ClassNotFoundException | ClassCastException ex2) {          
            final Exception e = ex2;
            throw new IllegalArgumentException(String.format("String '%s' does not denote a valid java.sql.Driver class name.", driver), e);
        }
        final BasicDataSource source = new BasicDataSource();
        source.setMaxTotal(Integer.MAX_VALUE);
        source.setTestOnBorrow(true);
        source.setValidationQueryTimeout(1);
        source.setDriverClassName(driver);
        source.setUrl(url);
        if (properties != null) {
            properties.forEach((name, value) -> source.addConnectionProperty(name.toString(), value.toString()));
        }
        if (username != null) {
            source.setUsername(username);
        }
        if (password != null) {
            source.setPassword(password);
        }
        switch (commitMode) {
            case FORCE_AUTO_COMMIT_MODE: {
                source.setDefaultAutoCommit(true);
                break;
            }
            case FORCE_MANUAL_COMMIT_MODE: {
                source.setDefaultAutoCommit(false);
                break;
            }
        }
        return wrap(source);
    }
    
    public static CloseableDataSource newSharedDataSource(final ConnectionPoolDataSource source) {
        final SharedPoolDataSource ds = new SharedPoolDataSource();
        ds.setConnectionPoolDataSource(source);
        ds.setMaxTotal(Integer.MAX_VALUE);
        ds.setDefaultTestOnBorrow(true);
        ds.setValidationQueryTimeout(1);
        return wrap(ds);
    }
    
    static <DS extends javax.sql.DataSource> CloseableDataSource wrap(final DS datasource) {
        if (datasource instanceof CloseableDataSource) {
            return (CloseableDataSource)datasource;
        }
        return new DatasourceWrapper((DataSource)datasource);
    }
    
    public enum CommitMode
    {
        FORCE_AUTO_COMMIT_MODE, 
        FORCE_MANUAL_COMMIT_MODE, 
        DRIVER_SPECIFIED_COMMIT_MODE;
    }
}
