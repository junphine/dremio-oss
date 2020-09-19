package com.dremio.exec.store.jdbc;

import javax.sql.*;
import java.io.*;
import java.util.logging.*;
import java.sql.*;

public interface CloseableDataSource extends DataSource, AutoCloseable
{
    default <DS extends javax.sql.DataSource> CloseableDataSource wrap(final DS datasource) {
        if (datasource instanceof CloseableDataSource) {
            return (CloseableDataSource)datasource;
        }
        return new DatasourceWrapper((DataSource)datasource);
    }
    
    public static final class DatasourceWrapper<DS extends javax.sql.DataSource> implements CloseableDataSource
    {
        private final DS datasource;
        
        DatasourceWrapper(final DS datasource) {
            this.datasource = datasource;
        }
        
        @Override
        public void close() throws Exception {
            ((AutoCloseable)this.datasource).close();
        }
        
        @Override
        public <T> T unwrap(final Class<T> iface) throws SQLException {
            return this.datasource.unwrap(iface);
        }
        
        @Override
        public boolean isWrapperFor(final Class<?> iface) throws SQLException {
            return this.datasource.isWrapperFor(iface);
        }
        
        @Override
        public void setLoginTimeout(final int seconds) throws SQLException {
            this.datasource.setLoginTimeout(seconds);
        }
        
        @Override
        public void setLogWriter(final PrintWriter out) throws SQLException {
            this.datasource.setLogWriter(out);
        }
        
        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return this.datasource.getParentLogger();
        }
        
        @Override
        public int getLoginTimeout() throws SQLException {
            return this.datasource.getLoginTimeout();
        }
        
        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return this.datasource.getLogWriter();
        }
        
        @Override
        public Connection getConnection(final String username, final String password) throws SQLException {
            return this.datasource.getConnection(username, password);
        }
        
        @Override
        public Connection getConnection() throws SQLException {
            return this.datasource.getConnection();
        }
    }
    
    @FunctionalInterface
    public interface Factory
    {
        CloseableDataSource newDataSource() throws SQLException;
    }
}
