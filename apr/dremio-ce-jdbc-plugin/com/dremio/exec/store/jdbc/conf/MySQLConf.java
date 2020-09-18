package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.jdbc.dialect.*;
import io.protostuff.*;
import javax.validation.constraints.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.server.*;
import com.google.common.annotations.*;
import javax.sql.*;
import java.sql.*;
import java.util.stream.*;
import org.apache.commons.lang3.reflect.*;
import com.dremio.exec.store.jdbc.*;
import java.lang.reflect.*;
import java.util.*;
import com.google.common.base.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.store.jdbc.dialect.arp.*;

@SourceType(value = "MYSQL", label = "MySQL", uiConfig = "mysql-layout.json")
public class MySQLConf extends LegacyCapableJdbcConf<MySQLConf>
{
    private static final String ARP_FILENAME = "arp/implementation/mysql-arp.yaml";
    private static final MySQLDialect MYSQL_ARP_DIALECT;
    private static final String POOLED_DATASOURCE = "org.mariadb.jdbc.MariaDbDataSource";
    @NotBlank
    @Tag(1)
    @DisplayMetadata(label = "Host")
    public String hostname;
    @NotBlank
    @Tag(2)
    @Min(1L)
    @Max(65535L)
    @DisplayMetadata(label = "Port")
    public String port;
    @Tag(4)
    public String username;
    @Tag(5)
    @Secret
    public String password;
    @Tag(6)
    public AuthenticationType authenticationType;
    @Tag(7)
    @DisplayMetadata(label = "Record fetch size")
    @NotMetadataImpacting
    public int fetchSize;
    @Tag(8)
    @DisplayMetadata(label = "Net write timeout (in seconds)")
    @NotMetadataImpacting
    public int netWriteTimeout;
    @Tag(9)
    @DisplayMetadata(label = "Enable legacy dialect")
    public boolean useLegacyDialect;
    @Tag(10)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)")
    public boolean enableExternalQuery;
    
    public MySQLConf() {
        this.port = "3306";
        this.fetchSize = 200;
        this.netWriteTimeout = 60;
        this.useLegacyDialect = false;
        this.enableExternalQuery = false;
    }
    
    @VisibleForTesting
    public JdbcStoragePlugin.Config toPluginConfig(final SabotContext context) {
        return JdbcStoragePlugin.Config.newBuilder().withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).build();
    }
    
    private CloseableDataSource newDataSource() throws SQLException {
        ConnectionPoolDataSource source;
        try {
            source = (ConnectionPoolDataSource)Class.forName("org.mariadb.jdbc.MariaDbDataSource").newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot instantiate MySQL datasource", e);
        }
        return this.newDataSource(source);
    }
    
    @VisibleForTesting
    CloseableDataSource newDataSource(final ConnectionPoolDataSource source) throws SQLException {
        final String url = this.toJdbcConnectionString();
        final Map<String, String> properties = new LinkedHashMap<String, String>();
        properties.put("useJDBCCompliantTimezoneShift", "true");
        properties.put("sessionVariables", String.format("net_write_timeout=%d", this.netWriteTimeout));
        final String encodedProperties = properties.entrySet().stream().map(entry -> entry.getKey() + '=' + (String)entry.getValue()).collect((Collector<? super Object, ?, String>)Collectors.joining("&"));
        try {
            MethodUtils.invokeExactMethod((Object)source, "setUrl", new Object[] { url });
            if (this.username != null) {
                MethodUtils.invokeExactMethod((Object)source, "setUser", new Object[] { this.username });
            }
            if (this.password != null) {
                MethodUtils.invokeExactMethod((Object)source, "setPassword", new Object[] { this.password });
            }
            MethodUtils.invokeExactMethod((Object)source, "setProperties", new Object[] { encodedProperties });
            return DataSources.newSharedDataSource(source);
        }
        catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause != null) {
                Throwables.throwIfInstanceOf(cause, (Class)SQLException.class);
            }
            throw new RuntimeException("Cannot instantiate MySQL datasource", e);
        }
        catch (ReflectiveOperationException e2) {
            throw new RuntimeException("Cannot instantiate MySQL datasource", e2);
        }
    }
    
    private String toJdbcConnectionString() {
        final String hostname = (String)Preconditions.checkNotNull((Object)this.hostname, (Object)"missing hostname");
        final String portAsString = (String)Preconditions.checkNotNull((Object)this.port, (Object)"missing port");
        final int port = Integer.parseInt(portAsString);
        return String.format("jdbc:mariadb://%s:%d", hostname, port);
    }
    
    @Override
    protected LegacyDialect getLegacyDialect() {
        return MySQLLegacyDialect.INSTANCE;
    }
    
    @Override
    protected ArpDialect getArpDialect() {
        return MySQLConf.MYSQL_ARP_DIALECT;
    }
    
    @VisibleForTesting
    public static MySQLDialect getDialectSingleton() {
        return MySQLConf.MYSQL_ARP_DIALECT;
    }
    
    @Override
    protected boolean getLegacyFlag() {
        return this.useLegacyDialect;
    }
    
    public static MySQLConf newMessage() {
        final MySQLConf result = new MySQLConf();
        result.useLegacyDialect = true;
        return result;
    }
    
    static {
        MYSQL_ARP_DIALECT = AbstractArpConf.loadArpFile("arp/implementation/mysql-arp.yaml", MySQLDialect::new);
    }
}
