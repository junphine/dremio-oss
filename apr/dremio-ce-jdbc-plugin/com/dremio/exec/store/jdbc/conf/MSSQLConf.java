package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.jdbc.dialect.*;
import io.protostuff.*;
import javax.validation.constraints.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.server.*;
import javax.sql.*;
import org.apache.commons.lang3.reflect.*;
import com.dremio.exec.store.jdbc.*;
import java.sql.*;
import java.lang.reflect.*;
import com.google.common.base.*;
import com.google.common.annotations.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.store.jdbc.dialect.arp.*;

@SourceType(value = "MSSQL", label = "Microsoft SQL Server", uiConfig = "mssql-layout.json")
public class MSSQLConf extends LegacyCapableJdbcConf<MSSQLConf>
{
    private static final String ARP_FILENAME = "arp/implementation/mssql-arp.yaml";
    private static final MSSQLDialect MS_ARP_DIALECT;
    private static final String POOLED_DATASOURCE = "com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource";
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
    @DisplayMetadata(label = "Username")
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
    @DisplayMetadata(label = "Database (optional)")
    public String database;
    @Tag(9)
    @DisplayMetadata(label = "Show only the initial database used for connecting")
    public boolean showOnlyConnectionDatabase;
    @Tag(10)
    @DisplayMetadata(label = "Enable legacy dialect")
    public boolean useLegacyDialect;
    @Tag(11)
    @DisplayMetadata(label = "Encrypt connection")
    @NotMetadataImpacting
    public boolean useSsl;
    @Tag(12)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Verify server certificate")
    public boolean enableServerVerification;
    @Tag(13)
    @NotMetadataImpacting
    @DisplayMetadata(label = "SSL/TLS server certificate distinguished name")
    public String hostnameOverride;
    @Tag(14)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)")
    public boolean enableExternalQuery;
    
    public MSSQLConf() {
        this.port = "1433";
        this.fetchSize = 200;
        this.showOnlyConnectionDatabase = false;
        this.useLegacyDialect = false;
        this.useSsl = false;
        this.enableServerVerification = true;
        this.enableExternalQuery = false;
    }
    
    @Override
    protected JdbcStoragePlugin.Config toPluginConfig(final SabotContext context) {
        return JdbcStoragePlugin.Config.newBuilder().withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withDatabase(this.database).withShowOnlyConnDatabase(this.showOnlyConnectionDatabase).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).build();
    }
    
    private CloseableDataSource newDataSource() throws SQLException {
        final String url = this.toJdbcConnectionString();
        try {
            final ConnectionPoolDataSource source = (ConnectionPoolDataSource)Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource").newInstance();
            MethodUtils.invokeExactMethod((Object)source, "setURL", new Object[] { url });
            if (this.username != null) {
                MethodUtils.invokeExactMethod((Object)source, "setUser", new Object[] { this.username });
            }
            if (this.password != null) {
                MethodUtils.invokeExactMethod((Object)source, "setPassword", new Object[] { this.password });
            }
            return DataSources.newSharedDataSource(source);
        }
        catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause != null) {
                Throwables.throwIfInstanceOf(cause, (Class)SQLException.class);
            }
            throw new RuntimeException("Cannot instantiate MSSQL datasource", e);
        }
        catch (ReflectiveOperationException e2) {
            throw new RuntimeException("Cannot instantiate MSSQL datasource", e2);
        }
    }
    
    @VisibleForTesting
    String toJdbcConnectionString() {
        final String hostname = (String)Preconditions.checkNotNull((Object)this.hostname, (Object)"missing hostname");
        final StringBuilder urlBuilder = new StringBuilder("jdbc:sqlserver://").append(hostname);
        if (!Strings.isNullOrEmpty(this.port)) {
            urlBuilder.append(":").append(this.port);
        }
        if (!Strings.isNullOrEmpty(this.database)) {
            urlBuilder.append(";databaseName=").append(this.database);
        }
        if (this.useSsl) {
            urlBuilder.append(";encrypt=true");
            if (!this.enableServerVerification) {
                urlBuilder.append(";trustServerCertificate=true");
            }
            else if (!Strings.isNullOrEmpty(this.hostnameOverride)) {
                urlBuilder.append(";hostNameInCertificate=").append(this.hostnameOverride);
            }
        }
        return urlBuilder.toString();
    }
    
    @Override
    protected LegacyDialect getLegacyDialect() {
        return MSSQLLegacyDialect.INSTANCE;
    }
    
    @Override
    protected ArpDialect getArpDialect() {
        return MSSQLConf.MS_ARP_DIALECT;
    }
    
    @Override
    protected boolean getLegacyFlag() {
        return this.useLegacyDialect;
    }
    
    public static MSSQLConf newMessage() {
        final MSSQLConf result = new MSSQLConf();
        result.useLegacyDialect = true;
        return result;
    }
    
    @VisibleForTesting
    public static MSSQLDialect getDialectSingleton() {
        return MSSQLConf.MS_ARP_DIALECT;
    }
    
    static {
        MS_ARP_DIALECT = AbstractArpConf.loadArpFile("arp/implementation/mssql-arp.yaml", MSSQLDialect::new);
    }
}
