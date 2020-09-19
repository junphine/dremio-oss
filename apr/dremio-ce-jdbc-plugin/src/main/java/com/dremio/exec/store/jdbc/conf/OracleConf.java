package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.jdbc.dialect.*;
import io.protostuff.*;
import javax.validation.constraints.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.server.*;
import javax.sql.*;
import java.sql.*;
import java.net.*;
import com.dremio.security.*;
import java.io.*;
import com.google.common.annotations.*;
import javax.security.auth.x500.*;
import org.apache.commons.lang3.reflect.*;
import com.dremio.exec.store.jdbc.*;
import com.google.common.base.*;
import java.lang.reflect.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.store.jdbc.dialect.arp.*;
import java.util.*;

@SourceType(value = "ORACLE", label = "Oracle", uiConfig = "oracle-layout.json")
public class OracleConf extends LegacyCapableJdbcConf<OracleConf>
{
    private static final String ARP_FILENAME = "arp/implementation/oracle-arp.yaml";
    private static final OracleDialect ORACLE_ARP_DIALECT;
    private static final String POOLED_DATASOURCE = "oracle.jdbc.pool.OracleConnectionPoolDataSource";
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
    @NotBlank
    @Tag(3)
    @DisplayMetadata(label = "Service Name")
    public String instance;
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
    @DisplayMetadata(label = "Encrypt connection")
    @NotMetadataImpacting
    public boolean useSsl;
    @Tag(9)
    @DisplayMetadata(label = "SSL/TLS server certificate distinguished name")
    @NotMetadataImpacting
    public String sslServerCertDN;
    @Tag(10)
    @DisplayMetadata(label = "Use timezone as connection region")
    @NotMetadataImpacting
    public boolean useTimezoneAsRegion;
    @Tag(11)
    @DisplayMetadata(label = "Enable legacy dialect")
    public boolean useLegacyDialect;
    @Tag(12)
    @DisplayMetadata(label = "Include synonyms")
    public boolean includeSynonyms;
    @Tag(13)
    @DisplayMetadata(label = "Secret resource url")
    public String secretResourceUrl;
    @Tag(14)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)")
    public boolean enableExternalQuery;
    
    public OracleConf() {
        this.port = "1521";
        this.fetchSize = 200;
        this.useSsl = false;
        this.useTimezoneAsRegion = true;
        this.useLegacyDialect = false;
        this.includeSynonyms = false;
        this.enableExternalQuery = false;
    }
    
    @Override
    protected JdbcStoragePlugin.Config toPluginConfig(final SabotContext context) {
        final JdbcStoragePlugin.Config.Builder builder = JdbcStoragePlugin.Config.newBuilder().withDialect(this.getDialect()).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).withDatasourceFactory(() -> this.newDataSource(context.getCredentialsService()));
        if (!this.includeSynonyms) {
            builder.addHiddenTableType("SYNONYM", new String[0]);
        }
        return builder.build();
    }
    
    private CloseableDataSource newDataSource(final CredentialsService credentialsService) throws SQLException {
        if (Strings.isNullOrEmpty(this.secretResourceUrl)) {
            Preconditions.checkNotNull(this.username, "missing username");
            Preconditions.checkNotNull(this.password, "missing password");
        }
        Preconditions.checkNotNull(this.hostname, "missing hostname");
        Preconditions.checkNotNull(this.port, "missing port");
        Preconditions.checkNotNull(this.instance, "missing instance");
        ConnectionPoolDataSource source;
        try {
            source = (ConnectionPoolDataSource)Class.forName("oracle.jdbc.pool.OracleConnectionPoolDataSource").newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot instantiate Oracle datasource", e);
        }
        return this.newDataSource(source, credentialsService);
    }
    
    @VisibleForTesting
    CloseableDataSource newDataSource(final ConnectionPoolDataSource dataSource, final CredentialsService credentialsService) throws SQLException {
        final Properties properties = new Properties();
        final int portAsInteger = Integer.parseInt(this.port);
        (properties).put("oracle.jdbc.timezoneAsRegion", Boolean.toString(this.useTimezoneAsRegion));
        (properties).put("includeSynonyms", Boolean.toString(this.includeSynonyms));
        PasswordCredentials credsFromCredentialsService = null;
        if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
            try {
                final URI secretURI = URI.create(this.secretResourceUrl);
                credsFromCredentialsService = (PasswordCredentials)credentialsService.getCredentials(secretURI);
            }
            catch (IOException e) {
                throw new SQLException(e.getMessage(), e);
            }
        }
        if (!this.useSsl) {
            return newDataSource(dataSource, String.format("jdbc:oracle:thin:@//%s:%d/%s", this.hostname, portAsInteger, this.instance), this.username, (credsFromCredentialsService != null) ? credsFromCredentialsService.getPassword() : this.password, properties);
        }
        String securityOption;
        if (!Strings.isNullOrEmpty(this.sslServerCertDN)) {
            this.checkSSLServerCertDN(this.sslServerCertDN);
            securityOption = String.format("(SECURITY = (SSL_SERVER_CERT_DN = \"%s\"))", this.sslServerCertDN);
            (properties).put("oracle.net.ssl_server_dn_match", "true");
        }
        else {
            securityOption = "";
        }
        return newDataSource(dataSource, String.format("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%s))%s)", this.hostname, portAsInteger, this.instance, securityOption), this.username, this.password, properties);
    }
    
    private void checkSSLServerCertDN(final String sslServerCertDN) {
        try {
            new X500Principal(sslServerCertDN);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Server certificate DN '%s' does not respect Oracle syntax", sslServerCertDN), e);
        }
    }
    
    private static CloseableDataSource newDataSource(final ConnectionPoolDataSource source, final String url, final String username, final String password, final Properties properties) throws SQLException {
        try {
            MethodUtils.invokeExactMethod(source, "setURL", new Object[] { url });
            if (properties != null) {
                MethodUtils.invokeExactMethod(source, "setConnectionProperties", new Object[] { properties });
            }
            MethodUtils.invokeExactMethod(source, "setUser", new Object[] { username });
            MethodUtils.invokeExactMethod(source, "setPassword", new Object[] { password });
            return DataSources.newSharedDataSource(source);
        }
        catch (InvocationTargetException e) {
            final Throwable cause = e.getCause();
            if (cause != null) {
                Throwables.propagateIfInstanceOf(cause, SQLException.class);
            }
            throw new RuntimeException("Cannot instantiate Oracle datasource", e);
        }
        catch (ReflectiveOperationException e2) {
            throw new RuntimeException("Cannot instantiate Oracle datasource", e2);
        }
    }
    
    @Override
    protected LegacyDialect getLegacyDialect() {
        return OracleLegacyDialect.INSTANCE;
    }
    
    @Override
    protected ArpDialect getArpDialect() {
        return OracleConf.ORACLE_ARP_DIALECT;
    }
    
    @Override
    protected boolean getLegacyFlag() {
        return this.useLegacyDialect;
    }
    
    public static OracleConf newMessage() {
        final OracleConf result = new OracleConf();
        result.useLegacyDialect = true;
        result.includeSynonyms = true;
        return result;
    }
    
    @VisibleForTesting
    public static OracleDialect getDialectSingleton() {
        return OracleConf.ORACLE_ARP_DIALECT;
    }
    
    static {
        ORACLE_ARP_DIALECT = AbstractArpConf.loadArpFile("arp/implementation/oracle-arp.yaml", OracleDialect::new);
    }
}
