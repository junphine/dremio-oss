package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.jdbc.dialect.*;
import io.protostuff.*;
import javax.validation.constraints.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.server.*;
import java.util.*;
import java.net.*;
import com.dremio.security.*;
import java.sql.*;
import java.io.*;
import com.dremio.exec.store.jdbc.*;
import com.google.common.base.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.store.jdbc.dialect.arp.*;
import com.google.common.annotations.*;

@SourceType(value = "POSTGRES", label = "PostgreSQL", uiConfig = "postgres-layout.json")
public class PostgresConf extends LegacyCapableJdbcConf<PostgresConf>
{
    private static final String ARP_FILENAME = "arp/implementation/postgresql-arp.yaml";
    private static final PostgreSQLDialect PG_ARP_DIALECT;
    private static final String DRIVER = "org.postgresql.Driver";
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
    @DisplayMetadata(label = "Database Name")
    public String databaseName;
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
    @DisplayMetadata(label = "Enable legacy dialect")
    public boolean useLegacyDialect;
    @Tag(9)
    @DisplayMetadata(label = "Encrypt connection")
    @NotMetadataImpacting
    public boolean useSsl;
    @Tag(10)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Validation Mode")
    public EncryptionValidationMode encryptionValidationMode;
    @Tag(11)
    @DisplayMetadata(label = "Secret resource url")
    public String secretResourceUrl;
    @Tag(12)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)")
    public boolean enableExternalQuery;
    
    public PostgresConf() {
        this.port = "5432";
        this.fetchSize = 200;
        this.useLegacyDialect = false;
        this.useSsl = false;
        this.encryptionValidationMode = EncryptionValidationMode.CERTIFICATE_AND_HOSTNAME_VALIDATION;
        this.enableExternalQuery = false;
    }
    
    @Override
    protected JdbcStoragePlugin.Config toPluginConfig(final SabotContext context) {
        return JdbcStoragePlugin.Config.newBuilder().withDialect(this.getDialect()).withDatasourceFactory(() -> this.newDataSource(context.getCredentialsService())).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).build();
    }
    
    private CloseableDataSource newDataSource(final CredentialsService credentialsService) throws SQLException {
        final Properties properties = new Properties();
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
        if (this.useSsl) {
            properties.setProperty("ssl", "true");
            properties.setProperty("sslmode", this.getSslMode());
        }
        properties.setProperty("OpenSourceSubProtocolOverride", "true");
        return DataSources.newGenericConnectionPoolDataSource("org.postgresql.Driver", this.toJdbcConnectionString(), this.username, (credsFromCredentialsService != null) ? credsFromCredentialsService.getPassword() : this.password, properties, DataSources.CommitMode.FORCE_MANUAL_COMMIT_MODE);
    }
    
    private String toJdbcConnectionString() {
        final String hostname = (String)Preconditions.checkNotNull(this.hostname, "missing hostname");
        final String portAsString = (String)Preconditions.checkNotNull(this.port, "missing port");
        final int port = Integer.parseInt(portAsString);
        final String db = (String)Preconditions.checkNotNull(this.databaseName, "missing database");
        return String.format("jdbc:postgresql://%s:%d/%s", hostname, port, db);
    }
    
    private String getSslMode() {
        Preconditions.checkNotNull(this.encryptionValidationMode, "missing validation mode");
        switch (this.encryptionValidationMode) {
            case CERTIFICATE_AND_HOSTNAME_VALIDATION: {
                return "verify-full";
            }
            case CERTIFICATE_ONLY_VALIDATION: {
                return "verify-ca";
            }
            case NO_VALIDATION: {
                return "require";
            }
            default: {
                throw new IllegalStateException(this.encryptionValidationMode + " is unknown");
            }
        }
    }
    
    @Override
    protected LegacyDialect getLegacyDialect() {
        return PostgreSQLLegacyDialect.INSTANCE;
    }
    
    @Override
    protected ArpDialect getArpDialect() {
        return PostgresConf.PG_ARP_DIALECT;
    }
    
    @Override
    protected boolean getLegacyFlag() {
        return this.useLegacyDialect;
    }
    
    public static PostgresConf newMessage() {
        final PostgresConf result = new PostgresConf();
        result.useLegacyDialect = true;
        return result;
    }
    
    @VisibleForTesting
    public static PostgreSQLDialect getDialectSingleton() {
        return PostgresConf.PG_ARP_DIALECT;
    }
    
    static {
        PG_ARP_DIALECT = AbstractArpConf.loadArpFile("arp/implementation/postgresql-arp.yaml", PostgreSQLDialect::new);
    }
}
