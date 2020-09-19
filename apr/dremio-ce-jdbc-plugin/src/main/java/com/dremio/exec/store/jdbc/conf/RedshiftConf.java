package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.store.jdbc.dialect.*;
import io.protostuff.*;
import com.dremio.exec.catalog.conf.*;
import com.dremio.exec.server.*;
import java.net.*;
import com.dremio.security.*;
import java.sql.*;
import java.io.*;
import com.google.common.base.*;
import com.dremio.exec.store.jdbc.*;
import java.util.*;
import com.dremio.exec.store.jdbc.legacy.*;
import com.dremio.exec.store.jdbc.dialect.arp.*;
import com.google.common.annotations.*;

@SourceType(value = "REDSHIFT", label = "Amazon Redshift", uiConfig = "redshift-layout.json")
public class RedshiftConf extends LegacyCapableJdbcConf<RedshiftConf>
{
    private static final String ARP_FILENAME = "arp/implementation/redshift-arp.yaml";
    private static final RedshiftDialect REDSHIFT_ARP_DIALECT;
    private static final String DRIVER = "com.amazon.redshift.jdbc.Driver";
    @Tag(1)
    @DisplayMetadata(label = "JDBC Connection String")
    public String connectionString;
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
    @DisplayMetadata(label = "Secret resource url")
    public String secretResourceUrl;
    @Tag(10)
    @NotMetadataImpacting
    @DisplayMetadata(label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)")
    public boolean enableExternalQuery;
    
    public RedshiftConf() {
        this.fetchSize = 200;
        this.useLegacyDialect = false;
        this.enableExternalQuery = false;
    }
    
    @Override
    protected JdbcStoragePlugin.Config toPluginConfig(final SabotContext context) {
        return JdbcStoragePlugin.Config.newBuilder().withDialect(this.getDialect()).withDatasourceFactory(() -> this.newDataSource(context.getCredentialsService())).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).build();
    }
    
    private CloseableDataSource newDataSource(final CredentialsService credentialsService) throws SQLException {
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
        return DataSources.newGenericConnectionPoolDataSource("com.amazon.redshift.jdbc.Driver", (String)Preconditions.checkNotNull(this.connectionString, "missing connection URL"), this.username, (credsFromCredentialsService != null) ? credsFromCredentialsService.getPassword() : this.password, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
    }
    
    @Override
    protected LegacyDialect getLegacyDialect() {
        return RedshiftLegacyDialect.INSTANCE;
    }
    
    @Override
    protected ArpDialect getArpDialect() {
        return RedshiftConf.REDSHIFT_ARP_DIALECT;
    }
    
    @Override
    protected boolean getLegacyFlag() {
        return this.useLegacyDialect;
    }
    
    public static RedshiftConf newMessage() {
        final RedshiftConf result = new RedshiftConf();
        result.useLegacyDialect = true;
        return result;
    }
    
    @VisibleForTesting
    public static RedshiftDialect getDialectSingleton() {
        return RedshiftConf.REDSHIFT_ARP_DIALECT;
    }
    
    static {
        REDSHIFT_ARP_DIALECT = AbstractArpConf.loadArpFile("arp/implementation/redshift-arp.yaml", RedshiftDialect::new);
    }
}
