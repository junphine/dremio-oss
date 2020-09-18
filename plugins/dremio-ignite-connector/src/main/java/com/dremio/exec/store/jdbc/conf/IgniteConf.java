package com.dremio.exec.store.jdbc.conf;
import com.dremio.common.dialect.DremioSqlDialect;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin.Config;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration for SQLite sources.
 */
@SourceType(value = "Ignite", label = "Ignite")
public class IgniteConf extends AbstractArpConf<IgniteConf> {
    private static final Logger logger = LoggerFactory.getLogger(IgniteConf.class);
    private static final String ARP_FILENAME = "arp/implementation/ignite-arp.yaml";
    private static final ArpDialect ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, (IgniteDialect::new));
    private static final String DRIVER = "org.apache.ignite.IgniteJdbcThinDriver";

    @Tag(1)
    @DisplayMetadata(label = "ServerAddress,Eg:localhost:10800")
    public String serverAddress;

    @Tag(2)
    @DisplayMetadata(label = "Record fetch size")
    @NotMetadataImpacting
    public int fetchSize = 200;
    
    
	   /**
	    The following block is required as Snowflake reports integers as NUMBER(38,0).
	   */
	  static class IgniteSchemaFetcher extends JdbcSchemaFetcher {
	
	    public IgniteSchemaFetcher(String name, DataSource dataSource, int timeout, Config config) {
	      super(name, dataSource, timeout, config);      
	    }
	
	   
	    @Override
	    protected boolean usePrepareForColumnMetadata() {
	      return false;
	    }
	  }

    static class IgniteDialect extends ArpDialect {

        public IgniteDialect(ArpYaml yaml) {
            super(yaml);
        }
        
        @Override
        public JdbcSchemaFetcher getSchemaFetcher(String name, DataSource dataSource, int timeout, JdbcStoragePlugin.Config config) {           
            JdbcSchemaFetcher fetcher = new IgniteSchemaFetcher(name, dataSource, timeout, config);
            return fetcher;
        }

        //@Override
        public JdbcSchemaFetcher getSchemaFetcher_2(String name, DataSource dataSource, int timeout, JdbcStoragePlugin.Config config) {
            String tableQuery = "SELECT NULL AS CAT, TABLE_SCHEMA AS SCH, TABLE_NAME AS NME from information_schema.tables WHERE 1 = 1";
            JdbcSchemaFetcher fetcher = new ArpDialect.ArpSchemaFetcher(tableQuery, name, dataSource, timeout, config,false,false);
            return fetcher;
        }
        
        @Override
        public DremioSqlDialect.ContainerSupport supportsCatalogs() {
            return DremioSqlDialect.ContainerSupport.UNSUPPORTED;
        }
        
        @Override
        public boolean supportsNestedAggregations() {
            return false;
        }

    }


    @VisibleForTesting    
    public String toJdbcConnectionString() {
        final String serverAddress = checkNotNull(this.serverAddress, "Missing serverAddress.");

        return String.format("jdbc:ignite:thin://%s", serverAddress);
    }

    @Override
    @VisibleForTesting
    public Config toPluginConfig(SabotContext context) {
        return JdbcStoragePlugin.Config.newBuilder()
            .withDialect(getDialect())
            .withFetchSize(fetchSize)
            .withDatasourceFactory(this::newDataSource)
            .build();
    }

    private CloseableDataSource newDataSource() {
        return DataSources.newGenericConnectionPoolDataSource(DRIVER,
            toJdbcConnectionString(), null, null, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
    }

    @Override
    public ArpDialect getDialect() {
        return ARP_DIALECT;
    }

}