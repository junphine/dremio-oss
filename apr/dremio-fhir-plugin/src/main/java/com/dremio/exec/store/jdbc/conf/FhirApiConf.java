package com.dremio.exec.store.jdbc.conf;

import com.dremio.common.dialect.DremioSqlDialect;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
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
import software.amazon.awssdk.utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration for SQLite sources.
 */
@SourceType(value = "fhir", label = "FHIR")
public class FhirApiConf extends AbstractArpConf<FhirApiConf> {
	private static final Logger logger = LoggerFactory.getLogger(FhirApiConf.class);
	private static final String ARP_FILENAME = "arp/implementation/fhir-arp.yaml";
	private static final ArpDialect ARP_DIALECT = AbstractArpConf.loadArpFile(ARP_FILENAME, (IgniteDialect::new));
	private static final String THIN_DRIVER = "org.apache.ignite.IgniteJdbcThinDriver";
	private static final String DRIVER = "org.apache.ignite.IgniteJdbcDriver";

	
	@NotBlank
    @Tag(1)
    @DisplayMetadata(label = "Host")
    public String hostname;
    @NotBlank
    @Tag(2)
    @Min(1L)
    @Max(65535L)
    @DisplayMetadata(label = "Port")
    public String port = "10800";
    
    
    @Tag(3)
	@DisplayMetadata(label = "Use client driver,set ignite cfg file,Eg:file:///etc/configs/ignite-jdbc.xml")
	@NotMetadataImpacting
	public String igniteCfg = null;
    
    @Tag(4)
    @DisplayMetadata(label = "User name")
    public String username;
    
    @Tag(5)
    @DisplayMetadata(label = "User password")
    @Secret
    public String password;
    
	@Tag(6)
	@DisplayMetadata(label = "Allows use distributed joins for non collocated data.")
	public boolean distributedJoins = false ;
	
	@Tag(7)
	@DisplayMetadata(label = "Query will be executed only on a local node.")
	public boolean local = false ;	
	

	@Tag(8)
	@DisplayMetadata(label = "Record fetch size")
	@NotMetadataImpacting
	public int fetchSize = 200;	

	/**
	 * The following block is required as Snowflake reports integers as
	 * NUMBER(38,0).
	 */
	static class IgniteSchemaFetcher extends JdbcSchemaFetcher {

		public IgniteSchemaFetcher(String name, DataSource dataSource, int timeout, Config config) {
			super(name, dataSource, timeout, config);
		}

		protected boolean usePrepareForColumnMetadata() {
			return false;
		}

		protected boolean usePrepareForGetTables() {
			return false;
		}
	}

	static class IgniteDialect extends ArpDialect {

		public IgniteDialect(ArpYaml yaml) {
			super(yaml);
		}

		// @Override
		public JdbcSchemaFetcher getSchemaFetcher_1(String name, DataSource dataSource, int timeout,
				JdbcStoragePlugin.Config config) {
			JdbcSchemaFetcher fetcher = new IgniteSchemaFetcher(name, dataSource, timeout, config);
			return fetcher;
		}

		// @Override
		public JdbcSchemaFetcher getSchemaFetcher(String name, DataSource dataSource, int timeout,
				JdbcStoragePlugin.Config config) {
			String tableQuery = "SELECT NULL AS CAT, TABLE_SCHEMA AS SCH, TABLE_NAME AS NME from information_schema.tables WHERE TABLE_SCHEMA != 'INFORMATION_SCHEMA' ";
			JdbcSchemaFetcher fetcher = new ArpDialect.ArpSchemaFetcher(tableQuery, name, dataSource, timeout, config,
					false, false);
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
		String params = "";
		if(this.distributedJoins) {
			params+=";distributedJoins=true";
		}
		if(this.local) {
			params+=";local=true";
		}
		
		if(!StringUtils.isEmpty(this.igniteCfg) && StringUtils.isEmpty(this.hostname)) {

			return String.format("jdbc:ignite:cfg://%s@%s", params,igniteCfg);
		}
		
		if(StringUtils.isEmpty(this.hostname)) {
			hostname = "localhost";
		}		
		return String.format("jdbc:ignite:thin://%s:%s/%s", hostname,port, params);
		
	}

	@Override
	@VisibleForTesting
	public Config toPluginConfig(SabotContext context) {
		return JdbcStoragePlugin.Config.newBuilder()
				.withDialect(getDialect()).withFetchSize(fetchSize)
				.withDatasourceFactory(this::newDataSource)
				.withAllowExternalQuery(true)
				.clearHiddenSchemas().addHiddenSchema("INFORMATION_SCHEMA")
				.build();
	}

	private CloseableDataSource newDataSource() {
		if(!StringUtils.isEmpty(this.igniteCfg) && StringUtils.isEmpty(this.hostname)) {
			return DataSources.newGenericConnectionPoolDataSource(DRIVER, toJdbcConnectionString(), username, password, null,
				DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
		}
		return DataSources.newGenericConnectionPoolDataSource(THIN_DRIVER, toJdbcConnectionString(), username, password, null,
				DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
		
	}

	@Override
	public ArpDialect getDialect() {
		return ARP_DIALECT;
	}

}