package com.dremio.service.users;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.ldaptive.ConnectionConfig;
import org.ldaptive.ConnectionFactory;
import org.ldaptive.DefaultConnectionFactory;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapEntry;
import org.ldaptive.auth.Authenticator;
import org.ldaptive.auth.FormatDnResolver;
import org.ldaptive.auth.PooledBindAuthenticationHandler;
import org.ldaptive.pool.BlockingConnectionPool;
import org.ldaptive.pool.IdlePruneStrategy;
import org.ldaptive.pool.PoolConfig;
import org.ldaptive.pool.PooledConnectionFactory;
import org.ldaptive.pool.SearchValidator;
import org.pac4j.core.profile.definition.CommonProfileDefinition;
import org.pac4j.core.profile.definition.ProfileDefinition;
import org.pac4j.ldap.profile.LdapProfile;

import com.dremio.common.util.FileUtils;
import com.dremio.common.utils.ProtobufUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;


/**
 * ladp object
 * @author WBPC1158
 *
 */
public class LdapPofileConfig {

	public static String BASE_DN = "dc=example,dc=com";
	public static String BASE_PEOPLE_DN = "ou=people,dc=example,dc=com";
	public static String UserFilter = "(objectClass=person)";
	public static String GroupFilter = "(objectClass=organizationalUnit)";

	public final static String CN = "cn";
	public final static String SN = "sn";
	public final static String UID = "uid";
	public final static String ROLE = "role";
	public final static String ROLE1 = "role1";
	public final static String ROLE2 = "role2";
	
	// userField, ldapField
	public static final Map<String,String> ProfileFields = new HashMap<>();
	
	public static final List<String> BaseFields = Arrays.asList(LdapPofileConfig.CN, LdapPofileConfig.SN, "uid", "username", "linkedid","userPassword","");
	
	public static final ProfileDefinition<LdapProfile> ProfileDefinition = new CommonProfileDefinition<>();
	
	private DefaultConnectionFactory connectionFactory;

	private Authenticator authenticator;
	
	public LdapPofileConfig(String configFile) {
		String configJSON = null;
		JsonNode config = null;
		try {
			configJSON = FileUtils.getResourceAsString(configFile);

			config = ProtobufUtils.newMapper().reader().readTree(configJSON);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			File file = new File("conf/" + configFile);
			try {
				configJSON = Resources.toString(file.toURL(), Charsets.UTF_8);

				config = ProtobufUtils.newMapper().reader().readTree(configJSON);

			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		init(config);
	}

	public LdapPofileConfig(JsonNode config) {
		init(config);
	}
	
	public void init(JsonNode config) {

		JsonNode baseDN = config.get("names").get("baseDN");
		if (baseDN != null && !baseDN.isEmpty()) {
			BASE_DN = baseDN.asText();
		}

		JsonNode peopleAttr = config.get("names").get("userAttributes");

		JsonNode basePeopleDN = peopleAttr.get("baseDNs");
		if (basePeopleDN != null && !basePeopleDN.isEmpty()) {
			BASE_PEOPLE_DN = basePeopleDN.get(0).asText();
		}
		
		ProfileFields.clear();		
		for(String primayAttrField: ProfileDefinition.getPrimaryAttributes()) {			
			ProfileFields.put(primayAttrField, primayAttrField);
		}
		
		Iterator<String>  it = peopleAttr.fieldNames();
		while(it.hasNext()) {
			String field = it.next();
			if(field.equalsIgnoreCase("baseDNs") || field.equalsIgnoreCase("searchScope")) {
				continue;
			}
			if(peopleAttr.get(field).isTextual()) {
				String value = peopleAttr.get(field).asText();
				ProfileFields.put(field,value);				
			}
		}
		ProfileFields.remove("username");
		
		JsonNode userFilterNode = config.get("names").get("userFilter");
		if (userFilterNode != null && !userFilterNode.isEmpty()) {
			UserFilter = userFilterNode.asText();
		}
		
		JsonNode groupFilterNode = config.get("names").get("groupFilter");
		if (groupFilterNode != null && !groupFilterNode.isEmpty()) {
			GroupFilter = groupFilterNode.asText();
		}

		int port = config.get("servers").get(0).get("port").asInt();
		String host = config.get("servers").get(0).get("hostname").asText();

		final ConnectionConfig connectionConfig = new ConnectionConfig();
		connectionConfig.setConnectTimeout(Duration.ofMillis(500));
		connectionConfig.setResponseTimeout(Duration.ofSeconds(1));
		connectionConfig.setLdapUrl("ldap://" + host + ":" + port);

		connectionFactory = new DefaultConnectionFactory(connectionConfig);

		PoolConfig poolConfig = new PoolConfig();
		poolConfig.setMinPoolSize(1);
		poolConfig.setMaxPoolSize(2);
		poolConfig.setValidateOnCheckOut(true);
		poolConfig.setValidateOnCheckIn(true);
		poolConfig.setValidatePeriodically(false);
		SearchValidator searchValidator = new SearchValidator();
		IdlePruneStrategy pruneStrategy = new IdlePruneStrategy();

		BlockingConnectionPool connectionPool = new BlockingConnectionPool();
		connectionPool.setPoolConfig(poolConfig);

		connectionPool.setValidator(searchValidator);
		connectionPool.setPruneStrategy(pruneStrategy);
		connectionPool.setConnectionFactory(connectionFactory);
		connectionPool.initialize();

		PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();
		pooledConnectionFactory.setConnectionPool(connectionPool);
		PooledBindAuthenticationHandler handler = new PooledBindAuthenticationHandler();
		handler.setConnectionFactory(pooledConnectionFactory);

		FormatDnResolver dnResolver = new FormatDnResolver();
		dnResolver.setFormat(CN + "=%s," + BASE_PEOPLE_DN);
		Authenticator ldaptiveAuthenticator = new Authenticator();
		ldaptiveAuthenticator.setDnResolver(dnResolver);
		ldaptiveAuthenticator.setAuthenticationHandler(handler);

		authenticator = new Authenticator();
		authenticator.setDnResolver(dnResolver);
		authenticator.setAuthenticationHandler(handler);
	}

	public ConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public Authenticator getAuthenticator() {
		return authenticator;
	}

	public Map<String, Object> getAttributesFromEntry(final LdapEntry entry) {
		final Map<String, Object> attributes = new HashMap<>();
		for (final LdapAttribute attribute : entry.getAttributes()) {
			final String name = attribute.getName();
			if (attribute.size() > 1) {
				attributes.put(name, attribute.getStringValues());
			} else {
				attributes.put(name, attribute.getStringValue());
			}
		}
		attributes.put("username", attributes.get("cn"));
		return attributes;
	}

}
