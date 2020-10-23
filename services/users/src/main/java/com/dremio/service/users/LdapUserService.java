/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.users;

import static java.lang.String.format;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.commons.lang3.StringUtils;
import org.ldaptive.Connection;
import org.ldaptive.ConnectionConfig;
import org.ldaptive.DefaultConnectionFactory;
import org.ldaptive.LdapAttribute;
import org.ldaptive.LdapEntry;
import org.ldaptive.LdapException;
import org.ldaptive.SearchFilter;
import org.ldaptive.SearchOperation;
import org.ldaptive.SearchRequest;
import org.ldaptive.SearchResult;
import org.ldaptive.auth.Authenticator;
import org.ldaptive.auth.PooledBindAuthenticationHandler;
import org.ldaptive.pool.PooledConnectionFactory;
import org.pac4j.core.credentials.UsernamePasswordCredentials;
import org.pac4j.core.exception.BadCredentialsException;
import org.pac4j.core.exception.TechnicalException;
import org.pac4j.core.profile.definition.CommonProfileDefinition;
import org.pac4j.ldap.profile.LdapProfile;
import org.pac4j.ldap.profile.service.LdapProfileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.util.FileUtils;
import com.dremio.common.utils.ProtobufUtils;
import com.dremio.datastore.SearchQueryUtils;
import com.dremio.datastore.SearchTypes.SearchFieldSorting;
import com.dremio.datastore.SearchTypes.SearchQuery;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.service.users.proto.UID;
import com.dremio.service.users.proto.UserAuth;
import com.dremio.service.users.proto.UserConfig;
import com.dremio.service.users.proto.UserInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;

import io.protostuff.ByteString;

/**
 * User group service with ldap server store. uid is equals username
 */
public class LdapUserService extends LdapProfileService implements UserService {
	private static final Logger logger = LoggerFactory.getLogger(LdapUserService.class);
	//ldap object to user
	private final Function<Map<String, Object>, User> infoConfigTransformer = new Function<Map<String, Object>, User>() {
		@Override
		public User apply(Map<String, Object> userConfig) {
			return SimpleUser.newBuilder().setUID(new UID((String) userConfig.get(LdapPofileConfig.CN)))
					.setUserName((String) userConfig.get(LdapPofileConfig.CN))
					.setFirstName((String) userConfig.get(CommonProfileDefinition.FIRST_NAME))
					.setLastName((String) userConfig.get(CommonProfileDefinition.FAMILY_NAME))
					.setEmail((String) userConfig.get(CommonProfileDefinition.EMAIL))					
					// .setCreatedAt((Long)userConfig.get("Uid"))
					// .setModifiedAt((Long)userConfig.get("Uid"))
					.setVersion((String) userConfig.get("tag")).build();
		}
	};

	private static final SecretKeyFactory secretKey = buildSecretKey();

	public static SecretKeyFactory buildSecretKey() {
		try {
			return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
		} catch (NoSuchAlgorithmException nsae) {
			throw new RuntimeException("Failed to initialize usergroup service", nsae);
		}
	}

	private static final Pattern PASSWORD_MATCHER = Pattern.compile("(?=.*[0-9])(?=.*[a-zA-Z]).{8,}");
	
	
	// mixed mode
	private SimpleUserService userStore = null;

	// when we call hasAnyUser() we cache the result in here so we never hit the
	// kvStore once the value is true
	private final AtomicBoolean anyUserFound = new AtomicBoolean();

	private static LdapPofileConfig ldapClient;

	@Inject
	public LdapUserService(String configFile, SimpleUserService internalUserService) {
		this.userStore = internalUserService;
		
		if(ldapClient==null) {
			
			ldapClient = new LdapPofileConfig(configFile);		
		}

		this.setConnectionFactory(ldapClient.getConnectionFactory());
		this.setLdapAuthenticator(ldapClient.getAuthenticator());
		this.setUsersDn(LdapPofileConfig.BASE_PEOPLE_DN);
		
		this.setIdAttribute(LdapPofileConfig.CN);
		this.setUsernameAttribute("username");
		this.setPasswordAttribute("userPassword");		
		
		this.setAttributes(StringUtils.join(LdapPofileConfig.ProfileFields.values(),','));
		//ldapProfileService.setProfileDefinition(LdapPofileConfig.ProfileDefinition);
		this.init();
	}

	private UserInfo findUserByUserName(String userName) {
		LdapProfile profile = findById(userName);
		if (profile != null) {
			UserConfig userConfig = toUserConfig(profile);

			String password = (String) profile.getAttribute("userPassword");
			if (password == null) {
				password = "";
			}

			UserInfo userInfo = new UserInfo();
			//userInfo.setAuth(buildUserAuth(userConfig.getUid(), password));
			userInfo.setConfig(userConfig);
			return userInfo;
		}
		return null;
	}

	@Override
	public User getUser(String userName) throws UserNotFoundException {
		if (SystemUser.SYSTEM_USER.getUserName().equals(userName)) {
			return SystemUser.SYSTEM_USER;
		}

		final UserInfo userInfo = findUserByUserName(userName);
		if (userInfo == null) {
			ldapClient = null;
			throw new UserNotFoundException(userName);
		}
		return fromUserConfig(userInfo.getConfig());
	}

	@Override
	public User getUser(UID uid) throws UserNotFoundException {
		return getUser(uid.getId());
	}

	@Override
	public User createUser(final User userConfig, final String authKey) throws IOException, IllegalArgumentException {
		final String userName = userConfig.getUserName();
		if (findUserByUserName(userName) != null) {
			throw UserException.validationError().message("User '%s' already exists.", userName).build(logger);
		}
		validatePassword(authKey);
		UserConfig newUser = toUserConfig(userConfig).setUid(new UID(UUID.randomUUID().toString()))
				.setCreatedAt(System.currentTimeMillis()).setModifiedAt(userConfig.getCreatedAt()).setTag(null);

		LdapProfile profile = fromUserConfigToProfile(newUser);
		super.create(profile, authKey);
		// Return the new state
		return fromUserConfig(newUser);
	}

	// Merge existing info about user with new one, except ModifiedAt
	private void merge(UserConfig newConfig, UserConfig oldConfig) {
		newConfig.setUid(oldConfig.getUid());
		if (newConfig.getCreatedAt() == null) {
			newConfig.setCreatedAt(oldConfig.getCreatedAt());
		}
		if (newConfig.getEmail() == null) {
			newConfig.setEmail(oldConfig.getEmail());
		}
		if (newConfig.getFirstName() == null) {
			newConfig.setFirstName(oldConfig.getFirstName());
		}
		if (newConfig.getLastName() == null) {
			newConfig.setLastName(oldConfig.getLastName());
		}
		if (newConfig.getUserName() == null) {
			newConfig.setUserName(oldConfig.getUserName());
		}
		if (newConfig.getGroupMembershipsList() == null) {
			newConfig.setGroupMembershipsList(oldConfig.getGroupMembershipsList());
		}
		if (newConfig.getTag() == null) {
			newConfig.setTag(oldConfig.getTag());
		}
	}

	@Override
	public User updateUser(final User userGroup, final String authKey)
			throws IOException, IllegalArgumentException, UserNotFoundException {
		UserConfig userConfig = toUserConfig(userGroup);
		final String userName = userConfig.getUserName();
		final UserInfo oldUserInfo = findUserByUserName(userName);
		if (oldUserInfo == null) {
			throw new UserNotFoundException(userName);
		}
		merge(userConfig, oldUserInfo.getConfig());
		userConfig.setModifiedAt(System.currentTimeMillis());

		UserInfo newUserInfo = new UserInfo();
		newUserInfo.setConfig(userConfig);

		if (authKey != null) {
			validatePassword(authKey);
			newUserInfo.setAuth(buildUserAuth(oldUserInfo.getConfig().getUid(), authKey));
		} else {
			newUserInfo.setAuth(oldUserInfo.getAuth());
		}
		LdapProfile profile = fromUserConfigToProfile(userConfig);
		super.update(profile, authKey);

		// Return the new state
		return fromUserConfig(userConfig);
	}

	@Override
	public User updateUserName(final String oldUserName, final String newUserName, final User userGroup,
			final String authKey) throws IOException, IllegalArgumentException, UserNotFoundException {
		final UserInfo oldUserInfo = findUserByUserName(oldUserName);
		if (oldUserInfo == null) {
			throw new UserNotFoundException(oldUserName);
		}
		if (findUserByUserName(newUserName) != null) {
			throw UserException.validationError().message("User '%s' already exists.", newUserName).build(logger);
		}
		UserConfig userConfig = toUserConfig(userGroup);
		if (!userConfig.getUserName().equals(newUserName)) {
			throw new IllegalArgumentException(
					"Usernames do not match " + newUserName + " , " + userConfig.getUserName());
		}
		merge(userConfig, oldUserInfo.getConfig());
		userConfig.setModifiedAt(System.currentTimeMillis());

		UserInfo info = new UserInfo();
		info.setConfig(userConfig);

		if (authKey != null) {
			validatePassword(authKey);
			info.setAuth(buildUserAuth(userConfig.getUid(), authKey));
		} else {
			// use previous password
			info.setAuth(oldUserInfo.getAuth());
		}
		LdapProfile profile = fromUserConfigToProfile(userConfig);
		super.update(profile, authKey);

		super.removeById(oldUserName);

		// Return the new state
		return fromUserConfig(userConfig);
	}

	@Override
	public void authenticate(String userName, String password) throws UserLoginException {

		UserInfo userInfo = findUserByUserName(userName);
		if (userInfo == null) {
			
			throw new UserLoginException(userName, "Invalid user credentials");
		}
		try {
			UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
			super.validate(credentials, null);

		} catch (BadCredentialsException ikse) {
			throw new UserLoginException(userName, "Invalid user credentials");
		}
	}

	@Override
	public Iterable<? extends User> getAllUsers(Integer limit) throws IOException {
		SearchFilter searchFilter = new SearchFilter(LdapPofileConfig.UserFilter);
		Iterable<? extends User> configs = Iterables.transform(search(searchFilter, limit), infoConfigTransformer);
		return configs;
	}

	@Override
	protected List<Map<String, Object>> read(final List<String> names, final String key, final String value) {
		final List<Map<String, Object>> listAttributes = new ArrayList<>();
		Connection connection = null;
		try {
			connection = ldapClient.getConnectionFactory().getConnection();
			connection.open();
			final SearchOperation search = new SearchOperation(connection);
			final SearchResult result = search.execute(new SearchRequest(this.getUsersDn(), key + "=" + value)).getResult();
			for (final LdapEntry entry : result.getEntries()) {
				listAttributes.add(ldapClient.getAttributesFromEntry(entry));
			}
		} catch (final LdapException e) {
			ldapClient = null;
			throw new TechnicalException(e);
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
		return listAttributes;
	}

	protected List<Map<String, Object>> search(SearchFilter searchFitler, Integer limit) {
		final List<Map<String, Object>> listAttributes = new ArrayList<>();
		Connection connection = null;
		try {
			connection = ldapClient.getConnectionFactory().getConnection();
			connection.open();
			final SearchOperation search = new SearchOperation(connection);
			SearchRequest searchRequest = new SearchRequest();
			searchRequest.setBaseDn(this.getUsersDn());
			searchRequest.setSearchFilter(searchFitler);
			// searchRequest.setReturnAttributes(names.toArray(new String[names.size()]));
			if (limit != null && limit > 0) {
				searchRequest.setSizeLimit(limit);
			}

			final SearchResult result = search.execute(searchRequest).getResult();
			for (final LdapEntry entry : result.getEntries()) {
				listAttributes.add(ldapClient.getAttributesFromEntry(entry));
			}
		} catch (final LdapException e) {
			ldapClient = null;
			throw new TechnicalException(e);
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
		return listAttributes;
	}

	@Override
	public boolean hasAnyUser() throws IOException {
		if (anyUserFound.get()) {
			return true;
		}
		boolean userFound = getAllUsers(1).iterator().hasNext();
		anyUserFound.set(userFound);
		return userFound;
	}

	@Override
	public Iterable<? extends User> searchUsers(String searchTerm, String sortColumn, SortOrder order, Integer limit)
			throws IOException {
		limit = limit == null ? 1000 : limit;

		if (searchTerm == null || searchTerm.isEmpty()) {
			return getAllUsers(limit);
		}

		SearchFilter searchFitler = new SearchFilter();

		searchFitler.setFilter("|(cn=" + searchTerm + "*)"+"(mail=" + searchTerm + "*)"+"(givenName=" + searchTerm + "*)"+"(sn=" + searchTerm + "*)");

		Iterable<? extends User> list = Iterables.transform(search(searchFitler, limit), infoConfigTransformer);
		return list;
	}

	@Override
	public void deleteUser(final String userName, String version) throws UserNotFoundException, IOException {
		final UserInfo info = findUserByUserName(userName);
		if (info != null) {
			super.removeById(info.getConfig().getUid().getId());
			if (!getAllUsers(1).iterator().hasNext()) {
				anyUserFound.set(false);
			}
		} else {
			ldapClient = null;
			throw new UserNotFoundException(userName);
		}
	}

	private UserAuth buildUserAuth(final UID uid, final String authKey) throws IllegalArgumentException {
		final UserAuth userAuth = new UserAuth();
		final SecureRandom random = new SecureRandom();
		final byte[] prefix = new byte[16];
		// create salt
		random.nextBytes(prefix);
		userAuth.setUid(uid);
		userAuth.setPrefix(ByteString.copyFrom(prefix));
		final PBEKeySpec spec = new PBEKeySpec(authKey.toCharArray(), prefix, 65536, 128);
		try {
			userAuth.setAuthKey(ByteString.copyFrom(secretKey.generateSecret(spec).getEncoded()));
		} catch (InvalidKeySpecException ikse) {
			throw new IllegalArgumentException(ikse.toString());
		}
		return userAuth;
	}
	

	/**
	 * Used only by command line for set-password
	 *
	 * @param userName username of user whose password is being reset
	 * @param password password
	 * @throws IllegalArgumentException if user does not exist or password doesn't
	 *                                  fit minimum requirements
	 */
	public void setPassword(String userName, String password) throws IllegalArgumentException {
		validatePassword(password);

		UserInfo info = findUserByUserName(userName);
		if (info == null) {
			throw new IllegalArgumentException(format("user %s does not exist", userName));
		}

		info.setAuth(buildUserAuth(info.getConfig().getUid(), password));
		LdapProfile profile = fromUserConfigToProfile(info.getConfig());
		super.update(profile, password);
	}

	public static void validatePassword(String input) throws IllegalArgumentException {
		if (input == null || input.isEmpty() || !PASSWORD_MATCHER.matcher(input).matches()) {
			throw UserException.validationError().message(
					"Invalid password: must be at least 8 letters long, must contain at least one number and one letter")
					.build(logger);
		}
	}
	
	protected UserConfig toUserConfig(User user) {
		return new UserConfig().setUid(user.getUID()).setUserName(user.getUserName()).setFirstName(user.getFirstName())
				.setLastName(user.getLastName()).setEmail(user.getEmail()).setCreatedAt(user.getCreatedAt())
				.setModifiedAt(user.getModifiedAt()).setTag(user.getVersion());
	}

	protected User fromUserConfig(UserConfig userConfig) {
		return SimpleUser.newBuilder().setUID(userConfig.getUid()).setUserName(userConfig.getUserName())
				.setFirstName(userConfig.getFirstName()).setLastName(userConfig.getLastName())
				.setEmail(userConfig.getEmail()).setCreatedAt(userConfig.getCreatedAt())
				.setModifiedAt(userConfig.getModifiedAt()).setVersion(userConfig.getTag()).build();
	}

	protected UserConfig toUserConfig(LdapProfile userConfig) {
		return new UserConfig().setUid(new UID(userConfig.getId())).setUserName(userConfig.getId())
				.setFirstName(userConfig.getFirstName()).setLastName(userConfig.getFamilyName())
				.setEmail(userConfig.getEmail()).setCreatedAt(System.currentTimeMillis())
				.setModifiedAt(System.currentTimeMillis()).setTag((String) userConfig.getAttribute("tag"));
	}

	protected LdapProfile fromUserConfigToProfile(UserConfig userConfig) {
		LdapProfile profile = new LdapProfile();
		profile.setId(userConfig.getUid().getId());
		profile.addAttribute(LdapPofileConfig.SN, userConfig.getUserName());
		profile.addAttribute(CommonProfileDefinition.FIRST_NAME, userConfig.getFirstName());
		profile.addAttribute(CommonProfileDefinition.FAMILY_NAME, userConfig.getLastName());
		profile.addAttribute(CommonProfileDefinition.EMAIL, userConfig.getEmail());
		profile.addAttribute("created", userConfig.getCreatedAt());

		return profile;
	}

}
