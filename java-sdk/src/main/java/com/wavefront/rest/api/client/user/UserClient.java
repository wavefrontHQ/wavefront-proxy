package com.wavefront.rest.api.client.user;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.reflect.TypeToken;
import com.wavefront.rest.api.client.BaseClient;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.user.request.UserRequest;
import com.wavefront.rest.api.client.user.response.PermissionGroup;
import com.wavefront.rest.api.client.user.response.UserStub;
import com.wavefront.rest.api.client.utils.HttpMethod;

public class UserClient extends BaseClient {

	private static final Logger logger = Logger.getLogger(UserClient.class.getName());
	private String serverurl = null;
	private String token;

	
	public UserClient() {
		super();

		ResourceBundle resourceBundle = ResourceBundle.getBundle("resource");

		String url = resourceBundle.getString("serverurl");
		String xAuthToken = resourceBundle.getString("token");

		if (StringUtils.isEmpty(url)) {
			throw new IllegalArgumentException("url value cannot be empty");
		}

		if (StringUtils.isEmpty(xAuthToken)) {
			throw new IllegalArgumentException("X-AUTH-TOKEN value cannot be empty");
		}

		this.serverurl = url;
		this.token = xAuthToken;
	}
	
	public UserClient(String url, String xAuthToken) {
		super();

		if (StringUtils.isEmpty(url)) {
			throw new IllegalArgumentException("url value cannot be empty");
		}

		if (StringUtils.isEmpty(xAuthToken)) {
			throw new IllegalArgumentException("X-AUTH-TOKEN value cannot be empty");
		}

		this.serverurl = url;
		this.token = xAuthToken;

	}



	public List<UserStub> getAllUsers() throws RestApiClientException {
		String url = String.format("%s%s", serverurl, "/api/user/all");

		if (execute(HttpMethod.GET, url, token)) {
			return parseUsersListFromJsonString(getResponseBodyAsString());
		}
		return null;
	}

	public String getAllUsersAsJsonString() throws RestApiClientException {
		String url = String.format("%s%s", serverurl, "/api/user/all");

		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}

		return null;
	}

	public void deleteUser(UserRequest userRequest) throws RestApiClientException {
		if (userRequest == null) {
			throw new IllegalArgumentException("User Request value cannot be null");
		}

		String id = userRequest.getId();
		deleteUser(id);
	}

	public void deleteUser(String id) throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User id cannot be empty");
		}
		id = id.trim();
		// Create url
		String url = String.format("%s%s%s", serverurl, "/api/user/", id);

		execute(HttpMethod.DELETE, url, token);

	}

	public UserStub getUserById(String id) throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User id cannot be empty");
		}
		id = id.trim();
		String url = String.format("%s%s%s", serverurl, "/api/user/", id);

		if (execute(HttpMethod.GET, url, token)) {
			return parseUserFromJsonString(getResponseBodyAsString());
		}
		return null;
	}

	public String getUserByIdAsJsonString(String id) throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User id cannot be empty");
		}
		id = id.trim();
		String url = String.format("%s%s%s", serverurl, "/api/user/", id);

		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	// POST /api/user/{id}/grant

	public UserStub grantUserPermission(String id, PermissionGroup permissionGroup) throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroup == null) {
			throw new IllegalArgumentException("Permission Group cannot be null");
		}

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		formData.add(new BasicNameValuePair("group", permissionGroup.toString()));
		id = id.trim();
		String url = String.format("%s%s%s%s", serverurl, "/api/user/", id, "/grant");
		if (execute(HttpMethod.POST, url, token, formData)) {
			return parseUserFromJsonString(getResponseBodyAsString());
		}

		return null;

	}

	public UserStub grantUserPermissions(UserRequest userRequest) throws RestApiClientException {

		String id = userRequest.getId();
		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}
		List<PermissionGroup> permissionGroups = userRequest.getGroups();
		if (permissionGroups == null || permissionGroups.size() <= 0) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		UserStub user = null;
		for (PermissionGroup permissionGroup : permissionGroups) {
			user = grantUserPermission(id, permissionGroup);
		}
		return user;
	}

	public UserStub grantUserPermissions(String id, PermissionGroup[] permissionGroups) throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroups == null) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		UserStub user = null;
		for (PermissionGroup permissionGroup : permissionGroups) {
			user = grantUserPermission(id, permissionGroup);
		}
		return user;
	}

	public String getGrantUserPermissionAsJsonString(String id, PermissionGroup permissionGroup)
			throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroup == null) {
			throw new IllegalArgumentException("Permission Group cannot be null");
		}

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		formData.add(new BasicNameValuePair("group", permissionGroup.toString()));

		id = id.trim();
		String url = String.format("%s%s%s%s", serverurl, "/api/user/", id, "/grant");

		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}

		return null;

	}

	public String getGrantUserPermissionsAsJsonString(String id, PermissionGroup[] permissionGroups)
			throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroups == null) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		String user = null;
		for (PermissionGroup permissionGroup : permissionGroups) {
			user = getGrantUserPermissionAsJsonString(id, permissionGroup);
		}
		return user;
	}

	public UserStub revokeUserPermission(String id, PermissionGroup permissionGroup) throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroup == null) {
			throw new IllegalArgumentException("Permission Group cannot be null");
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		formData.add(new BasicNameValuePair("group", permissionGroup.toString()));

		id = id.trim();
		String url = String.format("%s%s%s%s", serverurl, "/api/user/", id, "/revoke");

		if (execute(HttpMethod.POST, url, token, formData)) {
			return parseUserFromJsonString(getResponseBodyAsString());
		}

		return null;

	}

	public UserStub revokeUserPermissions(UserRequest userRequest) throws RestApiClientException {

		String id = userRequest.getId();
		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}
		List<PermissionGroup> permissionGroups = userRequest.getGroups();
		if (permissionGroups == null || permissionGroups.size() <= 0) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		UserStub user = null;
		for (PermissionGroup permissionGroup : permissionGroups) {
			user = revokeUserPermission(id, permissionGroup);
		}
		return user;
	}

	public UserStub revokeUserPermissions(String id, PermissionGroup[] permissionGroups) throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroups == null) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		UserStub user = null;
		for (PermissionGroup permissionGroup : permissionGroups) {
			user = revokeUserPermission(id, permissionGroup);
		}
		return user;
	}

	public String getRevokeUserPermissionAsJsonString(String id, PermissionGroup permissionGroup)
			throws RestApiClientException {
		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}
		if (permissionGroup == null) {
			throw new IllegalArgumentException("Permission Group cannot be null");
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		formData.add(new BasicNameValuePair("group", permissionGroup.toString()));

		id = id.trim();
		String url = String.format("%s%s%s%s", serverurl, "/api/user/", id, "/revoke");
		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	public String getRevokeUserPermissionsAsJsonString(String id, PermissionGroup[] permissionGroups)
			throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroups == null) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		String user = null;
		for (PermissionGroup permissionGroup : permissionGroups) {
			user = getRevokeUserPermissionAsJsonString(id, permissionGroup);
		}
		return user;
	}

	public UserStub createUser(String id, PermissionGroup permissionGroup, Boolean sendEmail)
			throws RestApiClientException {
		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}
		if (permissionGroup == null) {
			throw new IllegalArgumentException("Permission Group cannot be null");
		}
		if (sendEmail == null) {
			sendEmail = Boolean.FALSE;
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		formData.add(new BasicNameValuePair("groups", permissionGroup.toString()));
		formData.add(new BasicNameValuePair("id", id.trim()));
		return createUser(sendEmail, formData);
	}

	public UserStub createUser(UserRequest userRequest) throws RestApiClientException {

		String id = userRequest.getId();
		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}
		List<PermissionGroup> permissionGroups = userRequest.getGroups();

		if (permissionGroups == null || permissionGroups.size() <= 0) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}

		Boolean sendEmail = userRequest.getSendEmail();

		if (sendEmail == null) {
			sendEmail = Boolean.FALSE;
		}

		return createUser(id, permissionGroups, sendEmail);

	}

	public UserStub createUser(String id, List<PermissionGroup> permissionGroupList, Boolean sendEmail)
			throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroupList == null) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		if (sendEmail == null) {
			sendEmail = Boolean.FALSE;
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();

		if (permissionGroupList != null && permissionGroupList.size() > 0) {
			for (PermissionGroup permissionGroup : permissionGroupList) {
				formData.add(new BasicNameValuePair("groups", permissionGroup.toString()));
			}
		}

		formData.add(new BasicNameValuePair("id", id.trim()));

		return createUser(sendEmail, formData);

	}

	private UserStub createUser(Boolean sendEmail, List<NameValuePair> formData) throws RestApiClientException {

		String url = String.format("%s%s%s", serverurl, "/api/user/create?sendEmail=", sendEmail.toString());
		UserStub user = null;
		if (execute(HttpMethod.POST, url, token, formData)) {
			user = parseUserFromJsonString(getResponseBodyAsString());
		}
		return user;
	}

	public String getCreateUserAsJsonString(String id, List<PermissionGroup> permissionGroupList, Boolean sendEmail)
			throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroupList == null) {
			throw new IllegalArgumentException("Permission Groups cannot be null");
		}
		if (sendEmail == null) {
			sendEmail = Boolean.FALSE;
		}

		List<NameValuePair> formData = new ArrayList<NameValuePair>();

		if (permissionGroupList != null && permissionGroupList.size() > 0) {
			for (PermissionGroup permissionGroup : permissionGroupList) {
				formData.add(new BasicNameValuePair("groups", permissionGroup.toString()));
			}
		}

		formData.add(new BasicNameValuePair("id", id.trim()));

		String url = String.format("%s%s%s", serverurl, "/api/user/create?sendEmail=", sendEmail.toString());

		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	public String getCreateUserAsJsonString(String id, PermissionGroup permissionGroup, Boolean sendEmail)
			throws RestApiClientException {

		if (StringUtils.isBlank(id)) {
			throw new IllegalArgumentException("User Id cannot be empty");
		}

		if (permissionGroup == null) {
			throw new IllegalArgumentException("Permission Group cannot be null");
		}

		if (sendEmail == null) {
			sendEmail = Boolean.FALSE;
		}

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		formData.add(new BasicNameValuePair("groups", permissionGroup.toString()));
		formData.add(new BasicNameValuePair("id", id.trim()));

		String url = String.format("%s%s%s", serverurl, "/api/user/create?sendEmail=", sendEmail.toString());
		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;

	}

	public List<UserStub> parseUsersListFromJsonString(String jsonString) {
		List<UserStub> userStubs = null;
		if (StringUtils.isNotBlank(jsonString)) {
			userStubs = fromJson(jsonString, new TypeToken<List<UserStub>>() {
			}.getType());
		}

		return Collections.unmodifiableList(userStubs);
	}

	public UserStub parseUserFromJsonString(String json) {
		UserStub userStubs = null;
		if (StringUtils.isNotBlank(json)) {
			// convert response Json string to  UserStub object
			userStubs = fromJson(json, UserStub.class);
		}
		return userStubs;
	}

}
