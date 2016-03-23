package com.wavefront.rest.api.client.dashboard;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.wavefront.rest.api.client.BaseClient;
import com.wavefront.rest.api.client.dashboard.request.DashboardRequest;
import com.wavefront.rest.api.client.dashboard.response.DashboardHistory;
import com.wavefront.rest.api.client.dashboard.response.DashboardMetadataDTO;
import com.wavefront.rest.api.client.dashboard.response.DashboardModel;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.user.UserClient;
import com.wavefront.rest.api.client.utils.HttpMethod;

public class DashboardClient extends BaseClient {

	private static final Logger logger = Logger.getLogger(UserClient.class.getName());
	private String serverurl = null;
	private String token;

	public DashboardClient() {
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

	public DashboardClient(String url, String xAuthToken) {
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

	// GET /api/dashboard/ Lists dashboards

	public List<DashboardMetadataDTO> getDashboardSummaryList(List<String> customerTagList,
			List<String> userTagList) throws RestApiClientException {

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		if (customerTagList != null && customerTagList.size() > 0) {
			for (String customerTag : customerTagList) {
				formData.add(new BasicNameValuePair("customerTag", customerTag));
			}
		}
		if (userTagList != null && userTagList.size() > 0) {
			for (String userTag : userTagList) {
				formData.add(new BasicNameValuePair("userTag", userTag));
			}
		}

		String url = String.format("%s%s", serverurl, "/api/dashboard");

		if (execute(HttpMethod.GET, url, token, formData)) {
			return parseDashboardListFromJson(getResponseBodyAsString());
		}
		return null;

	}

	// GET /api/dashboard/ Lists dashboards

	public List<DashboardMetadataDTO> getDashboardSummaryList(DashboardRequest dashboardRequest)
			throws RestApiClientException {
		List<String> customerTag = dashboardRequest.getCustomerTag();
		List<String> userTag = dashboardRequest.getUserTag();
		return getDashboardSummaryList(customerTag, userTag);
	}

	public String getDashboardListAsJsonString(List<String> customerTagList, List<String> userTagList)
			throws RestApiClientException {

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		if (customerTagList != null && customerTagList.size() > 0) {
			for (String customerTag : customerTagList) {
				formData.add(new BasicNameValuePair("customerTag", customerTag));
			}
		}
		if (userTagList != null && userTagList.size() > 0) {
			for (String userTag : userTagList) {
				formData.add(new BasicNameValuePair("userTag", userTag));
			}
		}
		String url = String.format("%s%s", serverurl, "/api/dashboard");
		if (execute(HttpMethod.GET, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	// POST /api/dashboard/ Saves or creates a JSON-specified dashboard.

	public void createDashboardFromJson(DashboardRequest dashboardRequest) throws RestApiClientException {

		Boolean rejectIfExists = dashboardRequest.getRejectIfExists();
		String jsonBody = dashboardRequest.getBody();
		createDashboardFromJson(rejectIfExists, jsonBody);
	}

	// POST /api/dashboard/ Saves or creates a JSON-specified dashboard.

	public void createDashboardFromJson(Boolean rejectIfExists, String jsonBody) throws RestApiClientException {
		try {
			if (rejectIfExists == null) {
				rejectIfExists = false;
			}

			if (StringUtils.isBlank(jsonBody)) {
				throw new IllegalArgumentException("Body value cannot be empty");
			}

			com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
			parser.parse(jsonBody); // throws JsonSyntaxException

			StringEntity stringEntity = new StringEntity(jsonBody);

			String postUrl = String.format("%s%s%s", serverurl, "/api/dashboard/?rejectIfExists=", rejectIfExists);

			setContentType("application/json");
			execute(HttpMethod.POST, postUrl, token, stringEntity);
		} catch (UnsupportedEncodingException e) {
			String msg = String.format(
					"UnsupportedEncodingException while creating StringEntity , UnsupportedEncodingException: %s",
					e.getMessage());
			logger.log(Level.WARNING, msg);
			throw new RestApiClientException(msg, e);
		} catch (JsonSyntaxException jse) {
			throw new RestApiClientException(String.format("Unable to parse Json String: %s", jsonBody));
		} finally {
			// HACK : set back the content type
			setContentType("application/x-www-form-urlencoded");
		}

	}

	// POST /api/dashboard/{id}/create Creates an empty dashboard
	public void createDashboard(String dashboardId, String name) throws RestApiClientException {

		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Dashboard name value cannot be empty");
		}

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		// event name
		formData.add(new BasicNameValuePair("name", name));

		String postUrl = String.format("%s%s%s%s", serverurl, "/api/dashboard/", dashboardId, "/create");

		execute(HttpMethod.POST, postUrl, token, formData);

	}

	// POST /api/dashboard/{id}/create Creates an empty dashboard
	public void createDashboard(DashboardRequest dashboardRequest) throws RestApiClientException {
		String dashboardId = dashboardRequest.getId();
		String name = dashboardRequest.getName();

		createDashboard(dashboardId, name);
	}

	// POST /api/dashboard/{id}/undelete Undeletes the dashboard at {id}

	public void unDeleteDashboard(String dashboardId) throws RestApiClientException {

		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}
		String postUrl = String.format("%s%s%s%s", serverurl, "/api/dashboard/", dashboardId, "/undelete");
		execute(HttpMethod.POST, postUrl, token);
	}

	// POST /api/dashboard/{id}/undelete Undeletes the dashboard at {id}

	public void unDeleteDashboard(DashboardRequest dashboardRequest) throws RestApiClientException {
		String dashboardId = dashboardRequest.getId();
		unDeleteDashboard(dashboardId);
	}

	// POST /api/dashboard/{id}/delete Deletes the dashboard at {id}

	public void deleteDashboard(String dashboardId) throws RestApiClientException {

		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}
		String postUrl = String.format("%s%s%s%s", serverurl, "/api/dashboard/", dashboardId, "/delete");
		execute(HttpMethod.POST, postUrl, token);
	}

	// POST /api/dashboard/{id}/delete Deletes the dashboard at {id}

	public void deleteDashboard(DashboardRequest dashboardRequest) throws RestApiClientException {
		String dashboardId = dashboardRequest.getId();
		deleteDashboard(dashboardId);
	}

	// POST/api/dashboard/{id}/clone Clones a dashboard

	public void cloneDashboard(String dashboardId, String name, String url, Long version)
			throws RestApiClientException {
		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Dashboard name value cannot be empty");
		}
		if (StringUtils.isBlank(url)) {
			throw new IllegalArgumentException("Dashboard id (URL) of cloned dashboard value cannot be empty");
		}
		if (version != null && version < 0l) {
			throw new IllegalArgumentException("Dashboard version value cannot be less than zero");
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		formData.add(new BasicNameValuePair("name", name));
		formData.add(new BasicNameValuePair("url", url));
		if (version != null) {
			formData.add(new BasicNameValuePair("v", Long.toString(version)));
		}

		String postUrl = String.format("%s%s%s%s", serverurl, "/api/dashboard/", dashboardId, "/clone");
		execute(HttpMethod.POST, postUrl, token, formData);

	}

	public void cloneDashboard(DashboardRequest dashboardRequest) throws RestApiClientException {

		String dashboardId = dashboardRequest.getId();
		String name = dashboardRequest.getName();
		String url = dashboardRequest.getClonedUrl();
		Long version = dashboardRequest.getVersion();

		cloneDashboard(dashboardId, name, url, version);

	}

	// GET /api/dashboard/{id}/history

	public List<DashboardHistory> getDashboardHistory(String dashboardId) throws RestApiClientException {

		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}

		dashboardId = dashboardId.trim();
		String url = String.format("%s%s%s%s", serverurl, "/api/dashboard/", dashboardId, "/hostory");

		if (execute(HttpMethod.GET, url, token)) {
			return parseDashboardHistoryListFromJson(getResponseBodyAsString());
		}
		return null;
	}

	public List<DashboardHistory> getDashboardHistory(DashboardRequest dashboardRequest) throws RestApiClientException {

		if (dashboardRequest == null) {
			throw new IllegalArgumentException("Dashboard Request value cannot be empty");
		}
		String dashboardId = dashboardRequest.getId();
		return getDashboardHistory(dashboardId);
	}

	public String getDashboardHistoryAsJsonString(String dashboardId) throws RestApiClientException {

		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}

		dashboardId = dashboardId.trim();
		String url = String.format("%s%s%s%s", serverurl, "/api/dashboard/", dashboardId, "/hostory");

		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	// GET /api/dashboard/{id}/history

	public DashboardModel getDashboardById(String dashboardId) throws RestApiClientException {

		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}

		dashboardId = dashboardId.trim();
		String url = String.format("%s%s%s", serverurl, "/api/dashboard/", dashboardId);

		if (execute(HttpMethod.GET, url, token)) {
			return parseDashboardModelFromJson(getResponseBodyAsString());
		}
		return null;
	}

	public DashboardModel getDashboardById(DashboardRequest dashboardRequest) throws RestApiClientException {

		if (dashboardRequest == null) {
			throw new IllegalArgumentException("Dashboard Request value cannot be empty");
		}
		String dashboardId = dashboardRequest.getId();
		return getDashboardById(dashboardId);
	}

	public String getDashboardByIdAsJsonString(String dashboardId) throws RestApiClientException {

		if (StringUtils.isBlank(dashboardId)) {
			throw new IllegalArgumentException("Dashboard Id value cannot be empty");
		}

		dashboardId = dashboardId.trim();
		dashboardId = dashboardId.trim();
		String url = String.format("%s%s%s", serverurl, "/api/dashboard/", dashboardId);

		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	public List<DashboardHistory> parseDashboardHistoryListFromJson(String jsonString) {
		List<DashboardHistory> dashboardHistoryList = null;
		if (StringUtils.isNotBlank(jsonString)) {
			dashboardHistoryList = fromJson(jsonString, new TypeToken<List<DashboardHistory>>() {
			}.getType());
		}
		return dashboardHistoryList;
	}

	public DashboardModel parseDashboardModelFromJson(String jsonString) {
		DashboardModel dashboardModel = null;
		if (StringUtils.isNotBlank(jsonString)) {
			dashboardModel = fromJson(jsonString, DashboardModel.class);
		}
		return dashboardModel;

	}

	public List<DashboardMetadataDTO> parseDashboardListFromJson(String jsonString) {
		List<DashboardMetadataDTO> dashboardList = null;
		if (StringUtils.isNotBlank(jsonString)) {
			dashboardList = fromJson(jsonString, new TypeToken<List<DashboardMetadataDTO>>() {
			}.getType());
		}
		return dashboardList;

	}

}
