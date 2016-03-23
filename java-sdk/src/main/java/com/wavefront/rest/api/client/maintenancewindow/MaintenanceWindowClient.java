package com.wavefront.rest.api.client.maintenancewindow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.Args;

import com.google.gson.reflect.TypeToken;
import com.wavefront.rest.api.client.BaseClient;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.maintenancewindow.request.MaintenanceWindowRequest;
import com.wavefront.rest.api.client.maintenancewindow.response.MaintenanceWindow;
import com.wavefront.rest.api.client.maintenancewindow.response.MaintenanceWindowSummaryStub;
import com.wavefront.rest.api.client.utils.HttpMethod;

public class MaintenanceWindowClient extends BaseClient {

	private String serverurl = null;
	private String token;

	private static final Logger logger = Logger.getLogger(MaintenanceWindowClient.class.getName());

	public MaintenanceWindowClient(String url, String xAuthToken) {
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

	public MaintenanceWindowClient() {
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

	// 1) GET /api/alert/maintenancewindow/all Get a list of every maintenance
	// window
	public List<MaintenanceWindow> getAllMaintenanceWindows() throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/maintenancewindow/all");

		if (execute(HttpMethod.GET, url, token)) {
			return getMaintenanceWindowsList(getResponseBodyAsString());
		}

		return null;
	}

	// 1) GET /api/alert/maintenancewindow/all Get a list of every maintenance
	// window
	public String getAllMaintenanceWindowsAsJsonString() throws RestApiClientException {
		String url = String.format("%s%s", serverurl, "/api/alert/maintenancewindow/all");

		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}

		return null;
	}

	// 2) POST /api/alert/maintenancewindow/create Create a new maintenance
	// window

	public MaintenanceWindow createMaintenanceWindow(String title, Long startTimeSeconds, Long endTimeSeconds,
			String reason, List<String> alertTags, List<String> hostNames, List<String> hostTags)
					throws RestApiClientException {

		// TODO : What fields are Required ?????

		List<NameValuePair> formData = processFormData(title, startTimeSeconds, endTimeSeconds, reason, alertTags,
				hostNames, hostTags);

		String url = String.format("%s%s", serverurl, "/api/alert/maintenancewindow/create");

		if (execute(HttpMethod.POST, url, token, formData)) {
			return getMaintenanceWindow(getResponseBodyAsString());
		}

		return null;

	}

	// POST 2) /api/alert/maintenancewindow/create Create a new maintenance
	// window
	public MaintenanceWindow createMaintenanceWindow(MaintenanceWindowRequest maintenanceWindowRequest)
			throws RestApiClientException {

		Args.notNull(maintenanceWindowRequest, "Maintenance Window Request");

		String title = maintenanceWindowRequest.getTitle();
		Long startTimeSeconds = maintenanceWindowRequest.getStartTimeSeconds();
		Long endTimeSeconds = maintenanceWindowRequest.getEndTimeSeconds();
		String reason = maintenanceWindowRequest.getReason();
		List<String> alertTags = maintenanceWindowRequest.getAlertTags();
		List<String> hostNames = maintenanceWindowRequest.getHostNames();
		List<String> hostTags = maintenanceWindowRequest.getHostTags();

		return createMaintenanceWindow(title, startTimeSeconds, endTimeSeconds, reason, alertTags, hostNames, hostTags);

	}

	// 2) POST /api/alert/maintenancewindow/create Create a new maintenance
	// window
	public String getCreateMaintenanceWindowAsJsonString(String title, Long startTimeSeconds, Long endTimeSeconds,
			String reason, List<String> alertTags, List<String> hostNames, List<String> hostTags)
					throws RestApiClientException {

		// TODO : What fields are Required ?????
 
		List<NameValuePair> formData = processFormData(title, startTimeSeconds, endTimeSeconds, reason, alertTags,
				hostNames, hostTags);

		String url = String.format("%s%s", serverurl, "/api/alert/maintenancewindow/create");

		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}

		return null;

	}

	// 3) POST /api/alert/maintenancewindow/{id}/extend

	public void extendMaintenanceWindow(MaintenanceWindowRequest maintenanceWindowRequest)
			throws RestApiClientException {

		if (maintenanceWindowRequest == null) {
			throw new IllegalArgumentException("Maintenance Window Request value cannot be empty");
		}

		Long id = maintenanceWindowRequest.getId();
		Long seconds = maintenanceWindowRequest.getSeconds();

		extendMaintenanceWindow(id, seconds);

	}

	// 3) POST /api/alert/maintenancewindow/{id}/extend

	public void extendMaintenanceWindow(Long id, Long seconds) throws RestApiClientException {

		// TODO : What fields are Required ?????

		if (id == null) {
			throw new IllegalArgumentException("id value cannot be empty");
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		if (seconds != null && seconds > 0) {
			formData.add(new BasicNameValuePair("seconds", Long.toString(seconds)));
		}
		String url = String.format("%s%s%s/extend", serverurl,"/api/alert/maintenancewindow/", id);
		execute(HttpMethod.POST, url, token, formData);
	}

	// 4) DELETE /api/alert/maintenancewindow/{id} Delete a specific maintenance
	// window

	public void deleteMaintenanceWindow(Long id) throws RestApiClientException {

		// TODO : What fields are Required ?????

		if (id == null) {
			throw new IllegalArgumentException("id value cannot be empty");
		}
		String url = String.format("%s/api/alert/maintenancewindow/%s", serverurl, id);
		execute(HttpMethod.DELETE, url, token);
	}

	// 4) DELETE /api/alert/maintenancewindow/{id} Delete a specific maintenance
	// window

	public void deleteMaintenanceWindow(MaintenanceWindowRequest maintenanceWindowRequest)
			throws RestApiClientException {

		if (maintenanceWindowRequest == null) {
			throw new IllegalArgumentException("Maintenance Window Request value cannot be empty");
		}
		Long id = maintenanceWindowRequest.getId();
		deleteMaintenanceWindow(id);
	}

	// 5) POST /api/alert/maintenancewindow/{id} Update a maintenance window

	public void updateMaintenanceWindow(Long id, String title, Long startTimeSeconds, Long endTimeSeconds,
			String reason, List<String> alertTags, List<String> hostNames, List<String> hostTags)
					throws RestApiClientException {

		if (id == null) {
			throw new IllegalArgumentException("id value cannot be empty");
		}
 	
		List<NameValuePair> formData = processFormData(title, startTimeSeconds, endTimeSeconds, reason, alertTags,
				hostNames, hostTags);

		String url = String.format("%s/api/alert/maintenancewindow/%s", serverurl, id);
		 
		execute(HttpMethod.POST, url, token, formData);

	}

	// 5) POST /api/alert/maintenancewindow/{id} Update a maintenance window

	public void updateMaintenanceWindow(MaintenanceWindowRequest maintenanceWindowRequest)
			throws RestApiClientException {

		if (maintenanceWindowRequest == null) {
			throw new IllegalArgumentException("Maintenance Window Request value cannot be empty");
		}

		Long id = maintenanceWindowRequest.getId();
		String title = maintenanceWindowRequest.getTitle();
		Long startTimeSeconds = maintenanceWindowRequest.getStartTimeSeconds();
		Long endTimeSeconds = maintenanceWindowRequest.getEndTimeSeconds();
		String reason = maintenanceWindowRequest.getReason();
		List<String> alertTags = maintenanceWindowRequest.getAlertTags();
		List<String> hostNames = maintenanceWindowRequest.getHostNames();
		List<String> hostTags = maintenanceWindowRequest.getHostTags();

		updateMaintenanceWindow(id, title, startTimeSeconds, endTimeSeconds, reason, alertTags, hostNames, hostTags);
	}

	// 6) GET /api/alert/maintenancewindow/summary Get a filtered list of
	// maintenance windows
	public MaintenanceWindowSummaryStub getMaintenanceWindowListSummary() throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/maintenancewindow/all");

		if (execute(HttpMethod.GET, url, token)) {
			return getMaintenanceWindowSummaryStub(getResponseBodyAsString());
		}
		return null;
	}

	// 6) GET /api/alert/maintenancewindow/summary Get a filtered list of
	// maintenance windows
	public String getMaintenanceWindowListSummaryAsJsonString() throws RestApiClientException {
		String url = String.format("%s%s", serverurl, "/api/alert/maintenancewindow/all");

		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	// POST /api/alert/maintenancewindow/{id}/close Close a currently active
	// maintenance window

	private MaintenanceWindowSummaryStub getMaintenanceWindowSummaryStub(String json) {
		MaintenanceWindowSummaryStub maintenanceWindowSummaryStub = null;
		if (StringUtils.isNotBlank(json)) {
			maintenanceWindowSummaryStub = fromJson(json, MaintenanceWindowSummaryStub.class);
		}
		return maintenanceWindowSummaryStub;
	}

	// 7) POST /api/alert/maintenancewindow/{id}/close Close a currently active
	// maintenance window

	public void closeMaintenanceWindow(Long id) throws RestApiClientException {

		if (id == null) {
			throw new IllegalArgumentException("id value cannot be empty");
		}
		String url = String.format("%s/api/alert/maintenancewindow/%s/close", serverurl, id);

		execute(HttpMethod.POST, url, token);

	}

	// 7) POST /api/alert/maintenancewindow/{id}/close Close a currently active
	// maintenance window
	public void closeMaintenanceWindow(MaintenanceWindowRequest maintenanceWindowRequest)
			throws RestApiClientException {
		if (maintenanceWindowRequest == null) {
			throw new IllegalArgumentException("Maintenance Window Request value cannot be empty");
		}
		Long id = maintenanceWindowRequest.getId();
		closeMaintenanceWindow(id);
	}

	public MaintenanceWindow getMaintenanceWindow(String jsonString) {
		MaintenanceWindow maintenanceWindow = null;

		if (StringUtils.isNotBlank(jsonString)) {
			maintenanceWindow = fromJson(jsonString, MaintenanceWindow.class);
		}
		return maintenanceWindow;
	}

	public List<MaintenanceWindow> getMaintenanceWindowsList(String jsonString) {
		List<MaintenanceWindow> maintenanceWindows = null;
		if (StringUtils.isNotBlank(jsonString)) {
			maintenanceWindows = fromJson(jsonString, new TypeToken<List<MaintenanceWindow>>() {
			}.getType());
		}
		return maintenanceWindows;
	}



	private List<NameValuePair> processFormData(String title, Long startTimeSeconds, Long endTimeSeconds, String reason,
			List<String> alertTagList, List<String> hostNameList, List<String> hostTagList) {

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		 

		if (StringUtils.isNotBlank(title)) {
			formData.add(new BasicNameValuePair("title", title));
		}

		if (startTimeSeconds != null && startTimeSeconds > 0) {
			formData.add(new BasicNameValuePair("startTimeSeconds", Long.toString(startTimeSeconds)));
		}

		if (endTimeSeconds != null && endTimeSeconds > 0) {
			formData.add(new BasicNameValuePair("endTimeSeconds", Long.toString(endTimeSeconds)));
		}

		if (StringUtils.isNotBlank(reason)) {

			formData.add(new BasicNameValuePair("reason", reason));
		}

		if (alertTagList != null && alertTagList.size() > 0) {
			for (String alertTag : alertTagList) {
				formData.add(new BasicNameValuePair("alertTags", alertTag));
			}
		}

		if (hostNameList != null && hostNameList.size() > 0) {
			for (String hostName : hostNameList) {
				formData.add(new BasicNameValuePair("hostNames", hostName));
			}
		}

		if (hostTagList != null && hostTagList.size() > 0) {
			for (String hostTag : hostTagList) {
				formData.add(new BasicNameValuePair("hostTags", hostTag));
			}
		}

		return formData;
	}

}
