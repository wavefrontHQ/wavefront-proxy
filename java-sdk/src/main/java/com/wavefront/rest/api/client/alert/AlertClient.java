package com.wavefront.rest.api.client.alert;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.reflect.TypeToken;
import com.wavefront.rest.api.client.BaseClient;
import com.wavefront.rest.api.client.alert.request.AlertRequest;
import com.wavefront.rest.api.client.alert.response.Alert;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.utils.HttpMethod;
import com.wavefront.rest.api.client.utils.Severity;

/*
 * Alert API Client class.
 * 
 **/

public class AlertClient extends BaseClient {

	private String serverurl = null;
	private String token;

	/*
	 * Default constructor uses resource bundle "resource.priperties" file to
	 * load the server URL and Authentication Token.
	 * 
	 */

	public AlertClient() {
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

	/**
	 * 
	 * 
	 * @param url
	 *            - String: Server Url
	 * @param xAuthToken
	 *            - String: Authentication Token
	 * 
	 */
	public AlertClient(String url, String xAuthToken) {
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

	/**
	 * Get the list of snoozed alerts.
	 * 
	 * Process GET /api/alert/snoozed
	 * 
	 * @param customerTag:
	 *            List<String> of customerTag
	 * @param userTag
	 *            : List<String> of userTag
	 * 
	 * @return - returns List of Alert, otherwise null
	 * @throws RestApiClientException
	 */

	public List<Alert> getSnoozedAlertsList(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/snoozed");
		return getAlertList(customerTag, userTag, url);
	}

	/**
	 * Get the list of snoozed alerts.
	 * 
	 * Process GET /api/alert/snoozed
	 * 
	 * @param alertRequest:
	 *            AlertRequest object
	 * 
	 * @return - returns List of Alert, otherwise null
	 * @throws RestApiClientException
	 */
	public List<Alert> getSnoozedAlertsList(AlertRequest alertRequest) throws RestApiClientException {

		if (alertRequest == null) {
			throw new IllegalArgumentException("Alert Request value cannot be null");
		}
		List<String> customerTag = alertRequest.getCustomerTag();
		List<String> userTag = alertRequest.getUserTag();
		return getSnoozedAlertsList(customerTag, userTag);
	}

	// 1) GET /api/alert/snoozed Get Snoozed Alerts

	/**
	 * Get the list of snoozed alerts.
	 * 
	 * Process GET /api/alert/snoozed
	 * 
	 * @param alertRequest:
	 *            AlertRequest object
	 * 
	 * @return - returns String: Json String for list of Alert, otherwise null
	 * @throws RestApiClientException
	 */

	public String getAllSnoozedAlertsListAsJsonString(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {
		String url = String.format("%s%s", serverurl, "/api/alert/snoozed");
		return getAlertListAsJsonString(customerTag, userTag, url);
	}

	// 2 GET /api/alert/affected_by_maintenance Get In Maintenance Alerts
	public List<Alert> getMaintenanceAlertList(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/affected_by_maintenance");
		return getAlertList(customerTag, userTag, url);

	}

	// 2 GET /api/alert/affected_by_maintenance Get In Maintenance Alerts
	public List<Alert> getMaintenanceAlertList(AlertRequest alertRequest) throws RestApiClientException {

		if (alertRequest == null) {
			throw new IllegalArgumentException("Alert Request value cannot be null");
		}
		List<String> customerTag = alertRequest.getCustomerTag();
		List<String> userTag = alertRequest.getUserTag();

		return getMaintenanceAlertList(customerTag, userTag);
	}

	// 2 GET /api/alert/affected_by_maintenance Get In Maintenance Alerts
	public String getMaintenanceAlertListAsJsonString(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/affected_by_maintenance");
		return getAlertListAsJsonString(customerTag, userTag, url);
	}

	// 3) GET /api/alert/active Get Active Alerts

	public List<Alert> getActiveAlertList(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/active");
		return getAlertList(customerTag, userTag, url);
	}

	// 3) // GET /api/alert/active Get Active Alerts

	public List<Alert> getActiveAlertList(AlertRequest alertRequest) throws RestApiClientException {

		if (alertRequest == null) {
			throw new IllegalArgumentException("Alert Request value cannot be null");
		}
		List<String> customerTag = alertRequest.getCustomerTag();
		List<String> userTag = alertRequest.getUserTag();

		return getActiveAlertList(customerTag, userTag);
	}

	// 3 // GET /api/alert/active Get Active Alerts

	public String getActiveAlertListAsJsonString(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {
		String url = String.format("%s%s", serverurl, "/api/alert/active");
		return getAlertListAsJsonString(customerTag, userTag, url);
	}

	// 4) GET /api/alert/invalid Get Invalid Alerts

	public List<Alert> getInvalidAlertList(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/invalid");
		return getAlertList(customerTag, userTag, url);
	}

	// 4) GET /api/alert/invalid Get Invalid Alerts
	public List<Alert> getInvalidAlertList(AlertRequest alertRequest) throws RestApiClientException {

		if (alertRequest == null) {
			throw new IllegalArgumentException("Alert Request value cannot be null");
		}
		List<String> customerTag = alertRequest.getCustomerTag();
		List<String> userTag = alertRequest.getUserTag();

		return getInvalidAlertList(customerTag, userTag);
	}

	// 4) GET /api/alert/invalid Get Invalid Alerts

	public String getInvalidAlertListAsJsonString(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/invalid");
		return getAlertListAsJsonString(customerTag, userTag, url);
	}

	// 5) GET /api/alert/all Get All Alerts

	public List<Alert> getAllAlertsList(List<String> customerTag, List<String> userTag) throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/all");
		return getAlertList(customerTag, userTag, url);
	}

	// 5) GET /api/alert/all Get All Alerts
	public List<Alert> getAllAlertsList(AlertRequest alertRequest) throws RestApiClientException {

		if (alertRequest == null) {
			throw new IllegalArgumentException("Alert Request value cannot be null");
		}
		List<String> customerTag = alertRequest.getCustomerTag();
		List<String> userTag = alertRequest.getUserTag();

		return getAllAlertsList(customerTag, userTag);
	}

	// 5) GET /api/alert/all Get All Alerts

	public String getAllAlertListsAsJsonString(List<String> customerTag, List<String> userTag)
			throws RestApiClientException {

		String url = String.format("%s%s", serverurl, "/api/alert/all");
		return getAlertListAsJsonString(customerTag, userTag, url);
	}

	// 6) GET /api/alert{created} Retrieve a single alert by its id (creation
	// time)

	public Alert getAlertById(Long created) throws RestApiClientException {
		if (created == null || created <= 0L) {
			throw new IllegalArgumentException("Please provide the valid value for created.");
		}
		String url = String.format("%s%s%s", serverurl, "/api/alert/", Long.toString(created).trim());

		if (execute(HttpMethod.GET, url, token)) {
			return parseAlertFromJsonString(getResponseBodyAsString());
		}

		return null;
	}

	// 6) GET /api/alert{created} Retrieve a single alert by its id (creation
	// time)

	public Alert getAlertById(AlertRequest alertRequest) throws RestApiClientException {

		if (alertRequest == null) {
			throw new IllegalArgumentException("Alert Request value cannot be null");
		}

		Long created = alertRequest.getCreated();
		return getAlertById(created);
	}

	// 6) GET /api/alert{created} Retrieve a single alert by its id (creation
	// time)

	public String getAlertByIdAsJsonString(Long created) throws RestApiClientException {
		if (created == null || created <= 0L) {
			throw new IllegalArgumentException("Please provide the valid value for created.");
		}
		String url = String.format("%s%s%s", serverurl, "/api/alert/", Long.toString(created).trim());

		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}

		return null;
	}

	// 7 ) POST /api/alert/create Create an alert

	public Alert createAlert(String name, String condition, String displayExpression, Integer minutes,
			Integer resolveAfterMinutes, List<String> notifications, Severity severity, List<String> privateTags,
			List<String> sharedTags, String additionalInformation) throws RestApiClientException {

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Alert Name cannot be empty");
		}
		if (StringUtils.isBlank(condition)) {
			throw new IllegalArgumentException("Alert Condition cannot be empty");
		}

		if (notifications == null || notifications.size() == 0) {
			throw new IllegalArgumentException("Invalid value for the notifications");
		}

		if (minutes == null || minutes.longValue() <= 0L) {
			throw new IllegalArgumentException("Invalid value for the minutes");
		}

		if (severity == null) {
			throw new IllegalArgumentException("severity cannot be null");
		}

		List<NameValuePair> formData = processFormData(name, condition, displayExpression, minutes, resolveAfterMinutes,
				notifications, severity, privateTags, sharedTags, additionalInformation);

		String url = String.format("%s%s", serverurl, "/api/alert/create");

		if (execute(HttpMethod.POST, url, token, formData)) {
			return parseAlertFromJsonString(getResponseBodyAsString());
		}

		return null;

	}

	public Alert createAlert(AlertRequest alertRequest) throws RestApiClientException {

		String name = alertRequest.getName();

		String condition = alertRequest.getCondition();
		String displayExpression = alertRequest.getDisplayExpression();

		Integer minutes = alertRequest.getMinutes();
		Integer resolveAfterMinutes = alertRequest.getResolveAfterMinutes();
		List<String> notifications = alertRequest.getNotifications();

		Severity severity = alertRequest.getSeverity();

		List<String> privateTags = alertRequest.getPrivateTags();
		List<String> sharedTags = alertRequest.getSharedTags();

		String additionalInformation = alertRequest.getAdditionalInformation();

		return createAlert(name, condition, displayExpression, minutes, resolveAfterMinutes, notifications, severity,
				privateTags, sharedTags, additionalInformation);

	}

	public String getCreateAlertAsJsonString(String name, String condition, String displayExpression, Integer minutes,
			Integer resolveAfterMinutes, List<String> notifications, Severity severity, List<String> privateTags,
			List<String> sharedTags, String additionalInformation) throws RestApiClientException {

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Alert Name cannot be empty");
		}

		if (StringUtils.isBlank(condition)) {
			throw new IllegalArgumentException("Alert Condition cannot be empty");
		}

		if (notifications == null || notifications.size() == 0) {
			throw new IllegalArgumentException("Invalid value for the notifications");
		}

		if (minutes == null || minutes.longValue() <= 0L) {
			throw new IllegalArgumentException("Invalid value for the minutes");
		}

		if (severity == null) {
			throw new IllegalArgumentException("severity cannot be null");
		}

		List<NameValuePair> formData = processFormData(name, condition, displayExpression, minutes, resolveAfterMinutes,
				notifications, severity, privateTags, sharedTags, additionalInformation);

		String url = String.format("%s%s", serverurl, "/api/alert/create");

		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}

		return null;

	}

	// 7 ) POST /api/alert/{alertId} Update an alert

	public Alert updateAlert(Long alertId, String name, String condition, String displayExpression, Integer minutes,
			Integer resolveAfterMinutes, List<String> notifications, Severity severity, List<String> privateTags,
			List<String> sharedTags, String additionalInformation) throws RestApiClientException {

		if (alertId == null || alertId.longValue() <= 0L) {
			throw new IllegalArgumentException("Invalid value for the Alert Id");
		}

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Alert Name cannot be empty");
		}

		if (StringUtils.isBlank(condition)) {
			throw new IllegalArgumentException("Alert Condition cannot be empty");
		}

		if (notifications == null || notifications.size() == 0) {
			throw new IllegalArgumentException("Invalid value for the notifications");
		}

		if (minutes == null || minutes.longValue() <= 0L) {
			throw new IllegalArgumentException("Invalid value for the minutes");
		}

		if (severity == null) {
			throw new IllegalArgumentException("severity cannot be null");
		}

		List<NameValuePair> formData = processFormData(name, condition, displayExpression, minutes, resolveAfterMinutes,
				notifications, severity, privateTags, sharedTags, additionalInformation);

		String url = String.format("%s%s%s", serverurl, "/api/alert/", Long.toString(alertId));

		if (execute(HttpMethod.POST, url, token, formData)) {
			return parseAlertFromJsonString(getResponseBodyAsString());
		}

		return null;

	}

	public Alert updateAlert(AlertRequest alertRequest) throws RestApiClientException {

		Long alertId = alertRequest.getAlertId();
		String name = alertRequest.getName();
		String condition = alertRequest.getCondition();
		String displayExpression = alertRequest.getDisplayExpression();
		Integer minutes = alertRequest.getMinutes();
		Integer resolveAfterMinutes = alertRequest.getResolveAfterMinutes();
		List<String> notifications = alertRequest.getNotifications();
		Severity severity = alertRequest.getSeverity();
		List<String> privateTags = alertRequest.getPrivateTags();
		List<String> sharedTags = alertRequest.getSharedTags();
		String additionalInformation = alertRequest.getAdditionalInformation();

		return updateAlert(alertId, name, condition, displayExpression, minutes, resolveAfterMinutes, notifications,
				severity, privateTags, sharedTags, additionalInformation);

	}

	public String getUpdateAlertAsJsonString(Long alertId, String name, String condition, String displayExpression,
			Integer minutes, Integer resolveAfterMinutes, List<String> notifications, Severity severity,
			List<String> privateTags, List<String> sharedTags, String additionalInformation)
					throws RestApiClientException {

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Alert Name cannot be empty");
		}

		if (StringUtils.isBlank(condition)) {
			throw new IllegalArgumentException("Alert Condition cannot be empty");
		}

		if (notifications == null || notifications.size() == 0) {
			throw new IllegalArgumentException("Invalid value for the notifications");
		}

		if (minutes == null || minutes.longValue() <= 0L) {
			throw new IllegalArgumentException("Invalid value for the minutes");
		}

		if (severity == null) {
			throw new IllegalArgumentException("severity cannot be null");
		}

		List<NameValuePair> formData = processFormData(name, condition, displayExpression, minutes, resolveAfterMinutes,
				notifications, severity, privateTags, sharedTags, additionalInformation);

		String url = String.format("%s%s%s", serverurl, "/api/alert/", Long.toString(alertId));

		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}

		return null;

	}

	private List<NameValuePair> processFormData(String name, String condition, String displayExpression,
			Integer minutes, Integer resolveAfterMinutes, List<String> notifications, Severity severity,
			List<String> privateTags, List<String> sharedTags, String additionalInformation) {

		List<NameValuePair> formData = new ArrayList<NameValuePair>();

		if (StringUtils.isNotBlank(name)) {
			formData.add(new BasicNameValuePair("name", name));
		}
		if (StringUtils.isNotBlank(condition)) {
			formData.add(new BasicNameValuePair("condition", condition));
		}
		if (minutes != null) {
			formData.add(new BasicNameValuePair("minutes", Integer.toString(minutes)));
		}
		if (resolveAfterMinutes != null) {
			formData.add(new BasicNameValuePair("resolveAfterMinutes", Integer.toString(resolveAfterMinutes)));
		}
		if (notifications != null && notifications.size() > 0) {
			formData.add(new BasicNameValuePair("notifications", StringUtils.join(notifications, ",")));
		}
		if (severity != null) {
			formData.add(new BasicNameValuePair("severity", severity.toString()));
		}
		if (StringUtils.isNotBlank(displayExpression)) {
			formData.add(new BasicNameValuePair("displayExpression", displayExpression));
		}
		if (privateTags != null && privateTags.size() > 0) {
			formData.add(new BasicNameValuePair("privateTags", StringUtils.join(privateTags, ",")));
		}
		if (sharedTags != null && sharedTags.size() > 0) {
			formData.add(new BasicNameValuePair("sharedTags", StringUtils.join(sharedTags, ",")));
		}
		if (StringUtils.isNotBlank(additionalInformation)) {
			formData.add(new BasicNameValuePair("additionalInformation", additionalInformation));
		}
		return formData;
	}

	private List<Alert> getAlertList(List<String> customerTag, List<String> userTag, String url)
			throws RestApiClientException {

		List<NameValuePair> formData = processFormData(customerTag, userTag);

		if (execute(HttpMethod.GET, url, token, formData)) {
			return parseAlertListFromJsonString(getResponseBodyAsString());
		}
		return null;
	}

	private String getAlertListAsJsonString(List<String> customerTag, List<String> userTag, String url)
			throws RestApiClientException {

		List<NameValuePair> formData = processFormData(customerTag, userTag);

		if (execute(HttpMethod.GET, url, token, formData)) {
			return getResponseBodyAsString();
		}

		return null;
	}

	private List<NameValuePair> processFormData(List<String> customerTagList, List<String> userTagList) {
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
		return formData;
	}

	public List<Alert> parseAlertListFromJsonString(String json) {
		List<Alert> alertList = null;
		if (StringUtils.isNotBlank(json)) {
			alertList = fromJson(json, new TypeToken<List<Alert>>() {
			}.getType());
		}
		return alertList;
	}

	public Alert parseAlertFromJsonString(String json) {
		Alert alert = null;
		if (StringUtils.isNotBlank(json)) {
			// convert response Json string to ChartingQueryResponse object
			alert = fromJson(json, Alert.class);
		}
		return alert;
	}

}
