package com.wavefront.rest.api.client.event;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.wavefront.rest.api.client.BaseClient;
import com.wavefront.rest.api.client.event.request.EventRequest;
import com.wavefront.rest.api.client.event.response.Event;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.utils.HttpMethod;
import com.wavefront.rest.api.client.utils.Severity;

/*
 * Events API Client class contains all the methods that are needed to call Events API.
 * 
 * @author hchaudhry
 **/

public class EventClient extends BaseClient {

	private String serverurl = null;
	private String token;

	private static final Logger logger = Logger.getLogger(EventClient.class.getName());

	/*
	 * Default constructor. It uses resource bundle "resource.priperties" file
	 * to load the server URL and Authentication Token.
	 * 
	 */
	public EventClient() {
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
	 */
	public EventClient(String url, String xAuthToken) {
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
	 * This method will create the event based on the EventRequest object.
	 * 
	 * Process POST /api/events API call
	 * 
	 * @param eventRequest
	 *            - com.wavefront.rest.api.client.event.EventRequest object
	 * 
	 * @return - returns Event response object, otherwise null
	 * @throws RestApiClientException
	 */
	public Event createEvent(EventRequest eventRequest) throws RestApiClientException {
		if (eventRequest == null) {
			throw new IllegalArgumentException("Event Request value cannot be empty");
		}
		String name = eventRequest.getName();
		Long startTime = eventRequest.getStartTime();
		Long endTime = eventRequest.getEndTime();
		Boolean instantaneousEvent = eventRequest.getInstantaneousEvent();
		String additionalDetail = eventRequest.getAdditionalDetail();
		List<String> hosts = eventRequest.getHosts();
		Severity severity = eventRequest.getSeverity();
		String eventType = eventRequest.getEventType();
		return createEvent(name, startTime, endTime, instantaneousEvent, additionalDetail, hosts, severity, eventType);
	}

	/**
	 * 
	 * This method will create the event based on the parameters provided.
	 * 
	 * Process POST /api/events API call
	 * 
	 * @param eventName
	 *            - Name for the event - Event Name is required.
	 * @param startTime
	 *            - start time for the event
	 * @param endTime
	 *            - end time for the event
	 * @param instantaneousEvent
	 *            - boolean flag for instantaneous event
	 * @param additionalDetail
	 *            -additional details for the event
	 * @param hosts
	 *            - set of hosts affected by this event
	 * @param severity
	 *            - severity of the event
	 * @param eventType
	 *            - event type
	 * 
	 * @return - returns Event response object, otherwise null
	 * 
	 * @throws RestApiClientException
	 */
	public Event createEvent(String name, Long startTime, Long endTime, Boolean instantaneousEvent,
			String additionalDetail, List<String> hosts, Severity severity, String eventType)
					throws RestApiClientException {
		List<NameValuePair> formData = processFormData(name, startTime, endTime, instantaneousEvent, additionalDetail,
				hosts, severity, eventType);
		String url = String.format("%s%s", serverurl, "/api/events");
		if (execute(HttpMethod.POST, url, token, formData)) {
			return parseEvent(getResponseBodyAsString());
		}
		return null;
	}

	/**
	 * 
	 * This method will create the event based on the parameters provided.
	 * 
	 * Process POST /api/events API call
	 * 
	 * @param eventName
	 *            - Name for the event - Event Name is required.
	 * @param startTime
	 *            - start time for the event
	 * @param endTime
	 *            - end time for the event
	 * @param instantaneousEvent
	 *            - boolean flag for instantaneous event
	 * @param additionalDetail
	 *            -additional details for the event
	 * @param hosts
	 *            - set of hosts affected by this event
	 * @param severity
	 *            - severity of the event
	 * @param eventType
	 *            - event type
	 * 
	 * @return - returns Event response object, otherwise null
	 * 
	 * @throws RestApiClientException
	 */

	public String getCreateEventAsJsonString(String name, Long startTime, Long endTime, Boolean instantaneousEvent,
			String additionalDetail, List<String> hosts, Severity severity, String eventType)
					throws RestApiClientException {

		List<NameValuePair> formData = processFormData(name, startTime, endTime, instantaneousEvent, additionalDetail,
				hosts, severity, eventType);

		String url = String.format("%s%s", serverurl, "/api/events");
		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	/**
	 * 
	 * This method will create Instantaneous event based on the parameters
	 * provided.
	 * 
	 * Process POST /api/events API call
	 * 
	 * @param eventName
	 *            - Name for the event - Event Name is required.
	 * @param startTime
	 *            - start time for the event
	 * @param endTime
	 *            - end time for the event
	 * @param additionalDetail
	 *            -additional details for the event
	 * @param hosts
	 *            - set of hosts affected by this event
	 * @param severity
	 *            - severity of the event
	 * @param eventType
	 *            - event type
	 * 
	 * @return - returns Event response object, otherwise null
	 * 
	 * @throws RestApiClientException
	 */

	public Event createInstantaneousEvent(String eventName, Long startTime, Long endTime, String additionalDetail,
			List<String> hosts, Severity severity, String eventType) throws RestApiClientException {
		// create Instantaneous Event
		return createEvent(eventName, startTime, endTime, Boolean.TRUE, additionalDetail, hosts, severity, eventType);
	}

	/**
	 * 
	 * Close event based on the parameters provided.
	 * 
	 * Process: POST /api/events/close API call
	 * 
	 * @param eventName
	 *            - Name for the event - Event Name is required.
	 * @param startTime
	 *            - start time for the event - Start Time is Required
	 * @param endTime
	 *            - end time for the event
	 * 
	 * @return - returns Event response object, otherwise null
	 * 
	 * @throws RestApiClientExceptionv
	 */

	public Event closeEvent(String name, Long startTime, Long endTime) throws RestApiClientException {
		// form data map
		List<NameValuePair> formData = processFormData(name, startTime, endTime);
		// url for close event
		String url = String.format("%s%s", serverurl, "/api/events/close");

		if (execute(HttpMethod.POST, url, token, formData)) {
			return parseEvent(getResponseBodyAsString());
		}
		return null;
	}

	/**
	 * 
	 * Close event based on the parameters provided.
	 * 
	 * Process: POST /api/events/close API call
	 * 
	 * @param eventName
	 *            - Name for the event - Event Name is required.
	 * @param startTime
	 *            - start time for the event - Start Time is Required
	 * @param endTime
	 *            - end time for the event
	 * 
	 * @return - returns Json String response, otherwise null
	 * 
	 * @throws RestApiClientExceptionv
	 */

	public String getCloseEventAsJsonString(String name, Long startTime, Long endTime) throws RestApiClientException {

		List<NameValuePair> formData = processFormData(name, startTime, endTime);

		String url = String.format("%s%s", serverurl, "/api/events/close");
		if (execute(HttpMethod.POST, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	/**
	 * 
	 * Close event based on the Event Request object.
	 * 
	 * Process: POST /api/events/close API call
	 * 
	 * @param EventRequest
	 *            -EventRequest object - Event Name is required.
	 * 
	 * @return - returns Event response object, otherwise null
	 * 
	 * @throws RestApiClientException
	 */

	public Event closeEvent(EventRequest eventRequest) throws RestApiClientException {
		if (eventRequest == null) {
			throw new IllegalArgumentException("Event Request value cannot be empty");
		}
		Long startTime = eventRequest.getStartTime();
		String name = eventRequest.getName();
		Long endTime = eventRequest.getEndTime();
		return closeEvent(name, startTime, endTime);
	}

	/**
	 * 
	 * Delete event based on the parameters provided.
	 * 
	 * Process: DELETE /api/events/{startTime}/{name} API call
	 * 
	 * @param eventName
	 *            - Name for the event - Event Name is required.
	 * @param startTime
	 *            - start time for the event - Start Time is Required
	 * 
	 * @return - returns Event response object, otherwise null
	 * 
	 * @throws RestApiClientException
	 */

	public void deleteEvent(String name, Long startTime) throws RestApiClientException {

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Event Name cannot be empty");
		}
		if (startTime == null || startTime <= 0L) {
			throw new IllegalArgumentException("Start Time cannot be empty or less than zero");
		}
		// Create url
		String url = String.format("%s%s%s%s%s", serverurl, "/api/events/", startTime, "/", name);
		// execute the delete API call
		execute(HttpMethod.DELETE, url, token);
	}

	/**
	 * 
	 * Delete event based on EventRequest object.
	 * 
	 * Process: DELETE /api/events/{startTime}/{name} API call
	 * 
	 * @param EventRequest
	 *            - Event Name and start time is required.
	 * 
	 * @return - returns Event response object, otherwise null
	 * 
	 * @throws RestApiClientException
	 */
	public void deleteEvent(EventRequest eventRequest) throws RestApiClientException {
		if (eventRequest == null) {
			throw new IllegalArgumentException("Event Request value cannot be null");
		}
		Long startTime = eventRequest.getStartTime();
		String name = eventRequest.getName();
		deleteEvent(name, startTime);
	}

	private List<NameValuePair> processFormData(String name, Long startTime, Long endTime, Boolean instantaneousEvent,
			String additionalDetail, List<String> hosts, Severity severity, String eventType) {

		if (StringUtils.isBlank(name)) {
			throw new IllegalArgumentException("Event Name cannot be empty");
		}

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		// event name

		formData.add(new BasicNameValuePair("n", name));

		// event start time
		if (startTime != null && startTime > 0L) {
			formData.add(new BasicNameValuePair("s", Long.toString(startTime)));
		}
		if (endTime != null && endTime > 0L) {
			formData.add(new BasicNameValuePair("e", Long.toString(endTime)));
		}
		if (instantaneousEvent != null) {
			formData.add(new BasicNameValuePair("c", Boolean.toString(instantaneousEvent)));
		}
		if (StringUtils.isNotBlank(additionalDetail)) {
			formData.add(new BasicNameValuePair("d", additionalDetail));
		}
		if (hosts != null && hosts.size() > 0) {
			for (String host : hosts) {
				formData.add(new BasicNameValuePair("h", host));
			}
		}
		if (severity != null) {
			formData.add(new BasicNameValuePair("l", severity.toString()));
		}
		if (StringUtils.isNotBlank(eventType)) {
			formData.add(new BasicNameValuePair("t", eventType));
		}
		return formData;
	}

	private List<NameValuePair> processFormData(String name, Long startTime, Long endTime) {
		if (StringUtils.isEmpty(name)) {
			throw new IllegalArgumentException("Event Name cannot be empty");
		}
		if (startTime == null || startTime <= 0L) {
			throw new IllegalArgumentException("Start Time cannot be empty or less than zero");
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		// event name
		formData.add(new BasicNameValuePair("n", name));
		formData.add(new BasicNameValuePair("s", Long.toString(startTime)));
		if (endTime != null && endTime > 0L) {
			formData.add(new BasicNameValuePair("e", Long.toString(endTime)));
		}
		return formData;
	}

	private Event parseEvent(String jsonResponseString) {
		Event fromResponse = null;
		if (StringUtils.isNotBlank(jsonResponseString)) {
			fromResponse = fromJson(jsonResponseString, Event.class);
		}
		return fromResponse;
	}
}
