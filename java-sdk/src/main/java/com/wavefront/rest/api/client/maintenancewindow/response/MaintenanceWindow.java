package com.wavefront.rest.api.client.maintenancewindow.response;

import java.util.Arrays;
import java.util.List;

public class MaintenanceWindow {

	private String reason;
	private String title;
	private Long createdAt;
	private String creatorUserId;

	private Long startTimeInSeconds;
	private Long endTimeInSeconds;

	private List<String> relevantCustomerTags;
	private List<String> relevantHostNames;
	private List<String> relevantHostTags;

	private String customerId;
	private String eventName;

	public MaintenanceWindow() {
		super();

	}

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public Long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Long createdAt) {
		this.createdAt = createdAt;
	}

	public String getCreatorUserId() {
		return creatorUserId;
	}

	public void setCreatorUserId(String creatorUserId) {
		this.creatorUserId = creatorUserId;
	}

	public Long getStartTimeInSeconds() {
		return startTimeInSeconds;
	}

	public void setStartTimeInSeconds(Long startTimeInSeconds) {
		this.startTimeInSeconds = startTimeInSeconds;
	}

	public Long getEndTimeInSeconds() {
		return endTimeInSeconds;
	}

	public void setEndTimeInSeconds(Long endTimeInSeconds) {
		this.endTimeInSeconds = endTimeInSeconds;
	}

	public List<String> getRelevantCustomerTags() {
		return relevantCustomerTags;
	}

	public void setRelevantCustomerTags(List<String> relevantCustomerTags) {
		this.relevantCustomerTags = relevantCustomerTags;
	}

	public List<String> getRelevantHostNames() {
		return relevantHostNames;
	}

	public void setRelevantHostNames(List<String> relevantHostNames) {
		this.relevantHostNames = relevantHostNames;
	}

	public List<String> getRelevantHostTags() {
		return relevantHostTags;
	}

	public void setRelevantHostTags(List<String> relevantHostTags) {
		this.relevantHostTags = relevantHostTags;
	}

	public String getCustomerId() {
		return customerId;
	}

	public void setCustomerId(String customerId) {
		this.customerId = customerId;
	}

	public String getEventName() {
		return eventName;
	}

	public void setEventName(String eventName) {
		this.eventName = eventName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MaintenanceWindow [reason=" + reason + ", title=" + title + ", createdAt=" + createdAt
				+ ", creatorUserId=" + creatorUserId + ", startTimeInSeconds=" + startTimeInSeconds
				+ ", endTimeInSeconds=" + endTimeInSeconds + ", relevantCustomerTags=" + relevantCustomerTags
				+ ", relevantHostNames=" + relevantHostNames + ", relevantHostTags=" + relevantHostTags
				+ ", customerId=" + customerId + ", eventName=" + eventName + "]";
	}

 

}
