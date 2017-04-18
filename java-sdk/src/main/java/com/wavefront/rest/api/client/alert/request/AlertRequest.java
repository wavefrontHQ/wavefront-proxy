package com.wavefront.rest.api.client.alert.request;

import java.util.List;

import com.wavefront.rest.api.client.utils.Severity;

public class AlertRequest {

	// Get requests
	private List<String> customerTag;
	private List<String> userTag;
	private Long created;

	// Post Requests

	private Long alertId;
	private String name;
	private String condition;
	private String displayExpression;
	private Integer minutes;
	private Integer resolveAfterMinutes;
	private List<String> notifications;
	private Severity severity;
	private List<String> privateTags;
	private List<String> sharedTags;
	private String additionalInformation;

	public AlertRequest() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the customerTag
	 */
	public List<String> getCustomerTag() {
		return customerTag;
	}

	/**
	 * @param customerTag the customerTag to set
	 */
	public void setCustomerTag(List<String> customerTag) {
		this.customerTag = customerTag;
	}

	/**
	 * @return the userTag
	 */
	public List<String> getUserTag() {
		return userTag;
	}

	/**
	 * @param userTag the userTag to set
	 */
	public void setUserTag(List<String> userTag) {
		this.userTag = userTag;
	}

	/**
	 * @return the created
	 */
	public Long getCreated() {
		return created;
	}

	/**
	 * @param created the created to set
	 */
	public void setCreated(Long created) {
		this.created = created;
	}

	/**
	 * @return the alertId
	 */
	public Long getAlertId() {
		return alertId;
	}

	/**
	 * @param alertId the alertId to set
	 */
	public void setAlertId(Long alertId) {
		this.alertId = alertId;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the condition
	 */
	public String getCondition() {
		return condition;
	}

	/**
	 * @param condition the condition to set
	 */
	public void setCondition(String condition) {
		this.condition = condition;
	}

	/**
	 * @return the displayExpression
	 */
	public String getDisplayExpression() {
		return displayExpression;
	}

	/**
	 * @param displayExpression the displayExpression to set
	 */
	public void setDisplayExpression(String displayExpression) {
		this.displayExpression = displayExpression;
	}

	/**
	 * @return the minutes
	 */
	public Integer getMinutes() {
		return minutes;
	}

	/**
	 * @param minutes the minutes to set
	 */
	public void setMinutes(Integer minutes) {
		this.minutes = minutes;
	}

	/**
	 * @return the resolveAfterMinutes
	 */
	public Integer getResolveAfterMinutes() {
		return resolveAfterMinutes;
	}

	/**
	 * @param resolveAfterMinutes the resolveAfterMinutes to set
	 */
	public void setResolveAfterMinutes(Integer resolveAfterMinutes) {
		this.resolveAfterMinutes = resolveAfterMinutes;
	}

 

	/**
	 * @return the notifications
	 */
	public List<String> getNotifications() {
		return notifications;
	}

	/**
	 * @param notifications the notifications to set
	 */
	public void setNotifications(List<String> notifications) {
		this.notifications = notifications;
	}

	/**
	 * @return the severity
	 */
	public Severity getSeverity() {
		return severity;
	}

	/**
	 * @param severity the severity to set
	 */
	public void setSeverity(Severity severity) {
		this.severity = severity;
	}

	/**
	 * @return the privateTags
	 */
	public List<String> getPrivateTags() {
		return privateTags;
	}

	/**
	 * @param privateTags the privateTags to set
	 */
	public void setPrivateTags(List<String> privateTags) {
		this.privateTags = privateTags;
	}

	/**
	 * @return the sharedTags
	 */
	public List<String> getSharedTags() {
		return sharedTags;
	}

	/**
	 * @param sharedTags the sharedTags to set
	 */
	public void setSharedTags(List<String> sharedTags) {
		this.sharedTags = sharedTags;
	}

	/**
	 * @return the additionalInformation
	 */
	public String getAdditionalInformation() {
		return additionalInformation;
	}

	/**
	 * @param additionalInformation the additionalInformation to set
	 */
	public void setAdditionalInformation(String additionalInformation) {
		this.additionalInformation = additionalInformation;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "AlertRequest [customerTag=" + customerTag + ", userTag=" + userTag + ", created=" + created
				+ ", alertId=" + alertId + ", name=" + name + ", condition=" + condition + ", displayExpression="
				+ displayExpression + ", minutes=" + minutes + ", resolveAfterMinutes=" + resolveAfterMinutes
				+ ", notifications=" + notifications + ", severity=" + severity + ", privateTags=" + privateTags
				+ ", sharedTags=" + sharedTags + ", additionalInformation=" + additionalInformation + "]";
	}
 
 
 
}
