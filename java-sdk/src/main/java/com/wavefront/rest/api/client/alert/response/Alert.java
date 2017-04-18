package com.wavefront.rest.api.client.alert.response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.wavefront.rest.api.client.utils.Severity;

public class Alert {

	private String name;
	private String target;
	private Integer minutes;
	private ReportEvent event;
	private Long created;
	private Severity severity;

	private String condition;
	private Long snoozed;

	private List<HostLabelPair>  failingHostLabelPairs;

	private Long updated;
	private String additionalInformation;

	private Tags tags;

	private Boolean inTrash;
	private List<HostLabelPair>  prefiringHostLabelPairs;

	private List<String> notificants;

	private String displayExpression;
	private Integer resolveAfterMinutes;
	private Boolean queryFailing;
	private Long lastFailedTime;
	private String lastErrorMessage;

	private List<String> metricsUsed;
	private List<String> hostsUsed;

	private List<HostLabelPair> inMaintenanceHostLabelPairs;

	private List<String> activeMaintenanceWindows;

	private String updateUserId;
	private Long lastUpdated;

	
	private Map<String, Integer> customerTagsWithCounts;
	private Map<String, Integer> userTagsWithCounts ;

 
	
	/**
	 * 
	 */
	public Alert() {
		super();
		// TODO Auto-generated constructor stub
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
	 * @return the target
	 */
	public String getTarget() {
		return target;
	}


	/**
	 * @param target the target to set
	 */
	public void setTarget(String target) {
		this.target = target;
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
	 * @return the event
	 */
	public ReportEvent getEvent() {
		return event;
	}


	/**
	 * @param event the event to set
	 */
	public void setEvent(ReportEvent event) {
		this.event = event;
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
	 * @return the snoozed
	 */
	public Long getSnoozed() {
		return snoozed;
	}


	/**
	 * @param snoozed the snoozed to set
	 */
	public void setSnoozed(Long snoozed) {
		this.snoozed = snoozed;
	}


	/**
	 * @return the failingHostLabelPairs
	 */
	public List<HostLabelPair> getFailingHostLabelPairs() {
		return failingHostLabelPairs;
	}


	/**
	 * @param failingHostLabelPairs the failingHostLabelPairs to set
	 */
	public void setFailingHostLabelPairs(List<HostLabelPair> failingHostLabelPairs) {
		this.failingHostLabelPairs = failingHostLabelPairs;
	}


	/**
	 * @return the updated
	 */
	public Long getUpdated() {
		return updated;
	}


	/**
	 * @param updated the updated to set
	 */
	public void setUpdated(Long updated) {
		this.updated = updated;
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


	/**
	 * @return the tags
	 */
	public Tags getTags() {
		return tags;
	}


	/**
	 * @param tags the tags to set
	 */
	public void setTags(Tags tags) {
		this.tags = tags;
	}


	/**
	 * @return the inTrash
	 */
	public Boolean getInTrash() {
		return inTrash;
	}


	/**
	 * @param inTrash the inTrash to set
	 */
	public void setInTrash(Boolean inTrash) {
		this.inTrash = inTrash;
	}


	/**
	 * @return the prefiringHostLabelPairs
	 */
	public List<HostLabelPair> getPrefiringHostLabelPairs() {
		return prefiringHostLabelPairs;
	}


	/**
	 * @param prefiringHostLabelPairs the prefiringHostLabelPairs to set
	 */
	public void setPrefiringHostLabelPairs(List<HostLabelPair> prefiringHostLabelPairs) {
		this.prefiringHostLabelPairs = prefiringHostLabelPairs;
	}


	/**
	 * @return the notificants
	 */
	public List<String> getNotificants() {
		return notificants;
	}


	/**
	 * @param notificants the notificants to set
	 */
	public void setNotificants(List<String> notificants) {
		this.notificants = notificants;
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
	 * @return the queryFailing
	 */
	public Boolean getQueryFailing() {
		return queryFailing;
	}


	/**
	 * @param queryFailing the queryFailing to set
	 */
	public void setQueryFailing(Boolean queryFailing) {
		this.queryFailing = queryFailing;
	}


	/**
	 * @return the lastFailedTime
	 */
	public Long getLastFailedTime() {
		return lastFailedTime;
	}


	/**
	 * @param lastFailedTime the lastFailedTime to set
	 */
	public void setLastFailedTime(Long lastFailedTime) {
		this.lastFailedTime = lastFailedTime;
	}


	/**
	 * @return the lastErrorMessage
	 */
	public String getLastErrorMessage() {
		return lastErrorMessage;
	}


	/**
	 * @param lastErrorMessage the lastErrorMessage to set
	 */
	public void setLastErrorMessage(String lastErrorMessage) {
		this.lastErrorMessage = lastErrorMessage;
	}


	/**
	 * @return the metricsUsed
	 */
	public List<String> getMetricsUsed() {
		return metricsUsed;
	}


	/**
	 * @param metricsUsed the metricsUsed to set
	 */
	public void setMetricsUsed(List<String> metricsUsed) {
		this.metricsUsed = metricsUsed;
	}


	/**
	 * @return the hostsUsed
	 */
	public List<String> getHostsUsed() {
		return hostsUsed;
	}


	/**
	 * @param hostsUsed the hostsUsed to set
	 */
	public void setHostsUsed(List<String> hostsUsed) {
		this.hostsUsed = hostsUsed;
	}


	/**
	 * @return the inMaintenanceHostLabelPairs
	 */
	public List<HostLabelPair> getInMaintenanceHostLabelPairs() {
		return inMaintenanceHostLabelPairs;
	}


	/**
	 * @param inMaintenanceHostLabelPairs the inMaintenanceHostLabelPairs to set
	 */
	public void setInMaintenanceHostLabelPairs(List<HostLabelPair> inMaintenanceHostLabelPairs) {
		this.inMaintenanceHostLabelPairs = inMaintenanceHostLabelPairs;
	}


	/**
	 * @return the activeMaintenanceWindows
	 */
	public List<String> getActiveMaintenanceWindows() {
		return activeMaintenanceWindows;
	}


	/**
	 * @param activeMaintenanceWindows the activeMaintenanceWindows to set
	 */
	public void setActiveMaintenanceWindows(List<String> activeMaintenanceWindows) {
		this.activeMaintenanceWindows = activeMaintenanceWindows;
	}


	/**
	 * @return the updateUserId
	 */
	public String getUpdateUserId() {
		return updateUserId;
	}


	/**
	 * @param updateUserId the updateUserId to set
	 */
	public void setUpdateUserId(String updateUserId) {
		this.updateUserId = updateUserId;
	}


	/**
	 * @return the lastUpdated
	 */
	public Long getLastUpdated() {
		return lastUpdated;
	}


	/**
	 * @param lastUpdated the lastUpdated to set
	 */
	public void setLastUpdated(Long lastUpdated) {
		this.lastUpdated = lastUpdated;
	}


	/**
	 * @return the customerTagsWithCounts
	 */
	public Map<String, Integer> getCustomerTagsWithCounts() {
		return customerTagsWithCounts;
	}


	/**
	 * @param customerTagsWithCounts the customerTagsWithCounts to set
	 */
	public void setCustomerTagsWithCounts(Map<String, Integer> customerTagsWithCounts) {
		this.customerTagsWithCounts = customerTagsWithCounts;
	}


	/**
	 * @return the userTagsWithCounts
	 */
	public Map<String, Integer> getUserTagsWithCounts() {
		return userTagsWithCounts;
	}


	/**
	 * @param userTagsWithCounts the userTagsWithCounts to set
	 */
	public void setUserTagsWithCounts(Map<String, Integer> userTagsWithCounts) {
		this.userTagsWithCounts = userTagsWithCounts;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Alert [name=" + name + ", target=" + target + ", minutes=" + minutes + ", event=" + event + ", created="
				+ created + ", severity=" + severity + ", condition=" + condition + ", snoozed=" + snoozed
				+ ", failingHostLabelPairs=" + failingHostLabelPairs + ", updated=" + updated
				+ ", additionalInformation=" + additionalInformation + ", tags=" + tags + ", inTrash=" + inTrash
				+ ", prefiringHostLabelPairs=" + prefiringHostLabelPairs + ", notificants=" + notificants
				+ ", displayExpression=" + displayExpression + ", resolveAfterMinutes=" + resolveAfterMinutes
				+ ", queryFailing=" + queryFailing + ", lastFailedTime=" + lastFailedTime + ", lastErrorMessage="
				+ lastErrorMessage + ", metricsUsed=" + metricsUsed + ", hostsUsed=" + hostsUsed
				+ ", inMaintenanceHostLabelPairs=" + inMaintenanceHostLabelPairs + ", activeMaintenanceWindows="
				+ activeMaintenanceWindows + ", updateUserId=" + updateUserId + ", lastUpdated=" + lastUpdated
				+ ", customerTagsWithCounts=" + customerTagsWithCounts + ", userTagsWithCounts=" + userTagsWithCounts
				+ "]";
	}


 

 
	
	
}
