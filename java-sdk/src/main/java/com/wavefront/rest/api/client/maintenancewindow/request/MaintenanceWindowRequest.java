package com.wavefront.rest.api.client.maintenancewindow.request;

import java.util.List;

public class MaintenanceWindowRequest {

	
 	private Long id;
	private Long seconds;
	private String title;	
	private Long startTimeSeconds;
	private Long endTimeSeconds;
	private String reason;
	private List<String> alertTags;
	private List<String> hostNames;
	private List<String> hostTags;
 	
	public MaintenanceWindowRequest() {
		super();
 	}
 
	public String getTitle() {
		return title;
	}


	public void setTitle(String title) {
		this.title = title;
	}


	public Long getStartTimeSeconds() {
		return startTimeSeconds;
	}


	public void setStartTimeSeconds(Long startTimeSeconds) {
		this.startTimeSeconds = startTimeSeconds;
	}


	public Long getEndTimeSeconds() {
		return endTimeSeconds;
	}


	public void setEndTimeSeconds(Long endTimeSeconds) {
		this.endTimeSeconds = endTimeSeconds;
	}


	public String getReason() {
		return reason;
	}


	public void setReason(String reason) {
		this.reason = reason;
	}


	public List<String> getAlertTags() {
		return alertTags;
	}


	public void setAlertTags(List<String> alertTags) {
		this.alertTags = alertTags;
	}


	public List<String> getHostNames() {
		return hostNames;
	}


	public void setHostNames(List<String> hostNames) {
		this.hostNames = hostNames;
	}


	 


	public List<String> getHostTags() {
		return hostTags;
	}


	public void setHostTags(List<String> hostTags) {
		this.hostTags = hostTags;
	}


	public Long getId() {
		return id;
	}


	public void setId(Long id) {
		this.id = id;
	}


	public Long getSeconds() {
		return seconds;
	}


	public void setSeconds(Long seconds) {
		this.seconds = seconds;
	}


	@Override
	public String toString() {
		return "MaintenanceWindowRequest [title=" + title + ", startTimeSeconds=" + startTimeSeconds
				+ ", endTimeSeconds=" + endTimeSeconds + ", reason=" + reason + ", alertTags=" + alertTags
				+ ", hostNames=" + hostNames + ", hostTags=" + hostTags + ", id=" + id + ", seconds=" + seconds + "]";
	}

 
}
