package com.wavefront.rest.api.client.event.request;

import java.util.ArrayList;

import com.wavefront.rest.api.client.utils.Severity;

 
public class EventRequest {

	// request properties
	private String name;
	private Long startTime;
	private Long endTime;
	private Boolean instantaneousEvent;
	private String additionalDetail;
	private ArrayList<String> hosts;
	private Severity severity;
	private String eventType;

	public EventRequest() {
		super();
	}


	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getStartTime() {
		return startTime;
	}

	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}

	public Long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public Boolean getInstantaneousEvent() {
		return instantaneousEvent;
	}

	public void setInstantaneousEvent(Boolean instantaneousEvent) {
		this.instantaneousEvent = instantaneousEvent;
	}

	public String getAdditionalDetail() {
		return additionalDetail;
	}

	public void setAdditionalDetail(String additionalDetail) {
		this.additionalDetail = additionalDetail;
	}

	public ArrayList<String> getHosts() {
		return hosts;
	}

	public void setHosts(ArrayList<String> hosts) {
		this.hosts = hosts;
	}

	public Severity getSeverity() {
		return severity;
	}

	public void setSeverity(Severity severity) {
		this.severity = severity;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}



	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "EventRequest [name=" + name + ", startTime=" + startTime + ", endTime=" + endTime
				+ ", instantaneousEvent=" + instantaneousEvent + ", additionalDetail=" + additionalDetail + ", hosts="
				+ hosts + ", severity=" + severity + ", eventType=" + eventType + "]";
	}

  
}
