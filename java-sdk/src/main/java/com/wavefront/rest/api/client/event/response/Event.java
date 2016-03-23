package com.wavefront.rest.api.client.event.response;

import java.util.List;
import java.util.Map;

public class Event {

	private String name;
	private Long startTime;
	private Long endTime;
	private List<String> hosts;
	private Map<String, String> annotations;
	private Boolean isUserEvent;
	private String table;
	
	
	public Event() {
		super();

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
	 * @return the startTime
	 */
	public Long getStartTime() {
		return startTime;
	}


	/**
	 * @param startTime the startTime to set
	 */
	public void setStartTime(Long startTime) {
		this.startTime = startTime;
	}


	/**
	 * @return the endTime
	 */
	public Long getEndTime() {
		return endTime;
	}


	/**
	 * @param endTime the endTime to set
	 */
	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}


	/**
	 * @return the hosts
	 */
	public List<String> getHosts() {
		return hosts;
	}


	/**
	 * @param hosts the hosts to set
	 */
	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}


	/**
	 * @return the annotations
	 */
	public Map<String, String> getAnnotations() {
		return annotations;
	}


	/**
	 * @param annotations the annotations to set
	 */
	public void setAnnotations(Map<String, String> annotations) {
		this.annotations = annotations;
	}


	/**
	 * @return the isUserEvent
	 */
	public Boolean getIsUserEvent() {
		return isUserEvent;
	}


	/**
	 * @param isUserEvent the isUserEvent to set
	 */
	public void setIsUserEvent(Boolean isUserEvent) {
		this.isUserEvent = isUserEvent;
	}


	/**
	 * @return the table
	 */
	public String getTable() {
		return table;
	}


	/**
	 * @param table the table to set
	 */
	public void setTable(String table) {
		this.table = table;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Event [name=" + name + ", startTime=" + startTime + ", endTime=" + endTime + ", hosts=" + hosts
				+ ", annotations=" + annotations + ", isUserEvent=" + isUserEvent + ", table=" + table + "]";
	}

 
 

}
