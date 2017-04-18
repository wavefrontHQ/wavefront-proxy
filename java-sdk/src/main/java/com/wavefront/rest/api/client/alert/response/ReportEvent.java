package com.wavefront.rest.api.client.alert.response;

import java.util.List;
import java.util.Map;

public class ReportEvent {

	private String name;
	private Map<String, String> annotations;
	private String table;
	private Long startTime;
	private Long endTime;
	private List<String> hosts;
	private List<String> tags;
	private Long summarizedEvents;
	private Boolean isUserEvent;
	private Boolean isEphemeral;

	/**
	 * 
	 */
	public ReportEvent() {
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
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the annotations
	 */
	public Map<String, String> getAnnotations() {
		return annotations;
	}

	/**
	 * @param annotations
	 *            the annotations to set
	 */
	public void setAnnotations(Map<String, String> annotations) {
		this.annotations = annotations;
	}

	/**
	 * @return the table
	 */
	public String getTable() {
		return table;
	}

	/**
	 * @param table
	 *            the table to set
	 */
	public void setTable(String table) {
		this.table = table;
	}

	/**
	 * @return the startTime
	 */
	public Long getStartTime() {
		return startTime;
	}

	/**
	 * @param startTime
	 *            the startTime to set
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
	 * @param endTime
	 *            the endTime to set
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
	 * @param hosts
	 *            the hosts to set
	 */
	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}

	/**
	 * @return the tags
	 */
	public List<String> getTags() {
		return tags;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the summarizedEvents
	 */
	public Long getSummarizedEvents() {
		return summarizedEvents;
	}

	/**
	 * @param summarizedEvents
	 *            the summarizedEvents to set
	 */
	public void setSummarizedEvents(Long summarizedEvents) {
		this.summarizedEvents = summarizedEvents;
	}

	/**
	 * @return the isUserEvent
	 */
	public Boolean getIsUserEvent() {
		return isUserEvent;
	}

	/**
	 * @param isUserEvent
	 *            the isUserEvent to set
	 */
	public void setIsUserEvent(Boolean isUserEvent) {
		this.isUserEvent = isUserEvent;
	}

	/**
	 * @return the isEphemeral
	 */
	public Boolean getIsEphemeral() {
		return isEphemeral;
	}

	/**
	 * @param isEphemeral
	 *            the isEphemeral to set
	 */
	public void setIsEphemeral(Boolean isEphemeral) {
		this.isEphemeral = isEphemeral;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ReportEvent [name=" + name + ", annotations=" + annotations + ", table=" + table + ", startTime="
				+ startTime + ", endTime=" + endTime + ", hosts=" + hosts + ", tags=" + tags + ", summarizedEvents="
				+ summarizedEvents + ", isUserEvent=" + isUserEvent + ", isEphemeral=" + isEphemeral + "]";
	}

}
