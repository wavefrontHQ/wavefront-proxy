package com.wavefront.rest.api.client.chart.response;

import java.util.List;
import java.util.Map;

public class QueryEvent {

	
	private String name;
	private Long start;
	private Long end;	
	private Map<String, String> tags;
	private List<String> hosts;
	private Long summarized;
	private Boolean isEphemeral;
	
	
	
	/**
	 * 
	 */
	public QueryEvent() {
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
	 * @return the start
	 */
	public Long getStart() {
		return start;
	}
	/**
	 * @param start the start to set
	 */
	public void setStart(Long start) {
		this.start = start;
	}
	/**
	 * @return the end
	 */
	public Long getEnd() {
		return end;
	}
	/**
	 * @param end the end to set
	 */
	public void setEnd(Long end) {
		this.end = end;
	}
	/**
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return tags;
	}
	/**
	 * @param tags the tags to set
	 */
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
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
	 * @return the summarized
	 */
	public Long getSummarized() {
		return summarized;
	}
	/**
	 * @param summarized the summarized to set
	 */
	public void setSummarized(Long summarized) {
		this.summarized = summarized;
	}
	/**
	 * @return the isEphemeral
	 */
	public Boolean getIsEphemeral() {
		return isEphemeral;
	}
	/**
	 * @param isEphemeral the isEphemeral to set
	 */
	public void setIsEphemeral(Boolean isEphemeral) {
		this.isEphemeral = isEphemeral;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "QueryEvent [name=" + name + ", start=" + start + ", end=" + end + ", tags=" + tags + ", hosts=" + hosts
				+ ", summarized=" + summarized + ", isEphemeral=" + isEphemeral + "]";
	}
	
 
	
	
	
}
