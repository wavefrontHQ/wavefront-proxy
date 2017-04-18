package com.wavefront.rest.api.client.manage.response;

import java.util.List;

public class TaggedSource {
	
	private String  hostname;
	private String description;
	private List<String> userTags;
	
 	/**
	 * 
	 */
	public TaggedSource() {
		super();
		// TODO Auto-generated constructor stub
	}
 
	/**
	 * @return the hostname
	 */
	public String getHostname() {
		return hostname;
	}
 
	/**
	 * @param hostname the hostname to set
	 */
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
 
	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
 

	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
 

	/**
	 * @return the userTags
	 */
	public List<String> getUserTags() {
		return userTags;
	}
 
	/**
	 * @param userTags the userTags to set
	 */
	public void setUserTags(List<String> userTags) {
		this.userTags = userTags;
	}
 
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TaggedSource [hostname=" + hostname + ", description=" + description + ", userTags=" + userTags + "]";
	}
}

