package com.wavefront.rest.api.client.dashboard.request;

import java.util.List;

public class DashboardRequest {

	private String id;
	private String name;
	private String clonedUrl;
	private Long version;
	private Boolean rejectIfExists;
	private String body;
	private List<String> customerTag;
	private List<String> userTag;
	private Long startVersion;
	private Integer limit;

	/**
	 * 
	 */
	public DashboardRequest() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(String id) {
		this.id = id;
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
	 * @return the clonedUrl
	 */
	public String getClonedUrl() {
		return clonedUrl;
	}

	/**
	 * @param clonedUrl
	 *            the clonedUrl to set
	 */
	public void setClonedUrl(String clonedUrl) {
		this.clonedUrl = clonedUrl;
	}

	/**
	 * @return the version
	 */
	public Long getVersion() {
		return version;
	}

	/**
	 * @param version
	 *            the version to set
	 */
	public void setVersion(Long version) {
		this.version = version;
	}

	/**
	 * @return the rejectIfExists
	 */
	public Boolean getRejectIfExists() {
		return rejectIfExists;
	}

	/**
	 * @param rejectIfExists
	 *            the rejectIfExists to set
	 */
	public void setRejectIfExists(Boolean rejectIfExists) {
		this.rejectIfExists = rejectIfExists;
	}

	/**
	 * @return the body
	 */
	public String getBody() {
		return body;
	}

	/**
	 * @param body
	 *            the body to set
	 */
	public void setBody(String body) {
		this.body = body;
	}

	/**
	 * @return the customerTag
	 */
	public List<String> getCustomerTag() {
		return customerTag;
	}

	/**
	 * @param customerTag
	 *            the customerTag to set
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
	 * @param userTag
	 *            the userTag to set
	 */
	public void setUserTag(List<String> userTag) {
		this.userTag = userTag;
	}

	/**
	 * @return the startVersion
	 */
	public Long getStartVersion() {
		return startVersion;
	}

	/**
	 * @param startVersion
	 *            the startVersion to set
	 */
	public void setStartVersion(Long startVersion) {
		this.startVersion = startVersion;
	}

	/**
	 * @return the limit
	 */
	public Integer getLimit() {
		return limit;
	}

	/**
	 * @param limit
	 *            the limit to set
	 */
	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DashboardRequest [id=" + id + ", name=" + name + ", clonedUrl=" + clonedUrl + ", version=" + version
				+ ", rejectIfExists=" + rejectIfExists + ", body=" + body + ", customerTag=" + customerTag
				+ ", userTag=" + userTag + ", startVersion=" + startVersion + ", limit=" + limit + "]";
	}

}
