package com.wavefront.rest.api.client.dashboard.response;

import java.util.List;

public class DashboardHistory {

	private Integer version;
	private String update_user;
	private Long update_time;
	private List<String> change_description;

	public DashboardHistory() {
		super();

	}

	/**
	 * @return the version
	 */
	public Integer getVersion() {
		return version;
	}

	/**
	 * @param version
	 *            the version to set
	 */
	public void setVersion(Integer version) {
		this.version = version;
	}

	/**
	 * @return the update_user
	 */
	public String getUpdate_user() {
		return update_user;
	}

	/**
	 * @param update_user
	 *            the update_user to set
	 */
	public void setUpdate_user(String update_user) {
		this.update_user = update_user;
	}

	/**
	 * @return the update_time
	 */
	public Long getUpdate_time() {
		return update_time;
	}

	/**
	 * @param update_time
	 *            the update_time to set
	 */
	public void setUpdate_time(Long update_time) {
		this.update_time = update_time;
	}

	/**
	 * @return the change_description
	 */
	public List<String> getChange_description() {
		return change_description;
	}

	/**
	 * @param change_description
	 *            the change_description to set
	 */
	public void setChange_description(List<String> change_description) {
		this.change_description = change_description;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DashboardHistory [version=" + version + ", update_user=" + update_user + ", update_time=" + update_time
				+ ", change_description=" + change_description + "]";
	}

}
