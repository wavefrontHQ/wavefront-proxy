package com.wavefront.rest.api.client.user.request;

import java.util.List;

import com.wavefront.rest.api.client.user.response.PermissionGroup;

public class UserRequest {


	
	private Boolean sendEmail;
	private String id;
	private List<PermissionGroup>  groups;
	
	
	public UserRequest() {
		super();
		// TODO Auto-generated constructor stub
	}


	public Boolean getSendEmail() {
		return sendEmail;
	}


	public void setSendEmail(Boolean sendEmail) {
		this.sendEmail = sendEmail;
	}


	public String getId() {
		return id;
	}


	public void setId(String id) {
		this.id = id;
	}


	public List<PermissionGroup> getGroups() {
		return groups;
	}


	public void setGroups(List<PermissionGroup> groups) {
		this.groups = groups;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "UserRequest [sendEmail=" + sendEmail + ", id=" + id + ", groups=" + groups + "]";
	}
	
	
	
	
}
