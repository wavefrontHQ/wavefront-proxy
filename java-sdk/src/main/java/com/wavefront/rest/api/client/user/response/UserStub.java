package com.wavefront.rest.api.client.user.response;

import java.util.List;

/*
 * Model Class for the User
 * 
 * */

public class UserStub {
 	
	private String identifier;
	private List<String> groups;
	private String customer;
	
	
	public UserStub(){
		
	}


	public String getIdentifier() {
		return identifier;
	}


	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}


	public List<String> getGroups() {
		return groups;
	}


	public void setGroups(List<String> groups) {
		this.groups = groups;
	}


	public String getCustomer() {
		return customer;
	}


	public void setCustomer(String customer) {
		this.customer = customer;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "UserStub [identifier=" + identifier + ", groups=" + groups + ", customer=" + customer + "]";
	}

 
	
	
	
}
