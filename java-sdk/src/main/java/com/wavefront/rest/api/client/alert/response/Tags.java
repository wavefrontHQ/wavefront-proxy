package com.wavefront.rest.api.client.alert.response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



public class Tags {

	private List<String> customerTags;
	private Map<String, ArrayList<String>> userTags;

	/**
	 * @return the customerTags
	 */
	public List<String> getCustomerTags() {
		return customerTags;
	}

	/**
	 * @param customerTags the customerTags to set
	 */
	public void setCustomerTags(List<String> customerTags) {
		this.customerTags = customerTags;
	}

	/**
	 * @return the userTags
	 */
	public Map<String, ArrayList<String>> getUserTags() {
		return userTags;
	}

	/**
	 * @param userTags the userTags to set
	 */
	public void setUserTags(Map<String, ArrayList<String>> userTags) {
		this.userTags = userTags;
	}

	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Tags [customerTags=" + customerTags + ", userTags=" + userTags + "]";
	}
	
}
