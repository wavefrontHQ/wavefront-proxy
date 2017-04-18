package com.wavefront.rest.api.client.dashboard.response;

import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class DashboardModel {

	private String customer;
	private String url;
	private String name;
	private List<SectionModel> sections;
	private EventFilterType eventFilterType;
	private Map<String, String> parameters;
	private String description;

	public DashboardModel() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the customer
	 */
	public String getCustomer() {
		return customer;
	}

	/**
	 * @param customer
	 *            the customer to set
	 */
	public void setCustomer(String customer) {
		this.customer = customer;
	}

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url
	 *            the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
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
	 * @return the sections
	 */
	public List<SectionModel> getSections() {
		return sections;
	}

	/**
	 * @param sections
	 *            the sections to set
	 */
	public void setSections(List<SectionModel> sections) {
		this.sections = sections;
	}

	/**
	 * @return the eventFilterType
	 */
	public EventFilterType getEventFilterType() {
		return eventFilterType;
	}

	/**
	 * @param eventFilterType
	 *            the eventFilterType to set
	 */
	public void setEventFilterType(EventFilterType eventFilterType) {
		this.eventFilterType = eventFilterType;
	}

	/**
	 * @return the parameters
	 */
	public Map<String, String> getParameters() {
		return parameters;
	}

	/**
	 * @param parameters
	 *            the parameters to set
	 */
	public void setParameters(Map<String, String> parameters) {
		this.parameters = parameters;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DashboardModel [customer=" + customer + "description=" + description + ", url=" + url + ", name=" + name + ", sections=" + sections
				+ ", eventFilterType=" + eventFilterType + ", parameters=" + parameters + "]";
	}
	
	public String toJson(){
		
	
		Gson gson = new Gson();

		// convert java object to JSON format,
		// and returned as JSON formatted string
		String json = gson.toJson(this);
	
		return json.toString();
		
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

}
