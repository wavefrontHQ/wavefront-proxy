package com.wavefront.rest.api.client.dashboard.response;

import java.util.Map;

public class DashboardMetadataDTO {

	private String url;
	private String name;
	private String description;
	private Boolean displayDescription;
	private Boolean displaySectionTableOfContents;
	private Boolean displayQueryParameters;
	private Integer numCharts;

	private Boolean isFavorite;

	private Integer numFavorites;
	private Integer daysInTrash;

	private Boolean trash;

	private Map<String, Integer> customerTagsWithCounts;

	private Map<String, Integer> userTagsWithCounts;

	public DashboardMetadataDTO() {
		super();
		// TODO Auto-generated constructor stub
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
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description
	 *            the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the displayDescription
	 */
	public Boolean getDisplayDescription() {
		return displayDescription;
	}

	/**
	 * @param displayDescription
	 *            the displayDescription to set
	 */
	public void setDisplayDescription(Boolean displayDescription) {
		this.displayDescription = displayDescription;
	}

	/**
	 * @return the displaySectionTableOfContents
	 */
	public Boolean getDisplaySectionTableOfContents() {
		return displaySectionTableOfContents;
	}

	/**
	 * @param displaySectionTableOfContents
	 *            the displaySectionTableOfContents to set
	 */
	public void setDisplaySectionTableOfContents(Boolean displaySectionTableOfContents) {
		this.displaySectionTableOfContents = displaySectionTableOfContents;
	}

	/**
	 * @return the displayQueryParameters
	 */
	public Boolean getDisplayQueryParameters() {
		return displayQueryParameters;
	}

	/**
	 * @param displayQueryParameters
	 *            the displayQueryParameters to set
	 */
	public void setDisplayQueryParameters(Boolean displayQueryParameters) {
		this.displayQueryParameters = displayQueryParameters;
	}

	/**
	 * @return the numCharts
	 */
	public Integer getNumCharts() {
		return numCharts;
	}

	/**
	 * @param numCharts
	 *            the numCharts to set
	 */
	public void setNumCharts(Integer numCharts) {
		this.numCharts = numCharts;
	}

	/**
	 * @return the isFavorite
	 */
	public Boolean getIsFavorite() {
		return isFavorite;
	}

	/**
	 * @param isFavorite
	 *            the isFavorite to set
	 */
	public void setIsFavorite(Boolean isFavorite) {
		this.isFavorite = isFavorite;
	}

	/**
	 * @return the numFavorites
	 */
	public Integer getNumFavorites() {
		return numFavorites;
	}

	/**
	 * @param numFavorites
	 *            the numFavorites to set
	 */
	public void setNumFavorites(Integer numFavorites) {
		this.numFavorites = numFavorites;
	}

	/**
	 * @return the daysInTrash
	 */
	public Integer getDaysInTrash() {
		return daysInTrash;
	}

	/**
	 * @param daysInTrash
	 *            the daysInTrash to set
	 */
	public void setDaysInTrash(Integer daysInTrash) {
		this.daysInTrash = daysInTrash;
	}

	/**
	 * @return the trash
	 */
	public Boolean getTrash() {
		return trash;
	}

	/**
	 * @param trash
	 *            the trash to set
	 */
	public void setTrash(Boolean trash) {
		this.trash = trash;
	}

	/**
	 * @return the customerTagsWithCounts
	 */
	public Map<String, Integer> getCustomerTagsWithCounts() {
		return customerTagsWithCounts;
	}

	/**
	 * @param customerTagsWithCounts
	 *            the customerTagsWithCounts to set
	 */
	public void setCustomerTagsWithCounts(Map<String, Integer> customerTagsWithCounts) {
		this.customerTagsWithCounts = customerTagsWithCounts;
	}

	/**
	 * @return the userTagsWithCounts
	 */
	public Map<String, Integer> getUserTagsWithCounts() {
		return userTagsWithCounts;
	}

	/**
	 * @param userTagsWithCounts
	 *            the userTagsWithCounts to set
	 */
	public void setUserTagsWithCounts(Map<String, Integer> userTagsWithCounts) {
		this.userTagsWithCounts = userTagsWithCounts;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DashboardMetadataDTO [url=" + url + ", name=" + name + ", description=" + description
				+ ", displayDescription=" + displayDescription + ", displaySectionTableOfContents="
				+ displaySectionTableOfContents + ", displayQueryParameters=" + displayQueryParameters + ", numCharts="
				+ numCharts + ", isFavorite=" + isFavorite + ", numFavorites=" + numFavorites + ", daysInTrash="
				+ daysInTrash + ", trash=" + trash + ", customerTagsWithCounts=" + customerTagsWithCounts
				+ ", userTagsWithCounts=" + userTagsWithCounts + "]";
	}

}
