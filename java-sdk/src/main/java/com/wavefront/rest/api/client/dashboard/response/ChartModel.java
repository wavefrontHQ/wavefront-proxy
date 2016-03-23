package com.wavefront.rest.api.client.dashboard.response;

import java.util.List;



public class ChartModel {

	private String description;
	private String units;
	private Integer base;
	private Boolean includeObsoleteMetrics;
	private List<ChartSourceModel> sources;
	private Boolean noDefaultEvents;
	private Boolean interpolatePoints;
	private ChartSettingsModel chartSettings;
	private String summarization;
	private String name;

	public ChartModel() {
		super();
		// TODO Auto-generated constructor stub
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
	 * @return the units
	 */
	public String getUnits() {
		return units;
	}

	/**
	 * @param units the units to set
	 */
	public void setUnits(String units) {
		this.units = units;
	}

	/**
	 * @return the base
	 */
	public Integer getBase() {
		return base;
	}

	/**
	 * @param base the base to set
	 */
	public void setBase(Integer base) {
		this.base = base;
	}

	/**
	 * @return the includeObsoleteMetrics
	 */
	public Boolean getIncludeObsoleteMetrics() {
		return includeObsoleteMetrics;
	}

	/**
	 * @param includeObsoleteMetrics the includeObsoleteMetrics to set
	 */
	public void setIncludeObsoleteMetrics(Boolean includeObsoleteMetrics) {
		this.includeObsoleteMetrics = includeObsoleteMetrics;
	}

	/**
	 * @return the sources
	 */
	public List<ChartSourceModel> getSources() {
		return sources;
	}

	/**
	 * @param sources the sources to set
	 */
	public void setSources(List<ChartSourceModel> sources) {
		this.sources = sources;
	}

	/**
	 * @return the noDefaultEvents
	 */
	public Boolean getNoDefaultEvents() {
		return noDefaultEvents;
	}

	/**
	 * @param noDefaultEvents the noDefaultEvents to set
	 */
	public void setNoDefaultEvents(Boolean noDefaultEvents) {
		this.noDefaultEvents = noDefaultEvents;
	}

	/**
	 * @return the interpolatePoints
	 */
	public Boolean getInterpolatePoints() {
		return interpolatePoints;
	}

	/**
	 * @param interpolatePoints the interpolatePoints to set
	 */
	public void setInterpolatePoints(Boolean interpolatePoints) {
		this.interpolatePoints = interpolatePoints;
	}

	/**
	 * @return the chartSettings
	 */
	public ChartSettingsModel getChartSettings() {
		return chartSettings;
	}

	/**
	 * @param chartSettings the chartSettings to set
	 */
	public void setChartSettings(ChartSettingsModel chartSettings) {
		this.chartSettings = chartSettings;
	}

	/**
	 * @return the summarization
	 */
	public String getSummarization() {
		return summarization;
	}

	/**
	 * @param summarization the summarization to set
	 */
	public void setSummarization(String summarization) {
		this.summarization = summarization;
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ChartModel [description=" + description + ", units=" + units + ", base=" + base
				+ ", includeObsoleteMetrics=" + includeObsoleteMetrics + ", sources=" + sources + ", noDefaultEvents="
				+ noDefaultEvents + ", interpolatePoints=" + interpolatePoints + ", chartSettings=" + chartSettings
				+ ", summarization=" + summarization + ", name=" + name + "]";
	}

 
}
