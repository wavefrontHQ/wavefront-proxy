package com.wavefront.rest.api.client.chart.response;

import java.util.List;
import java.util.Map;

public class QueryResult {

	private String warnings;
	private List<TimeSeries> timeseries;
	private List<QueryEvent> events;
	private Integer skip;
	private Map<String, Integer> stats;
	private String name;
	private String query;
	private List<String> hostTagsUsed;
	private Integer numCompilationTasks;
	private List<QueryKeyContainer> queryKeys;
	private List<String> metricsUsed;
	private List<String> hostsUsed;
	private Integer granularity;

	public QueryResult() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the warnings
	 */
	public String getWarnings() {
		return warnings;
	}

	/**
	 * @param warnings
	 *            the warnings to set
	 */
	public void setWarnings(String warnings) {
		this.warnings = warnings;
	}

	/**
	 * @return the timeseries
	 */
	public List<TimeSeries> getTimeseries() {
		return timeseries;
	}

	/**
	 * @param timeseries
	 *            the timeseries to set
	 */
	public void setTimeseries(List<TimeSeries> timeseries) {
		this.timeseries = timeseries;
	}

	/**
	 * @return the events
	 */
	public List<QueryEvent> getEvents() {
		return events;
	}

	/**
	 * @param events
	 *            the events to set
	 */
	public void setEvents(List<QueryEvent> events) {
		this.events = events;
	}

	/**
	 * @return the skip
	 */
	public Integer getSkip() {
		return skip;
	}

	/**
	 * @param skip
	 *            the skip to set
	 */
	public void setSkip(Integer skip) {
		this.skip = skip;
	}

	/**
	 * @return the stats
	 */
	public Map<String, Integer> getStats() {
		return stats;
	}

	/**
	 * @param stats
	 *            the stats to set
	 */
	public void setStats(Map<String, Integer> stats) {
		this.stats = stats;
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
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @param query
	 *            the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return the hostTagsUsed
	 */
	public List<String> getHostTagsUsed() {
		return hostTagsUsed;
	}

	/**
	 * @param hostTagsUsed
	 *            the hostTagsUsed to set
	 */
	public void setHostTagsUsed(List<String> hostTagsUsed) {
		this.hostTagsUsed = hostTagsUsed;
	}

	/**
	 * @return the numCompilationTasks
	 */
	public Integer getNumCompilationTasks() {
		return numCompilationTasks;
	}

	/**
	 * @param numCompilationTasks
	 *            the numCompilationTasks to set
	 */
	public void setNumCompilationTasks(Integer numCompilationTasks) {
		this.numCompilationTasks = numCompilationTasks;
	}

	/**
	 * @return the queryKeys
	 */
	public List<QueryKeyContainer> getQueryKeys() {
		return queryKeys;
	}

	/**
	 * @param queryKeys
	 *            the queryKeys to set
	 */
	public void setQueryKeys(List<QueryKeyContainer> queryKeys) {
		this.queryKeys = queryKeys;
	}

	/**
	 * @return the metricsUsed
	 */
	public List<String> getMetricsUsed() {
		return metricsUsed;
	}

	/**
	 * @param metricsUsed
	 *            the metricsUsed to set
	 */
	public void setMetricsUsed(List<String> metricsUsed) {
		this.metricsUsed = metricsUsed;
	}

	/**
	 * @return the hostsUsed
	 */
	public List<String> getHostsUsed() {
		return hostsUsed;
	}

	/**
	 * @param hostsUsed
	 *            the hostsUsed to set
	 */
	public void setHostsUsed(List<String> hostsUsed) {
		this.hostsUsed = hostsUsed;
	}

	/**
	 * @return the granularity
	 */
	public Integer getGranularity() {
		return granularity;
	}

	/**
	 * @param granularity
	 *            the granularity to set
	 */
	public void setGranularity(Integer granularity) {
		this.granularity = granularity;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "QueryResult [warnings=" + warnings + ", timeseries=" + timeseries + ", events=" + events + ", skip="
				+ skip + ", stats=" + stats + ", name=" + name + ", query=" + query + ", hostTagsUsed=" + hostTagsUsed
				+ ", numCompilationTasks=" + numCompilationTasks + ", queryKeys=" + queryKeys + ", metricsUsed="
				+ metricsUsed + ", hostsUsed=" + hostsUsed + ", granularity=" + granularity + "]";
	}

}
