package com.wavefront.rest.api.client.chart.request;

import com.wavefront.rest.api.client.utils.Summarization;

public class QueryRequest {

	
	//request
//	GET /chart/raw
	private String host;
	private String source;
	private String metric;
 	private Long startTime;
	private Long endTime;
 	

	private String name;
	private String query;
	private String granularity;
	private String maxNumberOfPointsReturned;
	private Boolean pointReturnedOutSideQueryWindow;
	private Boolean autoEvents;
	private Summarization summarization;
	private Boolean listMode;
	private Boolean includeObsoleteMetrics;
	private Boolean strict;
		
		public QueryRequest() {
			super();
		}

	
		public String getHost() {
			return host;
		}



		public void setHost(String host) {
			this.host = host;
		}



		public String getSource() {
			return source;
		}



		public void setSource(String source) {
			this.source = source;
		}



		public String getMetric() {
			return metric;
		}



		public void setMetric(String metric) {
			this.metric = metric;
		}



		public Long getStartTime() {
			return startTime;
		}



		public void setStartTime(Long startTime) {
			this.startTime = startTime;
		}



		public Long getEndTime() {
			return endTime;
		}



		public void setEndTime(Long endTime) {
			this.endTime = endTime;
		}



		public String getName() {
			return name;
		}



		public void setName(String name) {
			this.name = name;
		}



		public String getQuery() {
			return query;
		}



		public void setQuery(String query) {
			this.query = query;
		}



		public String getGranularity() {
			return granularity;
		}



		public void setGranularity(String granularity) {
			this.granularity = granularity;
		}



		public String getMaxNumberOfPointsReturned() {
			return maxNumberOfPointsReturned;
		}



		public void setMaxNumberOfPointsReturned(String maxNumberOfPointsReturned) {
			this.maxNumberOfPointsReturned = maxNumberOfPointsReturned;
		}



		public Boolean getPointReturnedOutSideQueryWindow() {
			return pointReturnedOutSideQueryWindow;
		}



		public void setPointReturnedOutSideQueryWindow(Boolean pointReturnedOutSideQueryWindow) {
			this.pointReturnedOutSideQueryWindow = pointReturnedOutSideQueryWindow;
		}



		public Boolean getAutoEvents() {
			return autoEvents;
		}



		public void setAutoEvents(Boolean autoEvents) {
			this.autoEvents = autoEvents;
		}



		public Summarization getSummarization() {
			return summarization;
		}



		public void setSummarization(Summarization summarization) {
			this.summarization = summarization;
		}



		public Boolean getListMode() {
			return listMode;
		}



		public void setListMode(Boolean listMode) {
			this.listMode = listMode;
		}



		public Boolean getIncludeObsoleteMetrics() {
			return includeObsoleteMetrics;
		}



		public void setIncludeObsoleteMetrics(Boolean includeObsoleteMetrics) {
			this.includeObsoleteMetrics = includeObsoleteMetrics;
		}



		/**
		 * @return the strict
		 */
		public Boolean getStrict() {
			return strict;
		}



		/**
		 * @param strict the strict to set
		 */
		public void setStrict(Boolean strict) {
			this.strict = strict;
		}



		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "QueryRequest [host=" + host + ", source=" + source + ", metric=" + metric + ", startTime="
					+ startTime + ", endTime=" + endTime + ", name=" + name + ", query=" + query + ", granularity="
					+ granularity + ", maxNumberOfPointsReturned=" + maxNumberOfPointsReturned
					+ ", pointReturnedOutSideQueryWindow=" + pointReturnedOutSideQueryWindow + ", autoEvents="
					+ autoEvents + ", summarization=" + summarization + ", listMode=" + listMode
					+ ", includeObsoleteMetrics=" + includeObsoleteMetrics + ", strict=" + strict + "]";
		}

 
	
	
}
