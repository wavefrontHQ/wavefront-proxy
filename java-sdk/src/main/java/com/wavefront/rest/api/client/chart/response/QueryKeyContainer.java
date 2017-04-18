package com.wavefront.rest.api.client.chart.response;

import java.util.Map;



public class QueryKeyContainer {

	private String name;
	private String metric;;
	private String host;
	private Map<String, String> tags;
	private String hostTag;

	/**
	 * 
	 */
	public QueryKeyContainer() {
		super();
		// TODO Auto-generated constructor stub
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
	 * @return the metric
	 */
	public String getMetric() {
		return metric;
	}

	/**
	 * @param metric
	 *            the metric to set
	 */
	public void setMetric(String metric) {
		this.metric = metric;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @param host
	 *            the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return tags;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the hostTag
	 */
	public String getHostTag() {
		return hostTag;
	}

	/**
	 * @param hostTag
	 *            the hostTag to set
	 */
	public void setHostTag(String hostTag) {
		this.hostTag = hostTag;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "QueryKeyContainer [name=" + name + ", metric=" + metric + ", host=" + host + ", tags=" + tags
				+ ", hostTag=" + hostTag + "]";
	}

}
