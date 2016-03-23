package com.wavefront.rest.api.client.alert.response;

import java.util.Map;
 
public class HostLabelPair {

	private String host;
	private String label;
	private Map<String, String> tags;
	private Long observed;
	private Long firing;

	public HostLabelPair() {
		super();
		// TODO Auto-generated constructor stub
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
	 * @return the label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * @param label
	 *            the label to set
	 */
	public void setLabel(String label) {
		this.label = label;
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
	 * @return the observed
	 */
	public Long getObserved() {
		return observed;
	}

	/**
	 * @param observed
	 *            the observed to set
	 */
	public void setObserved(Long observed) {
		this.observed = observed;
	}

	/**
	 * @return the firing
	 */
	public Long getFiring() {
		return firing;
	}

	/**
	 * @param firing
	 *            the firing to set
	 */
	public void setFiring(Long firing) {
		this.firing = firing;
	}

	@Override
	public String toString() {
		return "HostLabelPair [host=" + host + ", label=" + label + ", tags=" + tags + ", observed=" + observed
				+ ", firing=" + firing + "]";
	}

}
