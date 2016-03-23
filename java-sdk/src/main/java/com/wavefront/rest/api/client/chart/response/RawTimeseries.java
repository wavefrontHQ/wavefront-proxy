package com.wavefront.rest.api.client.chart.response;

import java.util.List;
import java.util.Map;

public class RawTimeseries {

	private Map<String, String> tags;
	private List<RawPoint> points;

	public RawTimeseries() {
		super();
		// TODO Auto-generated constructor stub
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
	 * @return the points
	 */
	public List<RawPoint> getPoints() {
		return points;
	}

	/**
	 * @param points
	 *            the points to set
	 */
	public void setPoints(List<RawPoint> points) {
		this.points = points;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RawTimeseries [tags=" + tags + ", points=" + points + "]";
	}

}
