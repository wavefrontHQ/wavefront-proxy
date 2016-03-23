package com.wavefront.rest.api.client.chart.response;

public class TimeSeriesData {

	private Long timeStamp;
	private Double value;

	/**
	 * 
	 */
	public TimeSeriesData() {
		super();
	}

	/**
	 * @param timeStamp
	 * @param value
	 */
	public TimeSeriesData(Long timeStamp, Double value) {
		super();
		this.timeStamp = timeStamp;
		this.value = value;
	}

	/**
	 * @return the timeStamp
	 */
	public Long getTimeStamp() {
		return timeStamp;
	}

	/**
	 * @param timeStamp
	 *            the timeStamp to set
	 */
	public void setTimeStamp(Long timeStamp) {
		this.timeStamp = timeStamp;
	}

	/**
	 * @return the value
	 */
	public Double getValue() {
		return value;
	}

	/**
	 * @param value
	 *            the value to set
	 */
	public void setValue(Double value) {
		this.value = value;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TimeSeriesData [timeStamp=" + timeStamp + ", value=" + value + "]";
	}

}
