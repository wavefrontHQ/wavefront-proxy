package com.wavefront.rest.api.client.chart.response;

public class RawPoint {

	private Double value;
	private Long timestamp;

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "Points [value=" + value + ", timestamp=" + timestamp + "]";
	}

}
