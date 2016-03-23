package com.wavefront.rest.api.client.dashboard.response;

import java.util.List;

public class SectionRowModel {

	private List<ChartModel> charts;

	public SectionRowModel() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the charts
	 */
	public List<ChartModel> getCharts() {
		return charts;
	}

	/**
	 * @param charts
	 *            the charts to set
	 */
	public void setCharts(List<ChartModel> charts) {
		this.charts = charts;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SectionRowModel [charts=" + charts + "]";
	}

}
