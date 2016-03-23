package com.wavefront.rest.api.client.dashboard.response;

public class ChartSourceModel {

	private Boolean disabled;
	private PlotSource scatterPlotSource;
	private String name;
	private String query;

	public ChartSourceModel() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Boolean getDisabled() {
		return disabled;
	}

	public void setDisabled(Boolean disabled) {
		this.disabled = disabled;
	}

	public PlotSource getScatterPlotSource() {
		return scatterPlotSource;
	}

	public void setScatterPlotSource(PlotSource scatterPlotSource) {
		this.scatterPlotSource = scatterPlotSource;
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

	@Override
	public String toString() {
		return "ChartSourceModel [disabled=" + disabled + ", scatterPlotSource=" + scatterPlotSource + ", name=" + name
				+ ", query=" + query + "]";
	}

}
