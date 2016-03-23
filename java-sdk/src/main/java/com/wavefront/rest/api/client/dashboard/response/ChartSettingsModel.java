package com.wavefront.rest.api.client.dashboard.response;

public class ChartSettingsModel {

	private Double min;

	private Double max;

	private String lineType;
	private String stackType;

	private Long expectedDataSpacing;
	private String windowing;
	private Long windowSize;
	private Boolean showHosts;
	private Boolean showLabels;

	private String columnTags;

	private Boolean webgl;
	private Boolean autoColumnTags;

	private Double xMin;
	private Double xMax;
	private Double yMax;
	private Double yMin;
	private Boolean timeBasedColoring;
	private String type;

	public ChartSettingsModel() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Double getMin() {
		return min;
	}

	public void setMin(Double min) {
		this.min = min;
	}

	public Double getMax() {
		return max;
	}

	public void setMax(Double max) {
		this.max = max;
	}

	public String getLineType() {
		return lineType;
	}

	public void setLineType(String lineType) {
		this.lineType = lineType;
	}

	public String getStackType() {
		return stackType;
	}

	public void setStackType(String stackType) {
		this.stackType = stackType;
	}

	public Long getExpectedDataSpacing() {
		return expectedDataSpacing;
	}

	public void setExpectedDataSpacing(Long expectedDataSpacing) {
		this.expectedDataSpacing = expectedDataSpacing;
	}

	public String getWindowing() {
		return windowing;
	}

	public void setWindowing(String windowing) {
		this.windowing = windowing;
	}

	public Long getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(Long windowSize) {
		this.windowSize = windowSize;
	}

	public Boolean getShowHosts() {
		return showHosts;
	}

	public void setShowHosts(Boolean showHosts) {
		this.showHosts = showHosts;
	}

	public Boolean getShowLabels() {
		return showLabels;
	}

	public void setShowLabels(Boolean showLabels) {
		this.showLabels = showLabels;
	}

	public Double getxMax() {
		return xMax;
	}

	public void setxMax(Double xMax) {
		this.xMax = xMax;
	}

	public Double getxMin() {
		return xMin;
	}

	public void setxMin(Double xMin) {
		this.xMin = xMin;
	}

	public Double getyMax() {
		return yMax;
	}

	public void setyMax(Double yMax) {
		this.yMax = yMax;
	}

	public Double getyMin() {
		return yMin;
	}

	public void setyMin(Double yMin) {
		this.yMin = yMin;
	}

	public Boolean getTimeBasedColoring() {
		return timeBasedColoring;
	}

	public void setTimeBasedColoring(Boolean timeBasedColoring) {
		this.timeBasedColoring = timeBasedColoring;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

 
	public Boolean getWebgl() {
		return webgl;
	}

	public void setWebgl(Boolean webgl) {
		this.webgl = webgl;
	}

	public Boolean getAutoColumnTags() {
		return autoColumnTags;
	}

	public void setAutoColumnTags(Boolean autoColumnTags) {
		this.autoColumnTags = autoColumnTags;
	}

	/**
	 * @return the columnTags
	 */
	public String getColumnTags() {
		return columnTags;
	}

	/**
	 * @param columnTags the columnTags to set
	 */
	public void setColumnTags(String columnTags) {
		this.columnTags = columnTags;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "ChartSettingsModel [min=" + min + ", max=" + max + ", lineType=" + lineType + ", stackType=" + stackType
				+ ", expectedDataSpacing=" + expectedDataSpacing + ", windowing=" + windowing + ", windowSize="
				+ windowSize + ", showHosts=" + showHosts + ", showLabels=" + showLabels + ", columnTags=" + columnTags
				+ ", webgl=" + webgl + ", autoColumnTags=" + autoColumnTags + ", xMin=" + xMin + ", xMax=" + xMax
				+ ", yMax=" + yMax + ", yMin=" + yMin + ", timeBasedColoring=" + timeBasedColoring + ", type=" + type
				+ "]";
	}
 
	
	
}
