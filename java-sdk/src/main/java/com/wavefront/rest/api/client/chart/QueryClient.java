package com.wavefront.rest.api.client.chart;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.wavefront.rest.api.client.BaseClient;
import com.wavefront.rest.api.client.chart.request.QueryRequest;
import com.wavefront.rest.api.client.chart.response.QueryResult;
import com.wavefront.rest.api.client.chart.response.RawTimeseries;
import com.wavefront.rest.api.client.chart.response.TimeSeriesData;
import com.wavefront.rest.api.client.chart.response.TimeSeriesDataDeserializer;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.utils.HttpMethod;
import com.wavefront.rest.api.client.utils.Summarization;

public class QueryClient extends BaseClient {

	private String serverurl = null;
	private String token;

	public QueryClient() {
		super();

		ResourceBundle resourceBundle = ResourceBundle.getBundle("resource");
		String url = resourceBundle.getString("serverurl");
		String xAuthToken = resourceBundle.getString("token");

		if (StringUtils.isEmpty(url)) {
			throw new IllegalArgumentException("url value cannot be empty");
		}

		if (StringUtils.isEmpty(xAuthToken)) {
			throw new IllegalArgumentException("X-AUTH-TOKEN value cannot be empty");
		}

		this.serverurl = url;
		this.token = xAuthToken;
	}

	public QueryClient(String url, String xAuthToken) {
		super();
		if (StringUtils.isEmpty(url)) {
			throw new IllegalArgumentException("url value cannot be empty");
		}
		if (StringUtils.isEmpty(xAuthToken)) {
			throw new IllegalArgumentException("X-AUTH-TOKEN value cannot be empty");
		}
		this.serverurl = url;
		this.token = xAuthToken;

	}

	public QueryResult performChartingQuery(String name, String query, Long startTime, Long endTime, String granularity,
			String maxNumberOfPointsReturned, Boolean pointReturnedOutSideQueryWindow, Boolean autoEvents,
			Summarization summarization, Boolean listMode, Boolean includeObsoleteMetrics, Boolean strict)
					throws RestApiClientException {

		List<NameValuePair> formData = processFormData(name, query, startTime, endTime, granularity,
				maxNumberOfPointsReturned, pointReturnedOutSideQueryWindow, autoEvents, summarization, listMode,
				includeObsoleteMetrics, strict);

		String url = String.format("%s%s", serverurl, "/chart/api");
		if (execute(HttpMethod.GET, url, token, formData)) {
			return parseQueryResultFromJsonString(getResponseBodyAsString());
		}
		return null;
	}

	private List<NameValuePair> processFormData(String name, String query, Long startTime, Long endTime,
			String granularity, String maxNumberOfPointsReturned, Boolean pointReturnedOutSideQueryWindow,
			Boolean autoEvents, Summarization summarization, Boolean listMode, Boolean includeObsoleteMetrics,
			Boolean strict) {
		if (StringUtils.isEmpty(query)) {
			throw new IllegalArgumentException("Query value cannot be empty");
		}
		if (startTime == null || startTime <= 0L) {
			throw new IllegalArgumentException("startTime value cannot be empty");
		}
		if (StringUtils.isEmpty(granularity)) {
			throw new IllegalArgumentException("Granularity value cannot be empty");
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();

		if (StringUtils.isNotEmpty(name)) {
			formData.add(new BasicNameValuePair("n", name));
		}
		formData.add(new BasicNameValuePair("q", query));
		formData.add(new BasicNameValuePair("s", Long.toString(startTime)));
		if (endTime != null && endTime > 0L) {
			formData.add(new BasicNameValuePair("e", Long.toString(endTime)));
		}

		formData.add(new BasicNameValuePair("g", granularity));

		if (StringUtils.isNotEmpty(maxNumberOfPointsReturned)) {
			formData.add(new BasicNameValuePair("p", maxNumberOfPointsReturned));
		}
		if (pointReturnedOutSideQueryWindow != null) {
			formData.add(new BasicNameValuePair("i", Boolean.toString(pointReturnedOutSideQueryWindow)));
		}
		if (autoEvents != null) {
			formData.add(new BasicNameValuePair("autoEvents", Boolean.toString(autoEvents)));
		}
		if (summarization != null) {
			formData.add(new BasicNameValuePair("summarization", summarization.toString()));
		}
		if (listMode != null) {
			formData.add(new BasicNameValuePair("listMode", Boolean.toString(listMode)));
		}
		if (includeObsoleteMetrics != null) {
			formData.add(new BasicNameValuePair("includeObsoleteMetrics", Boolean.toString(includeObsoleteMetrics)));
		}
		if (strict != null) {
			formData.add(new BasicNameValuePair("includeObsoleteMetrics", Boolean.toString(strict)));
		}
		return formData;
	}

	public String getChartingQueryResultAsJsonString(String name, String query, Long startTime, Long endTime,
			String granularity, String maxNumberOfPointsReturned, Boolean pointReturnedOutSideQueryWindow,
			Boolean autoEvents, Summarization summarization, Boolean listMode, Boolean includeObsoleteMetrics,
			Boolean strict) throws RestApiClientException {

		List<NameValuePair> formData = processFormData(name, query, startTime, endTime, granularity,
				maxNumberOfPointsReturned, pointReturnedOutSideQueryWindow, autoEvents, summarization, listMode,
				includeObsoleteMetrics, strict);

		String url = String.format("%s%s", serverurl, "/chart/api");

		if (execute(HttpMethod.GET, url, token, formData)) {
			return getResponseBodyAsString();
		}

		return null;
	}

	public QueryResult performChartingQuery(QueryRequest queryRequest) throws RestApiClientException {

		if (queryRequest == null) {
			throw new IllegalArgumentException("Query Request value cannot be empty");
		}

		String name = queryRequest.getName();
		String query = queryRequest.getQuery();
		Long startTime = queryRequest.getStartTime();
		Long endTime = queryRequest.getEndTime();
		String granularity = queryRequest.getGranularity();
		String maxNumberOfPointsReturned = queryRequest.getMaxNumberOfPointsReturned();
		Boolean pointReturnedOutSideQueryWindow = queryRequest.getPointReturnedOutSideQueryWindow();
		Boolean autoEvents = queryRequest.getAutoEvents();
		Summarization summarization = queryRequest.getSummarization();
		Boolean listMode = queryRequest.getListMode();
		Boolean includeObsoleteMetrics = queryRequest.getIncludeObsoleteMetrics();
		Boolean strict = queryRequest.getStrict();
		return performChartingQuery(name, query, startTime, endTime, granularity, maxNumberOfPointsReturned,
				pointReturnedOutSideQueryWindow, autoEvents, summarization, listMode, includeObsoleteMetrics, strict);

	}

	public RawTimeseries performRawDataPointQuery(QueryRequest queryRequest) throws RestApiClientException {

		if (queryRequest == null) {
			throw new IllegalArgumentException("Query value value cannot be empty");
		}

		String host = queryRequest.getHost();
		String source = queryRequest.getSource();
		String metric = queryRequest.getMetric();
		Long startTime = queryRequest.getStartTime();
		Long endTime = queryRequest.getEndTime();

		return performRawDataPointQuery(host, source, metric, startTime, endTime);

	}

	public String getawDataPointQueryResultAsJsonString(String host, String source, String metric, Long startTime,
			Long endTime) throws RestApiClientException {

		List<NameValuePair> formData = processFormData(host, source, metric, startTime, endTime);
		String url = String.format("%s%s", serverurl, "/chart/raw");
		if (execute(HttpMethod.GET, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	public RawTimeseries performRawDataPointQuery(String host, String source, String metric, Long startTime,
			Long endTime) throws RestApiClientException {

		List<NameValuePair> formData = processFormData(host, source, metric, startTime, endTime);
		String url = String.format("%s%s", serverurl, "/chart/raw");
		if (execute(HttpMethod.GET, url, token, formData)) {
			return parseRawTimeseriesResultFromJsonString(getResponseBodyAsString());
		}
		return null;
	}

	private List<NameValuePair> processFormData(String host, String source, String metric, Long startTime,
			Long endTime) {
		if (StringUtils.isEmpty(metric)) {
			throw new IllegalArgumentException("Metric value cannot be empty");
		}
		List<NameValuePair> formData = new ArrayList<NameValuePair>();

		formData.add(new BasicNameValuePair("metric", metric));
		if (StringUtils.isNotEmpty(host)) {
			formData.add(new BasicNameValuePair("host", host));
		}
		if (StringUtils.isNotEmpty(source)) {
			formData.add(new BasicNameValuePair("source", source));
		}
		if (startTime != null && startTime > 0L) {
			formData.add(new BasicNameValuePair("startTime", Long.toString(startTime)));
		}
		if (endTime != null && endTime > 0L) {
			formData.add(new BasicNameValuePair("endTime", Long.toString(endTime)));
		}
		return formData;
	}

	public RawTimeseries parseRawTimeseriesResultFromJsonString(String json) {
		RawTimeseries fromResponse = null;
		if (StringUtils.isNotBlank(json)) {
			// convert response Json string to RawTimeseries object
			fromResponse = fromJson(json, RawTimeseries.class);
		}
		return fromResponse;
	}

	public QueryResult parseQueryResultFromJsonString(String json) {

		QueryResult fromResponse = null;
		if (StringUtils.isNotBlank(json)) {
			GsonBuilder gsonBuilder = new GsonBuilder();
			gsonBuilder.registerTypeAdapter(TimeSeriesData.class, new TimeSeriesDataDeserializer());
			Gson gson = gsonBuilder.create();

			fromResponse = gson.fromJson(json, QueryResult.class);
		}
		return fromResponse;

	}

}
