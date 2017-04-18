package com.wavefront.rest.api.client.manage;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.wavefront.rest.api.client.BaseClient;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.manage.request.ManageRequest;
import com.wavefront.rest.api.client.manage.response.TaggedSource;
import com.wavefront.rest.api.client.manage.response.TaggedSourceBundle;
import com.wavefront.rest.api.client.manage.response.TaggedSourceDataDeserializer;
import com.wavefront.rest.api.client.utils.HttpMethod;

public class ManageClient extends BaseClient {

	private String serverurl = null;
	private String token;

	private static final Logger logger = Logger.getLogger(ManageClient.class.getName());

	public ManageClient(String url, String xAuthToken) {
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

	public ManageClient() {
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

	// GET /api/manage/source/ Request a subset of sources

	public TaggedSourceBundle getSources(String lastEntityId, Boolean descending, Integer limit)
			throws RestApiClientException {

		List<NameValuePair> formData = processFormData(lastEntityId, descending, limit);

		String url = String.format("%s%s", serverurl, "/api/manage/source");

		if (execute(HttpMethod.GET, url, token, formData)) {
			return parseTaggedSourceBundleFromJsonString(getResponseBodyAsString());
		}
		return null;
	}

	// GET /api/manage/source/ Request a subset of sources
	public String getSourcesResultAsString(String lastEntityId, Boolean descending, Integer limit)
			throws RestApiClientException {

		List<NameValuePair> formData = processFormData(lastEntityId, descending, limit);

		String url = String.format("%s%s", serverurl, "/api/manage/source");

		if (execute(HttpMethod.GET, url, token, formData)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	// GET /api/manage/source/ Request a subset of sources
	public TaggedSourceBundle getSourcesResultAsJsonString(ManageRequest manageRequest) throws RestApiClientException {
		String lastEntityId = manageRequest.getLastEntityId();
		Boolean descending = manageRequest.getDesc();
		Integer limit = manageRequest.getLimit();
		return getSources(lastEntityId, descending, limit);
	}

	private List<NameValuePair> processFormData(String lastEntityId, Boolean descending, Integer limit) {

		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		if (StringUtils.isNotEmpty(lastEntityId)) {
			formData.add(new BasicNameValuePair("lastEntityId", lastEntityId));
		}
		// true is default
		if (descending != null) {
			formData.add(new BasicNameValuePair("desc", descending.toString()));
		} else {
			formData.add(new BasicNameValuePair("desc", Boolean.TRUE.toString()));
		}
		// limit default is 100
		if (limit == null) {
			formData.add(new BasicNameValuePair("limit", Integer.toString(100)));
		} else if (limit > 0 && limit <= 10000) {
			formData.add(new BasicNameValuePair("limit", Integer.toString(limit)));
		} else if (limit > 10000) {
			throw new IllegalArgumentException("limit value must be <= 10000).");
		}
		return formData;
	}

	// GET /api/manage/source/{source} Return applied tags and description for a
	// source/host

	public TaggedSource getSource(ManageRequest manageRequest) throws RestApiClientException {

		if (manageRequest == null) {
			throw new IllegalArgumentException("Manage Request value cannot be null");
		}

		String source = manageRequest.getSource();
		return getSource(source);

	}

	public TaggedSource getSource(String source) throws RestApiClientException {
		if (StringUtils.isEmpty(source)) {
			throw new IllegalArgumentException("Source value cannot be empty");
		}
		String url = String.format("%s/api/manage/source/%s", serverurl, source);
		if (execute(HttpMethod.GET, url, token)) {
			return parseTaggedSourceFromJsonString(getResponseBodyAsString());
		}
		return null;
	}

	public String getSourceResultAsJsonString(String source) throws RestApiClientException {
		if (StringUtils.isEmpty(source)) {
			throw new IllegalArgumentException("Source value cannot be empty");
		}
		String url = String.format("%s/api/manage/source/%s", serverurl, source);
		if (execute(HttpMethod.GET, url, token)) {
			return getResponseBodyAsString();
		}
		return null;
	}

	// DELETE /api/manage/source/{source}/tags/{tag} Remove tag from a
	// source/host

	public void removeTagFromSource(String source, String tag) throws RestApiClientException {
		if (StringUtils.isEmpty(source)) {
			throw new IllegalArgumentException("Source value cannot be empty");
		}
		if (StringUtils.isEmpty(tag)) {
			throw new IllegalArgumentException("Tag value cannot be empty");
		}
		String url = String.format("%s/api/manage/source/%s/tags/%s", serverurl, source, tag);
		execute(HttpMethod.DELETE, url, token);
	}

	// DELETE /api/manage/source/{source}/tags/{tag} Remove tag from a
	// source/host

	public void removeTagFromSource(ManageRequest manageRequest) throws RestApiClientException {

		if (manageRequest == null) {
			throw new IllegalArgumentException("Manage Request value cannot be null");
		}
		String source = manageRequest.getSource();
		String tag = manageRequest.getTag();
		removeTagFromSource(source, tag);
	}

	// POST /api/manage/source/{source}/tags/{tag} Tag a source/host

	public void addTagToSource(String source, String tag) throws RestApiClientException {

		if (StringUtils.isEmpty(source)) {
			throw new IllegalArgumentException("Source value cannot be empty");
		}
		if (StringUtils.isEmpty(tag)) {
			throw new IllegalArgumentException("Tag value cannot be empty");
		}

		String url = String.format("%s/api/manage/source/%s/tags/%s", serverurl, source, tag);

		execute(HttpMethod.POST, url, token);
	}

	// POST /api/manage/source/{source}/tags/{tag} Tag a source/host

	public void addTagToSource(ManageRequest manageRequest) throws RestApiClientException {

		if (manageRequest == null) {
			throw new IllegalArgumentException("Manage Request value cannot be null");
		}

		String source = manageRequest.getSource();
		String tag = manageRequest.getTag();

		addTagToSource(source, tag);
	}

	// POST /api/manage/source/{source}/description Set description for a
	// source/host

	public void addDescriptionToSource(String source, String description) throws RestApiClientException {

		if (StringUtils.isEmpty(source)) {
			throw new IllegalArgumentException("Source value cannot be empty");
		}
		try {
			source = source.trim();
			List<NameValuePair> formData = new ArrayList<NameValuePair>();
			if (StringUtils.isNotEmpty(description)) {
				formData.add(new BasicNameValuePair("body", description));
			}
			String url = String.format("%s/api/manage/source/%s/description", serverurl, source);

			setContentType("text/plain");
			execute(HttpMethod.POST, url, token, formData);
		} finally {
			// HACK : set back the content type
			setContentType("application/x-www-form-urlencoded");
		}
	}

	// POST /api/manage/source/{source}/tags/{tag} Tag a source/host

	public void addDescriptionToSource(ManageRequest manageRequest) throws RestApiClientException {

		if (manageRequest == null) {
			throw new IllegalArgumentException("Manage Request value cannot be null");
		}

		String source = manageRequest.getSource();
		String description = manageRequest.getBody();

		addTagToSource(source, description);
	}

	// DELETE /api/manage/source/{source}/tags Remove all tags from a
	// source/host

	public void removeAllTagFromSource(String source) throws RestApiClientException {

		if (StringUtils.isEmpty(source)) {
			throw new IllegalArgumentException("Source value cannot be empty");
		}
		source = source.trim();
		String url = String.format("%s/api/manage/source/%s/tags/", serverurl, source);

		execute(HttpMethod.DELETE, url, token);
	}

	// DELETE /api/manage/source/{source}/tags/{tag} Remove tag from a
	// source/host

	public void removeAllTagFromSource(ManageRequest manageRequest) throws RestApiClientException {
		if (manageRequest == null) {
			throw new IllegalArgumentException("Manage Request value cannot be null");
		}
		String source = manageRequest.getSource();
		removeAllTagFromSource(source);
	}

	public TaggedSource parseTaggedSourceFromJsonString(String jsonResponseString) {
		TaggedSource taggedSource = null;
		if (StringUtils.isNotBlank(jsonResponseString)) {
			GsonBuilder gsonBuilder = new GsonBuilder();
			gsonBuilder.registerTypeAdapter(TaggedSource.class, new TaggedSourceDataDeserializer());
			Gson gson = gsonBuilder.create();

			// convert response Json string to TaggedSource object
			taggedSource = gson.fromJson(jsonResponseString, TaggedSource.class);
		}
		return taggedSource;
	}

	public TaggedSourceBundle parseTaggedSourceBundleFromJsonString(String jsonResponseString) {
		TaggedSourceBundle taggedSourceBundle = null;
		if (StringUtils.isNotBlank(jsonResponseString)) {
			GsonBuilder gsonBuilder = new GsonBuilder();
			gsonBuilder.registerTypeAdapter(TaggedSource.class, new TaggedSourceDataDeserializer());
			Gson gson = gsonBuilder.create();
			// convert response Json string to TaggedSourceBundle object
			taggedSourceBundle = gson.fromJson(jsonResponseString, TaggedSourceBundle.class);
		}
		return taggedSourceBundle;
	}

}
