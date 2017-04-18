package com.wavefront.rest.api.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.google.gson.Gson;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.utils.HttpMethod;

/*
 * This class is responsible for all the httpclient related operations.
 * 
 * */
public class BaseClient {

	private static final Logger logger = Logger.getLogger(BaseClient.class.getName());

	private final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.120 Safari/537.36";
	private String contentType = "application/x-www-form-urlencoded";

	private int httpResponseCode;
	private Map<String, String> httpResponseHeaders = new HashMap<String, String>();
	private String responseBodyAsString;
	private HttpRequestBase httpRequest;
	private CloseableHttpResponse response = null;
	private CloseableHttpClient httpclient = null;

	/**
	 * 
	 */
	public BaseClient() {
		super();
	}

	private CloseableHttpClient createHttpClient() {
		int timeout = 60;
		RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
				.setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();
		return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
	}

	private void configureMethod(HttpRequestBase httpMethod, String token) {
		httpRequest = httpMethod;
		// setting request headers.
		httpRequest.setProtocolVersion(new ProtocolVersion("HTTP", 1, 1));
		httpRequest.setHeader(HttpHeaders.USER_AGENT, USER_AGENT);
		httpRequest.setHeader(HttpHeaders.CONNECTION, "close");
		httpRequest.setHeader(HttpHeaders.CONTENT_TYPE, contentType);
		httpRequest.setHeader(HttpHeaders.ACCEPT, "application/json");
		httpRequest.setHeader(HttpHeaders.ACCEPT_ENCODING, "gzip, deflate");
		httpRequest.setHeader(HttpHeaders.ACCEPT_LANGUAGE, "en-US,en;q=0.8");
		httpRequest.setHeader("X-AUTH-TOKEN", token);
		// log the hwaders
		if (logger.isLoggable(Level.FINE)) {
			logRequestHeaders(this.httpRequest);
		}

	}

	private void logRequestHeaders(HttpRequestBase httpRequestBase) {
		Header[] requestHeaders = httpRequestBase.getAllHeaders();
		for (Header requestHeader : requestHeaders) {
			logger.log(Level.FINE, String.format("Request Header Name:%s,\tHeader Value:%s", requestHeader.getName(),
					requestHeader.getValue()));
		}
	}

	public int getHttpResponseCode() {
		return httpResponseCode;
	}

	private void setHttpResponseCode(int httpResponseCode) {
		this.httpResponseCode = httpResponseCode;
		if (logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, String.format("Http Response Code: %s", this.httpResponseCode));
		}
	}

	public Map<String, String> getHttpResponseHeaders() {
		return httpResponseHeaders;
	}

	private void setHttpResponseHeaders(HttpResponse response) {

		Header[] responseHeadersArray = response.getAllHeaders();
		for (Header header : responseHeadersArray) {

			if (logger.isLoggable(Level.FINE)) {
				logger.log(Level.FINE, String.format("Response Header Name:%s,\tHeader Value:%s", header.getName(),
						header.getValue()));
			}
			this.httpResponseHeaders.put(header.getName(), header.getValue());
		}
	}

	protected boolean execute(HttpMethod httpMethod, String url, String token) throws RestApiClientException {
		List<NameValuePair> formData = new ArrayList<NameValuePair>();
		return execute(httpMethod, url, token, formData);
	}

	protected boolean execute(HttpMethod httpMethod, String url, String token, List<NameValuePair> formData)
			throws RestApiClientException {
		return execute(httpMethod, url, token, formData, null);
	}

	protected boolean execute(HttpMethod httpMethod, String url, String token, StringEntity stringEntity)
			throws RestApiClientException {

		return execute(httpMethod, url, token, null, stringEntity);
	}

	private boolean execute(HttpMethod httpMethod, String url, String token, List<NameValuePair> formData,
			HttpEntity httpEntity) throws RestApiClientException {

		HttpRequestBase httpRequestBase = null;

		switch (httpMethod) {
		case POST:
			httpRequestBase = new HttpPost();
			return processPost(httpRequestBase, url, token, formData, httpEntity);
		case GET:
			httpRequestBase = new HttpGet();
			return processGet(httpRequestBase, url, token, formData);
		case DELETE:
			httpRequestBase = new HttpDelete();
			return processDelete(httpRequestBase, url, token);
		case HEAD:
			httpRequestBase = new HttpHead();

		case PATCH:
			httpRequestBase = new HttpPatch();

		case PUT:
			httpRequestBase = new HttpPut();

		default:
			throw new RestApiClientException("Http method mot supported: " + httpMethod);
		}

	}

	private boolean processPost(HttpRequestBase httpRequestBase, String url, String token, List<NameValuePair> formData,
			HttpEntity httpEntity) throws RestApiClientException {

		validate(httpRequestBase, url);
		configureMethod(httpRequestBase, token);
		processPostFormData(httpRequestBase, formData, httpEntity);
		return processRequest();
	}

	private boolean processGet(HttpRequestBase httpRequestBase, String url, String token, List<NameValuePair> formData)
			throws RestApiClientException {
		validate(httpRequestBase, url);
		configureMethod(httpRequestBase, token);
		processGetFormData(httpRequestBase, formData);
		return processRequest();
	}

	private boolean processDelete(HttpRequestBase httpRequestBase, String url, String token)
			throws RestApiClientException {
		validate(httpRequestBase, url);
		configureMethod(httpRequestBase, token);
		return processRequest();

	}

	private void validate(HttpRequestBase httpRequestBase, String url) throws RestApiClientException {
		// vlidate the uri
		validateUrl(url);
		try {
			httpRequestBase.setURI(new URI(url));
		} catch (URISyntaxException e) {
			String msg = String.format(
					"URISyntaxException while setting HttpRequestBase object, URISyntaxException: %s", e.getMessage());
			logger.log(Level.WARNING, msg);
			throw new RestApiClientException(msg, e);
		}

	}

	private boolean processRequest() throws RestApiClientException {
		try {

			httpclient = createHttpClient();
			// send request and receive the response.
			response = httpclient.execute(this.httpRequest);
			if (logger.isLoggable(Level.FINE)) {
				logger.log(Level.FINE, String.format("Response Code : %s, for Request Url :%s", httpResponseCode,
						httpRequest.getURI()));
			}
			// get the status code
			int httpResponseCode = response.getStatusLine().getStatusCode();

			// set response code
			setHttpResponseCode(httpResponseCode);
			// set response Headers
			setHttpResponseHeaders(response);
			// get the entity
			HttpEntity httpEntity = response.getEntity();
			String responseBody = null;
			if (httpEntity != null) {
				// parse the response body
				responseBody = EntityUtils.toString(httpEntity, StandardCharsets.UTF_8);
				// save response as JSON string
				setResponseBodyAsString(responseBody);
			}
			if (httpResponseCode >= 400 && httpResponseCode <= 599) {
					logger.log(Level.WARNING, responseBody);
				return false;
			}
		} catch (IOException e) {
			String msg = String.format("IOException while processing HttpRequest, Exception: %s", e.getMessage());
			logger.log(Level.WARNING, msg);
			throw new RestApiClientException(msg, e);
		} finally {
			// close connection
			closeConnection();
		}

		return true;
	}

	private void closeConnection() {
		if (this.response != null) {
			try {
				this.response.close();
			} catch (IOException e) {
				String msg = String.format("IOException while closing HttpResponse Connection, Exception: %s",
						e.getMessage());
				logger.log(Level.WARNING, msg);
			}
		}
		if (this.httpclient != null) {
			try {
				this.httpclient.close();
			} catch (IOException e) {
				String msg = String.format("IOException while closing HttpClient Connection, Exception: %s",
						e.getMessage());
				logger.log(Level.WARNING, msg);
			}
		}
	}

	private void processPostFormData(HttpRequestBase httpRequestBase, List<NameValuePair> formData,
			HttpEntity httpEntity) throws RestApiClientException {
		try {

			if (httpRequestBase instanceof HttpPost) {
				if (formData != null && formData.size() > 0) {
					((HttpPost) httpRequestBase).setEntity(new UrlEncodedFormEntity(formData));
				} else if (httpEntity != null && httpEntity instanceof StringEntity) {
					((HttpPost) httpRequestBase).setEntity(httpEntity);
				}
			}
		} catch (UnsupportedEncodingException e) {
			String msg = String.format(
					"UnsupportedEncodingException while papulating HttpPost request, UnsupportedEncodingException: %s",
					e.getMessage());
			logger.log(Level.WARNING, msg);
			throw new RestApiClientException(msg, e);
		}

	}

	private void processGetFormData(HttpRequestBase httpRequestBase, List<NameValuePair> formData)
			throws RestApiClientException {
		try {

			if (httpRequestBase instanceof HttpGet) {
				if (formData != null && formData.size() > 0) {
					URIBuilder uriBuilder = new URIBuilder(httpRequestBase.getURI());
					for (NameValuePair nameValuePair : formData) {
						String name = nameValuePair.getName();
						String value = nameValuePair.getValue();
						// skip any empty values.
						if (StringUtils.isEmpty(name) || StringUtils.isEmpty(value)) {
							continue;
						}
						uriBuilder.addParameter(name, value);
					}
					URI uri = uriBuilder.build();
					httpRequestBase.setURI(uri);
				}
			}
		} catch (URISyntaxException e) {
			String msg = String.format("URISyntaxException while papulating HttpGet request, URISyntaxException: %s",
					e.getMessage());
			logger.log(Level.WARNING, msg);
			throw new RestApiClientException(msg, e);
		}

	}

	public String getResponseBodyAsString() {
		return responseBodyAsString;
	}

	private void setResponseBodyAsString(String responseBodyAsString) {
		this.responseBodyAsString = responseBodyAsString;
		if (logger.isLoggable(Level.FINE)) {
			logger.log(Level.FINE, String.format("Http Response Body: %s", this.responseBodyAsString));
		}
	}

	private void validateUrl(String serverurl) throws RestApiClientException {
		String[] schemes = { "http", "https" };
		UrlValidator urlValidator = new UrlValidator(schemes);
		if (!urlValidator.isValid(serverurl)) {
			String msg = String.format("Malformed Url: %s", serverurl);
			logger.log(Level.WARNING, msg);
			throw new RestApiClientException(msg);
		}
	}

	/**
	 * @param contentType
	 *            the contentType to set
	 */
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public <T> T fromJson(String json, Class<T> type) {
		Gson gson = new Gson();
		return gson.fromJson(json, type);

	}

	public <T> T fromJson(String json, Type typeOfT) {
		Gson gson = new Gson();
		return gson.fromJson(json, typeOfT);
	}

	public String toJson(Object obj, Type type) {
		Gson gson = new Gson();
		return gson.toJson(obj, type);
	}

}
