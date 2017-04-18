package com.wavefront.rest.api.client.exception;

public class RestApiClientException extends Exception {

 
	private static final long serialVersionUID = -8860418700090464416L;

	public RestApiClientException() {
		super();
	}

	public RestApiClientException(String message) {
		super(message);
	}

	public RestApiClientException(Throwable cause) {
		super(cause);
	}

	public RestApiClientException(String message, Throwable cause) {
		super(message, cause);
	}

	public RestApiClientException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
