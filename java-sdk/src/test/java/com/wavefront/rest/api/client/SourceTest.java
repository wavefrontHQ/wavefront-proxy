package com.wavefront.rest.api.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;

import com.google.gson.Gson;
import com.wavefront.rest.api.client.alert.AlertClient;
import com.wavefront.rest.api.client.alert.request.AlertRequest;
import com.wavefront.rest.api.client.alert.response.Alert;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.manage.ManageClient;
import com.wavefront.rest.api.client.manage.response.TaggedSourceBundle;
import com.wavefront.rest.api.client.utils.HttpMethod;
 import com.wavefront.rest.api.client.utils.Severity;
 
public class SourceTest {

	public static void main(String[] args) throws RestApiClientException {

 
		
		String token = " ";
		String serverUrl = "https://metrics.wavefront.com";

		ManageClient mc = new ManageClient(serverUrl, token);

	 
		
		String  list = mc.getSourcesResultAsString(null, null, null);
		
		System.out.println(list);
	 	
		
		
		
 

	}

}
