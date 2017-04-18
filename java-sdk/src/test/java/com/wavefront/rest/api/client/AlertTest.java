package com.wavefront.rest.api.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.NameValuePair;

import com.google.gson.Gson;
import com.wavefront.rest.api.client.alert.AlertClient;
import com.wavefront.rest.api.client.alert.request.AlertRequest;
import com.wavefront.rest.api.client.alert.response.Alert;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.utils.HttpMethod;
 import com.wavefront.rest.api.client.utils.Severity;
 
public class AlertTest {

	public static void main(String[] args) throws RestApiClientException {

//		String[] socialSourceList = { "CHOICE", "DEALERRATER", "TRIPADVISOR", "YELP", "ORBITZ", "AGODA", "CTRIP",
//				"DIANPING", "BEST_CHINA_HOTEL", "EXPEDIA", "HOTELS", "QUNAR", "HRS", "ELONG", "BOOKING", "GOOGLE_LOCAL",
//				"WOTIF", "LATE_ROOMS", "RAKUTEN_TRAVEL", "PRICELINE", "HOTEL_DE", "OPENTABLE", "TRAVELOCITY", "CARS",
//				"HOLIDAY_CHECK" };

		
		String[] socialSourceList = { "CHOICE" };
		
		
		
		String token = "";
		String serverUrl = "https://metrics.wavefront.com";

		AlertClient ac = new AlertClient(serverUrl, token);

		ArrayList<String> customerTag = new ArrayList<String>();
		
		customerTag.add("customersuccess");
		customerTag.add("test");
		
		List<Alert> list = ac.getAllAlertsList(customerTag, null);
		
		
		System.out.println(list);
		
		
		
		
		
		
		
//		String name = "Social : Reviews Scraped below Threshold";

	 

//		for (String socialSource : socialSourceList) {
//
//			name =   "Social : Reviews Scraped below Threshold - " + socialSource;
//
//			String	condition = "mavg(720h,sum(rate(ts(Review.Gathered.count, SocialSource=" + socialSource
//					+ ")))) * 0.3 > mavg(4h,sum(rate(ts(Review.Gathered.count, SocialSource=" + socialSource
//					+ "))))";
//
//			String displayExpression = "";
//			Integer minutes = 2;
//
//			String notifications = "test@test.com";
//
//			Severity severity = Severity.WARN;
//			String privateTags = "";
//
//			String sharedTags = "SocialMedia";
//			String additionalInformation = "You were interested in knowing if the duplicate detector failed. The (Reviews Updated + Reviews Duplicated) in 4 hours has fallen to below 30% of the average of the last month and hence this alert has been fired.";
//
//			
//			
//			System.out.println(condition);
//			
//			
//			
//			AlertRequest ar = new AlertRequest();
//			
//			
//			ar.setName(name);
//			ar.setCondition(condition);
//			ar.setDisplayExpression(displayExpression);
//			ar.setMinutes(minutes);
//			ar.setNotifications(notifications);
//			ar.setSeverity(severity);
//			ar.setPrivateTags(privateTags);
//			ar.setSharedTags(sharedTags);
//			ar.setAdditionalInformation(additionalInformation);
//			
//			
//			 Gson gson = new Gson();
//			 String json =  gson.toJson(ar, AlertRequest.class);
//			
//			 System.out.println(json);
//		 
//			 
//			 
//			
//			Alert alert = ac.createAlert(name, condition, displayExpression, minutes, notifications, severity,
//					privateTags, sharedTags, additionalInformation);
//			
//			
//			if(alert == null){
//				String response = ac.getResponseBodyAsString();
//				System.out.println(response);
//			}else{
//				System.out.println(alert);
//			}
//			
			
			
			 
//		}

	}

}
