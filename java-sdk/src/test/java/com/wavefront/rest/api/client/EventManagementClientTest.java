
package com.wavefront.rest.api.client;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Locale;
import java.util.ResourceBundle;

import com.wavefront.rest.api.client.event.EventClient;
import com.wavefront.rest.api.client.event.request.EventRequest;
import com.wavefront.rest.api.client.event.response.Event;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.utils.Severity;

public class EventManagementClientTest {

	public static void main(String[] args) throws RestApiClientException, InterruptedException, MalformedURLException {

 		EventClient eventClient = new EventClient("https://metrics.wavefront.com", " ");
		 
 	
// 	EventClient eventClient = new EventClient( );

//		usingEventRequestObject(eventClient);
//
		usingParameters(eventClient);
//		
		
		
	 
		
		
		
		
		Event event = new Event();
//		
//		String[] arr = {"host1", "host2"};
//		event.setHosts(arr);
//		
//		String json = eventClient.toJson( event, Event.class);
//		
//		System.out.println(json);
		
		
//		String json = eventClient.getCreateEventAsJsonString("MyEvent", null, null, true, "", arr, Severity.INFO, null);
//		
//		System.out.println(json);
		
		
		
	}

	private static void usingParameters(EventClient eventClient) throws RestApiClientException, InterruptedException {

		String name = "Test-event-" + ((int) (Math.random() * 10000));

		// long startTime = System.currentTimeMillis();
		//

		ArrayList<String> hosts   =  new ArrayList<String>();
		hosts.add("h1");
		hosts.add("h2");
		System.out.println("Creating an event");
		
		
		Event nevent = eventClient.createEvent  (name, null, null, true, null, hosts, null, null);
		System.out.println(	nevent);
		
		
		System.out.println(	eventClient.getResponseBodyAsString());
		
		
		
		
		 System.exit(1);
		
		Event event = eventClient.createEvent(name, null, null, false, null, null, null, null);
		
		System.out.println("Response Code: " + eventClient.getHttpResponseCode());
		System.out.println("Event Created: " + event);
		System.out.println("Event Created json String: " + eventClient.getResponseBodyAsString());
		System.out.println("==========================================");

		System.out.println("Next Closing an event");
		Thread.currentThread().sleep(2000);

		event = eventClient.closeEvent(name, event.getStartTime(), null);
		System.out.println("Response Code: " + eventClient.getHttpResponseCode());
		System.out.println("Event Closed: " + event);
		System.out.println("Event Closed json String: " + eventClient.getResponseBodyAsString());
		System.out.println("==========================================");

		System.out.println("Next Deleting an event");
		Thread.currentThread().sleep(2000);

		eventClient.deleteEvent(name, event.getStartTime());
		System.out.println("Response Code: " + eventClient.getHttpResponseCode());
		System.out.println("Event Deleted: " + event);

		System.out.println("==========================================");

	}

	private static void usingEventRequestObject(EventClient eventClient)
			throws RestApiClientException, InterruptedException {

		String name = "Test-event-" + ((int) (Math.random() * 10000));

		// create a new Event Request
		System.out.println("Creating an event");
		EventRequest eventRequest = new EventRequest();
		eventRequest.setName(name);

		Event event = eventClient.createEvent(eventRequest);

		System.out.println("Response Code: " + eventClient.getHttpResponseCode());
		System.out.println("Event Created: " + event);
		System.out.println("Event Created json String: " + eventClient.getResponseBodyAsString());
		System.out.println("==========================================");

		System.out.println("Next Closing an event");
		Thread.currentThread().sleep(2000);

		eventRequest.setStartTime(event.getStartTime());
		System.out.println("Response Code: " + eventClient.getHttpResponseCode());
		System.out.println("Event Closed: " + event);
		System.out.println("Event Closed json String: " + eventClient.getResponseBodyAsString());
		System.out.println("==========================================");

		System.out.println("Next Deleting an event");
		Thread.currentThread().sleep(2000);

		eventClient.deleteEvent(eventRequest);
		System.out.println("Response Code: " + eventClient.getHttpResponseCode());
		System.out.println("Event Deleted: " + event);

		System.out.println("==========================================");

	}

}
