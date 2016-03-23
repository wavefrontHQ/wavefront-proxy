/**
 * 
 */
package com.wavefront.rest.api.util.aws;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang3.StringUtils;

import com.wavefront.rest.api.util.*;
import com.wavefront.rest.api.client.chart.*;
import com.wavefront.rest.api.client.chart.response.QueryResult;
import com.wavefront.rest.api.client.chart.response.TimeSeries;
import com.wavefront.rest.api.client.exception.RestApiClientException;
import com.wavefront.rest.api.client.utils.Summarization;


/**
 * @author salildeshmukh
 *
 */
public class AwsBillingDashboardGenerator  {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws RestApiClientException {
		// TODO Auto-generated method stub
		
		QueryClient queryClient = new QueryClient("https://try.wavefront.com",
				""); 
		
		String query = "if(count(ts(aws.instance.price or aws.reservedInstance.count),instanceType,availabilityZone),1)";
		long startTime=1453913604;
		long endTime = 1453935204;
		String granularity="s";
		Summarization summ = Summarization.MEAN ;

		
		//Get result in QueryResult Obj. Cant find the tags in this obj
		QueryResult queryResult = queryClient.performChartingQuery("", query, startTime,endTime, granularity, "", false, false, summ, true, false, true);
		System.out.println("QueryResult  : " + queryResult.toString() + "\n\n\n\n");
		
	 
		List<TimeSeries> timeseries = queryResult.getTimeseries();
		for (ListIterator<TimeSeries> iter = timeseries.listIterator(); iter.hasNext(); ) {
		     TimeSeries ts = iter.next();
		    
			System.out.println(ts.toString());
		    
		}

	 
			
		
		
//		//Get result as Json String. This works well 
//		String jsonBody = queryClient.getChartingQueryResultAsJsonString("", query, startTime,endTime, granularity, "", false, false, summ, true, false, true);
//		System.out.println("JsonBody  : " + jsonBody + "\n\n\n\n");
		
		
	}

}
