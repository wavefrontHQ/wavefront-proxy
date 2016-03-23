package com.wavefront.rest.api.client.chart.response;

import java.lang.reflect.Type;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/*
 * 
 * Custom Json Deserializer fpr Time Series data.
 * 
 * */

public class TimeSeriesDataDeserializer implements JsonDeserializer<TimeSeriesData> {

	@Override
	public TimeSeriesData deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {

		Long timeStamp = 0l;
		Double value = 0.0;
		try {
			JsonArray jsonArray = json.getAsJsonArray();
			timeStamp = jsonArray.get(0).getAsLong();

			value = jsonArray.get(1).getAsDouble();
		} catch (JsonParseException jpe) {
			// TODO : LOG the issue here.

			return new TimeSeriesData(timeStamp, value);
		}

		return new TimeSeriesData(timeStamp, value);
	}

}
