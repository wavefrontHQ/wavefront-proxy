package com.wavefront.rest.api.client.manage.response;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

/*
 * 
 * Custom Json Deserializer fpr TaggedSource
 * 
 * */

public class TaggedSourceDataDeserializer implements JsonDeserializer<TaggedSource> {

	@Override
	public TaggedSource deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
		List<String> userTags = new ArrayList<String>();
		String description = new String();
		String hostname = new String();
		
		
		try {
			JsonObject object = json.getAsJsonObject();
			Set<Map.Entry<String, JsonElement>> entries = object.entrySet();
			for (Map.Entry<String, JsonElement> entry : entries) {
				String key = entry.getKey();
				if (StringUtils.isNotBlank(key)) {
					key = key.trim();
					if (key.equalsIgnoreCase("userTags")) {
						JsonArray jsonArray = entry.getValue().getAsJsonArray();
						for (int i = 0; i < jsonArray.size(); i++) {
							userTags.add(jsonArray.get(i).toString());
						}
					} else if (key.equalsIgnoreCase("description")) {
 					
						description = new String(entry.getValue().getAsString());
					}else if (key.equalsIgnoreCase("hostname")) {

 				
						hostname = new String(entry.getValue().getAsString());
					}
				}
			}
		} catch (JsonParseException jpe) {
			// TODO : LOG the issue here.

		}
		
		TaggedSource  taggedSource = new TaggedSource();
		
		taggedSource.setDescription(description);
		taggedSource.setHostname(hostname);
		taggedSource.setUserTags(userTags);
		
		return taggedSource;
	}

}
