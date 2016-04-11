package com.wavefront.agent;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

/**
 * Deserializer of ResubmissionTasks from JSON.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class ResubmissionTaskDeserializer implements
    JsonSerializer<Object>, JsonDeserializer<Object> {

  private static final String CLASS_META_KEY = "CLASS_META_KEY";
  private static final Gson gson = new Gson();

  @Override
  public Object deserialize(JsonElement jsonElement, Type type,
                            JsonDeserializationContext jsonDeserializationContext)
      throws JsonParseException {
    JsonObject jsonObj = jsonElement.getAsJsonObject();
    if (!jsonObj.has(CLASS_META_KEY)) {
      // cannot deserialize.
      return null;
    }
    String className = jsonObj.get(CLASS_META_KEY).getAsString();
    try {
      Class<?> clz = Class.forName(className);
      return gson.fromJson(jsonElement, clz);
    } catch (ClassNotFoundException e) {
      // can no longer parse results.
      return null;
    }
  }

  @Override
  public JsonElement serialize(Object object, Type type,
                               JsonSerializationContext jsonSerializationContext) {
    JsonElement jsonEle = gson.toJsonTree(object);
    jsonEle.getAsJsonObject().addProperty(CLASS_META_KEY,
        object.getClass().getName());
    return jsonEle;
  }

}