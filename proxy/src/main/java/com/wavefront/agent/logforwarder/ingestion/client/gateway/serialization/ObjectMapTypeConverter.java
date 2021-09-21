package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:22 AM
 */
public enum ObjectMapTypeConverter implements JsonSerializer<Map<String, Object>>, JsonDeserializer<Map<String, Object>> {
  INSTANCE;

  public static final Type TYPE = TypeTokens.MAP_OF_OBJECTS_BY_STRING;

  private ObjectMapTypeConverter() {
  }

  public JsonElement serialize(Map<String, Object> map, Type type, JsonSerializationContext context) {
    JsonObject mapObject = new JsonObject();
    Iterator var5 = map.entrySet().iterator();

    while(var5.hasNext()) {
      Map.Entry<String, Object> e = (Map.Entry)var5.next();
      Object v = e.getValue();
      if (v == null) {
        mapObject.add((String)e.getKey(), JsonNull.INSTANCE);
      } else if (v instanceof JsonElement) {
        mapObject.add((String)e.getKey(), (JsonElement)v);
      } else {
        mapObject.add((String)e.getKey(), context.serialize(v));
      }
    }

    return mapObject;
  }

  public Map<String, Object> deserialize(JsonElement json, Type unused, JsonDeserializationContext context) throws JsonParseException {
    if (!json.isJsonObject()) {
      throw new JsonParseException("Expecting a json Map object but found: " + json);
    } else {
      Map<String, Object> result = new HashMap();
      JsonObject jsonObject = json.getAsJsonObject();
      Iterator var6 = jsonObject.entrySet().iterator();

      while(var6.hasNext()) {
        Map.Entry<String, JsonElement> entry = (Map.Entry)var6.next();
        String key = (String)entry.getKey();
        JsonElement element = (JsonElement)entry.getValue();
        if (element.isJsonNull()) {
          result.put(key, (Object)null);
        } else if (element.isJsonPrimitive()) {
          JsonPrimitive elem = element.getAsJsonPrimitive();
          Object value = null;
          if (elem.isBoolean()) {
            value = elem.getAsBoolean();
          } else if (elem.isString()) {
            value = elem.getAsString();
          } else {
            if (!elem.isNumber()) {
              throw new RuntimeException("Unexpected value type for json element key:" + key + " value:" + element);
            }

            BigDecimal num = elem.getAsBigDecimal();

            try {
              value = num.longValueExact();
            } catch (ArithmeticException var14) {
              value = num.doubleValue();
            }
          }

          result.put(key, value);
        } else {
          result.put(key, element);
        }
      }

      return result;
    }
  }
}
