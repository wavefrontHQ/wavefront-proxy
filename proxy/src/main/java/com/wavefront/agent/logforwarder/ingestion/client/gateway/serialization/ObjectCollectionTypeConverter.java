package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:28 AM
 */
public enum ObjectCollectionTypeConverter implements JsonSerializer<Collection<Object>>, JsonDeserializer<Collection<Object>> {
  INSTANCE;

  public static final Type TYPE_LIST = TypeTokens.LIST_OF_OBJECTS;
  public static final Type TYPE_SET = TypeTokens.SET_OF_OBJECTS;
  public static final Type TYPE_COLLECTION = TypeTokens.COLLECTION_OF_OBJECTS;

  private ObjectCollectionTypeConverter() {
  }

  public JsonElement serialize(Collection<Object> set, Type type, JsonSerializationContext context) {
    JsonArray setObject = new JsonArray();
    Iterator var5 = set.iterator();

    while(var5.hasNext()) {
      Object e = var5.next();
      if (e == null) {
        setObject.add(JsonNull.INSTANCE);
      } else if (e instanceof JsonElement) {
        setObject.add((JsonElement)e);
      } else {
        setObject.add(context.serialize(e));
      }
    }

    return setObject;
  }

  public Collection<Object> deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
    if (!json.isJsonArray()) {
      throw new JsonParseException("Expecting a json array object but found: " + json);
    } else {
      Object result;
      if (TYPE_SET.equals(type)) {
        result = new HashSet();
      } else {
        if (!TYPE_LIST.equals(type) && !TYPE_COLLECTION.equals(type)) {
          throw new JsonParseException("Unexpected target type: " + type);
        }

        result = new LinkedList();
      }

      JsonArray jsonArray = json.getAsJsonArray();
      Iterator var6 = jsonArray.iterator();

      while(var6.hasNext()) {
        JsonElement entry = (JsonElement)var6.next();
        if (entry.isJsonNull()) {
          ((Collection)result).add((Object)null);
        } else if (entry.isJsonPrimitive()) {
          JsonPrimitive elem = entry.getAsJsonPrimitive();
          Object value = null;
          if (elem.isBoolean()) {
            value = elem.getAsBoolean();
          } else if (elem.isString()) {
            value = elem.getAsString();
          } else {
            if (!elem.isNumber()) {
              throw new RuntimeException("Unexpected value type for json element:" + elem);
            }

            BigDecimal num = elem.getAsBigDecimal();

            try {
              value = num.longValueExact();
            } catch (ArithmeticException var12) {
              value = num.doubleValue();
            }
          }

          ((Collection)result).add(value);
        } else {
          ((Collection)result).add(entry);
        }
      }

      return (Collection)result;
    }
  }
}
