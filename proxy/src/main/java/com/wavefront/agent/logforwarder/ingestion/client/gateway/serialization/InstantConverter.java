package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:23 AM
 */
public enum InstantConverter implements JsonSerializer<Instant>, JsonDeserializer<Instant> {
  INSTANCE;

  private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;
  public static final Type TYPE = (new TypeToken<Instant>() {
  }).getType();

  private InstantConverter() {
  }

  public JsonElement serialize(Instant src, Type typeOfSrc, JsonSerializationContext context) {
    return new JsonPrimitive(FORMATTER.format(src));
  }

  public Instant deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
    return (Instant)FORMATTER.parse(json.getAsString(), Instant::from);
  }
}
