package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;

import java.lang.reflect.Type;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:29 AM
 */
public final class UtcDateTypeAdapter implements JsonSerializer<Date>, JsonDeserializer<Date> {
  private static final DateTimeFormatter DATE_FORMAT;
  public static final UtcDateTypeAdapter INSTANCE;

  private UtcDateTypeAdapter() {
  }

  public synchronized JsonElement serialize(Date date, Type type, JsonSerializationContext jsonSerializationContext) {
    String dateFormatAsString = DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
    return new JsonPrimitive(dateFormatAsString);
  }

  public synchronized Date deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) {
    try {
      ZonedDateTime zdt = ZonedDateTime.parse(jsonElement.getAsString(), DATE_FORMAT);
      return Date.from(zdt.toInstant());
    } catch (DateTimeParseException var5) {
      throw new JsonSyntaxException(jsonElement.getAsString(), var5);
    }
  }

  static {
    DATE_FORMAT = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC"));
    INSTANCE = new UtcDateTypeAdapter();
  }
}
