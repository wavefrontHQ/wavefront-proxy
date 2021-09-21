package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:29 AM
 */
enum PathConverter implements JsonSerializer<Path>, JsonDeserializer<Path> {
  INSTANCE;

  private PathConverter() {
  }

  public JsonElement serialize(Path src, Type typeOfSrc, JsonSerializationContext context) {
    return new JsonPrimitive(src.toAbsolutePath().toString());
  }

  public Path deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
    return Paths.get(json.getAsString());
  }
}