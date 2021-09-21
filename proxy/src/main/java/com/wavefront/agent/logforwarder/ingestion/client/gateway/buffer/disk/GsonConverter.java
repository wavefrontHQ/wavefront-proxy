package com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.disk;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.util.Base64;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.buffer.disk.DiskbackedQueue;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 2:24 PM
 */
public class GsonConverter<T> implements DiskbackedQueue.Converter<T> {
  private final Gson gson;
  private final Class<T> type;

  public GsonConverter(Class<T> type) {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(byte[].class, new ByteArrayToBase64TypeAdapter());
    this.gson = builder.create();
    this.type = type;
  }

  public T fromBytes(byte[] bytes) {
    Reader reader = new InputStreamReader(new ByteArrayInputStream(bytes));
    return this.gson.fromJson(reader, this.type);
  }

  public void toBytes(T object, OutputStream bytes) throws IOException {
    Writer writer = new OutputStreamWriter(bytes);
    this.gson.toJson(object, writer);
    writer.close();
  }

  public static class ByteArrayToBase64TypeAdapter implements JsonSerializer<byte[]>, JsonDeserializer<byte[]> {
    private static final Base64.Decoder DECODER = Base64.getDecoder();
    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    public ByteArrayToBase64TypeAdapter() {
    }

    public byte[] deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
      return DECODER.decode(json.getAsString());
    }

    public JsonElement serialize(byte[] src, Type typeOfSrc, JsonSerializationContext context) {
      return new JsonPrimitive(ENCODER.encodeToString(src));
    }
  }
}