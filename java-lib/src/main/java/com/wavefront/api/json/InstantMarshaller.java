package com.wavefront.api.json;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.joda.time.Instant;

import java.io.IOException;

/**
 * Marshaller for Joda Instant to JSON and back.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class InstantMarshaller {

  public static class Serializer extends JsonSerializer<Instant> {

    @Override
    public void serialize(Instant value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
      jgen.writeNumber(value.getMillis());
    }
  }


  public static class Deserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      return new Instant(jp.getLongValue());
    }
  }
}
