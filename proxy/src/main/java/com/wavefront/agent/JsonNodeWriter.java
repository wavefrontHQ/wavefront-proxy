package com.wavefront.agent;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

/**
 * Writer that serializes JsonNodes.
 *
 * @author Clement Pang (clement@wavefront.com)
 */
public class JsonNodeWriter implements MessageBodyWriter<JsonNode> {

  private final ObjectMapper mapper = new ObjectMapper();
  private final JsonFactory factory = new JsonFactory();

  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return JsonNode.class.isAssignableFrom(type);
  }

  @Override
  public long getSize(JsonNode jsonNode, Class<?> type, Type genericType, Annotation[] annotations,
                      MediaType mediaType) {
    return -1;
  }

  @Override
  public void writeTo(JsonNode jsonNode, Class<?> type, Type genericType, Annotation[] annotations,
                      MediaType mediaType, MultivaluedMap<String, Object> httpHeaders,
                      OutputStream entityStream) throws IOException, WebApplicationException {
    JsonGenerator generator = factory.createGenerator(entityStream);
    mapper.writeTree(generator, jsonNode);
  }
}
