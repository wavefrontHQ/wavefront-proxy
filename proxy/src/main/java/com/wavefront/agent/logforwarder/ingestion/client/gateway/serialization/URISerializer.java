package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 1:59 PM
 */
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.net.URI;

public class URISerializer extends Serializer<URI> {
  public static final URISerializer INSTANCE = new URISerializer();

  public URISerializer() {
    this.setImmutable(true);
    this.setAcceptsNull(false);
  }

  public void write(Kryo kryo, Output output, URI uri) {
    output.writeString(uri.toString());
  }

  public URI read(Kryo kryo, Input input, Class<URI> uriClass) {
    return URI.create(input.readString());
  }
}