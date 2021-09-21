package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.time.ZoneId;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:51 AM
 */
public class ZoneIdSerializer extends Serializer<ZoneId> {
  public static final ZoneIdSerializer INSTANCE = new ZoneIdSerializer();

  public ZoneIdSerializer() {
    this.setAcceptsNull(false);
    this.setImmutable(true);
  }

  public void write(Kryo kryo, Output output, ZoneId object) {
    write(output, object);
  }

  public ZoneId read(Kryo kryo, Input input, Class<ZoneId> type) {
    return read(input);
  }

  protected static void write(Output output, ZoneId object) {
    output.writeString(object.getId());
  }

  protected static ZoneId read(Input input) {
    String id = input.readString();
    return ZoneId.of(id);
  }
}
