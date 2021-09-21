package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.time.Instant;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:49 AM
 */
public class InstantSerializer extends Serializer<Instant> {
  public static final InstantSerializer INSTANCE = new InstantSerializer();

  public InstantSerializer() {
    this.setAcceptsNull(false);
    this.setImmutable(true);
  }

  public void write(Kryo kryo, Output output, Instant object) {
    write(output, object);
  }

  public Instant read(Kryo kryo, Input input, Class<Instant> type) {
    return read(input);
  }

  protected static void write(Output output, Instant object) {
    output.writeLong(object.getEpochSecond());
    output.writeInt(object.getNano());
  }

  protected static Instant read(Input input) {
    long epochSecond = input.readLong();
    int nano = input.readInt();
    return Instant.ofEpochSecond(epochSecond, (long)nano);
  }
}