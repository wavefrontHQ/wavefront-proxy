package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.UUID;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 1:58 PM
 */
public class UUIDSerializer extends Serializer<UUID> {
  public static final UUIDSerializer INSTANCE = new UUIDSerializer();

  public UUIDSerializer() {
    this.setImmutable(true);
    this.setAcceptsNull(false);
  }

  public void write(Kryo kryo, Output output, UUID uuid) {
    output.writeLong(uuid.getMostSignificantBits(), false);
    output.writeLong(uuid.getLeastSignificantBits(), false);
  }

  public UUID read(Kryo kryo, Input input, Class<UUID> uuidClass) {
    return new UUID(input.readLong(false), input.readLong(false));
  }
}
