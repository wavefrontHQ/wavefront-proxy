package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.nio.ByteBuffer;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 1:59 PM
 */
final class ByteBufferSerializer extends Serializer<ByteBuffer> {
  public static final Serializer<ByteBuffer> INSTANCE = new ByteBufferSerializer();

  public ByteBufferSerializer() {
    this.setImmutable(true);
    this.setAcceptsNull(false);
  }

  public void write(Kryo kryo, Output output, ByteBuffer object) {
    int count = object.limit();
    byte[] array = object.array();
    output.writeInt(count);
    output.writeBytes(array, 0, count);
  }

  public ByteBuffer read(Kryo kryo, Input input, Class<ByteBuffer> type) {
    int length = input.readInt();
    return ByteBuffer.wrap(input.readBytes(length));
  }
}