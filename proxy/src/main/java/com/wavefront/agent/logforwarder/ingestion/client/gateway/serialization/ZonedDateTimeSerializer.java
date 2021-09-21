package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:48 AM
 */
public class ZonedDateTimeSerializer extends Serializer<ZonedDateTime> {
  public static final ZonedDateTimeSerializer INSTANCE = new ZonedDateTimeSerializer();

  private ZonedDateTimeSerializer() {
    this.setAcceptsNull(false);
    this.setImmutable(true);
  }

  public void write(Kryo kryo, Output output, ZonedDateTime object) {
    InstantSerializer.write(output, object.toInstant());
    ZoneIdSerializer.write(output, object.getZone());
  }

  public ZonedDateTime read(Kryo kryo, Input input, Class<ZonedDateTime> type) {
    Instant instant = InstantSerializer.read(input);
    ZoneId zone = ZoneIdSerializer.read(input);
    return ZonedDateTime.ofInstant(instant, zone);
  }
}
