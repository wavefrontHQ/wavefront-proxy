package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.stream.JsonWriter;

import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.FNVHash;

import java.io.IOException;
import java.io.Writer;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:30 AM
 */
public final class HashingJsonWriter extends JsonWriter {
  private static final int BEG_ARRAY = 1;
  private static final int END_ARRAY = -1;
  private static final int BEG_OBJ = 2;
  private static final int END_OBJ = -2;
  private static final int NAME = 3;
  private static final int STRING = 4;
  private static final int NULL = 5;
  private static final int NUMBER = 6;
  private static final int BOOL = 7;
  private long hash;
  private static final Writer UNWRITABLE_WRITER = new Writer() {
    public void write(char[] buffer, int offset, int counter) {
      throw new AssertionError();
    }

    public void flush() throws IOException {
      throw new AssertionError();
    }

    public void close() throws IOException {
      throw new AssertionError();
    }
  };

  public HashingJsonWriter(long hash) {
    super(UNWRITABLE_WRITER);
    this.setLenient(true);
    this.setSerializeNulls(false);
    this.hash = hash;
  }

  public JsonWriter beginArray() throws IOException {
    this.hash = FNVHash.compute(1, this.hash);
    return this;
  }

  public JsonWriter endArray() throws IOException {
    this.hash = FNVHash.compute(-1, this.hash);
    return this;
  }

  public JsonWriter beginObject() throws IOException {
    this.hash = FNVHash.compute(2, this.hash);
    return this;
  }

  public JsonWriter endObject() throws IOException {
    this.hash = FNVHash.compute(-2, this.hash);
    return this;
  }

  public JsonWriter name(String name) throws IOException {
    long h = this.hash;
    h = FNVHash.compute(3, h);
    h = FNVHash.compute(name, h);
    this.hash = h;
    return this;
  }

  public JsonWriter value(String value) throws IOException {
    long h = this.hash;
    h = FNVHash.compute(4, h);
    if (value != null) {
      h = FNVHash.compute(value, h);
    } else {
      h = FNVHash.compute(5, h);
    }

    this.hash = h;
    return this;
  }

  public JsonWriter nullValue() throws IOException {
    this.hash = FNVHash.compute(5, this.hash);
    return this;
  }

  public JsonWriter value(boolean value) throws IOException {
    long h = this.hash;
    h = FNVHash.compute(7, h);
    h = FNVHash.compute(Boolean.hashCode(value), h);
    this.hash = h;
    return this;
  }

  public JsonWriter value(double value) throws IOException {
    long h = this.hash;
    h = FNVHash.compute(6, h);
    long bits = Double.doubleToLongBits(value);
    h = FNVHash.compute((int)bits, h);
    h = FNVHash.compute((int)(bits >> 32), h);
    this.hash = h;
    return this;
  }

  public JsonWriter value(Boolean value) throws IOException {
    long h = this.hash;
    h = FNVHash.compute(7, h);
    if (value != null) {
      h = FNVHash.compute(Boolean.hashCode(value), h);
    } else {
      h = FNVHash.compute(5, h);
    }

    this.hash = h;
    return this;
  }

  public JsonWriter value(long value) throws IOException {
    long h = this.hash;
    h = FNVHash.compute(6, h);
    h = FNVHash.compute((int)value, h);
    h = FNVHash.compute((int)(value >> 32), h);
    this.hash = h;
    return this;
  }

  public JsonWriter value(Number value) throws IOException {
    long h = this.hash;
    h = FNVHash.compute(6, h);
    if (value != null) {
      h = FNVHash.compute(value.toString(), h);
    } else {
      h = FNVHash.compute(5, h);
    }

    this.hash = h;
    return this;
  }

  public void flush() throws IOException {
  }

  public void close() throws IOException {
  }

  public long getHash() {
    return this.hash;
  }
}
