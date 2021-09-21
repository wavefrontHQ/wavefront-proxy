package com.wavefront.agent.logforwarder.ingestion.client.gateway.utils;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:31 AM
 */
public class FNVHash {
  public static final long FNV_PRIME = 1099511628211L;
  public static final long FNV_OFFSET_MINUS_MSB = 4695981039346656037L;

  private FNVHash() {
  }

  public static long compute(CharSequence data) {
    return compute(data, 4695981039346656037L);
  }

  public static long compute(CharSequence data, long hash) {
    int length = data.length();

    for(int pos = 0; pos < length; ++pos) {
      int code = data.charAt(pos);
      hash ^= (long)(code + code << 8);
      hash *= 1099511628211L;
    }

    return hash;
  }

  public static long compute(int code, long hash) {
    hash ^= (long)(code + code << 8);
    hash *= 1099511628211L;
    return hash;
  }

  public static int compute32(CharSequence data) {
    return compute32(data, 4695981039346656037L);
  }

  public static int compute32(CharSequence data, long hash) {
    hash = compute(data, hash);
    return (int)(hash >> 31 | 4294967295L & hash);
  }

  public static long compute(byte[] data, int offset, int length) {
    return compute(data, offset, length, 4695981039346656037L);
  }

  public static long compute(byte[] data, int offset, int length, long hash) {
    for(int i = offset; i < length; ++i) {
      int code = data[i];
      hash ^= (long)code;
      hash *= 1099511628211L;
    }

    return hash;
  }
}
