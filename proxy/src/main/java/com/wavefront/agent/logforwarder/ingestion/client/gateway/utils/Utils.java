package com.wavefront.agent.logforwarder.ingestion.client.gateway.utils;

import com.vmware.xenon.common.FNVHash;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization.GsonSerializers;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization.JsonMapper;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization.KryoSerializers;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization.StringBuilderThreadLocal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 10:49 AM
 */
public class Utils {
  public static final String CHARSET = "UTF-8";
  private static final ConcurrentMap<String, String> KINDS = new ConcurrentSkipListMap();
  private static final StringBuilderThreadLocal builderPerThread = new StringBuilderThreadLocal();
  public static final long DEFAULT_TIME_DRIFT_THRESHOLD_MICROS;

  private Utils() {
  }

  public static long getSystemNowMicrosUtc() {
    return System.currentTimeMillis() * 1000L;
  }

  public static String buildKind(Class<?> type) {
    return (String)KINDS.computeIfAbsent(type.getName(), (name) -> {
      return toDocumentKind(type);
    });
  }

  public static String toDocumentKind(Class<?> type) {
    return type.getCanonicalName().replace('.', ':');
  }

  public static String toJsonHtml(Object body) {
    if (body instanceof String) {
      return (String)body;
    } else {
      StringBuilder content = getBuilder();
      JsonMapper mapper = GsonSerializers.getJsonMapperFor(body);
      mapper.toJsonHtml(body, content);
      return content.toString();
    }
  }

  public static String computeHash(CharSequence content) {
    return Long.toHexString(FNVHash.compute(content));
  }

  public static StringBuilder getBuilder() {
    return builderPerThread.get();
  }

  public static boolean isContentTypeKryoBinary(String contentType) {
    return contentType.length() == "application/kryo-octet-stream".length() && contentType.charAt(12) == 'k' && contentType.charAt(13) == 'r' && contentType.charAt(14) == 'y' && contentType.charAt(15) == 'o';
  }

  public static boolean isContentTypeText(String contentType) {
    return "application/json".equals(contentType) || contentType.contains("application/json") || contentType.contains("text") || contentType.contains("css") || contentType.contains("script") || contentType.contains("html") || contentType.contains("xml") || contentType.contains("yaml") || contentType.contains("yml");
  }

  public static <T> T clone(T t) {
    return KryoSerializers.clone(t);
  }

  public static String toJson(Object body) {
    if (body instanceof String) {
      return (String)body;
    } else {
      StringBuilder content = getBuilder();
      JsonMapper mapper = GsonSerializers.getJsonMapperFor(body);
      mapper.toJson(body, content);
      return content.toString();
    }
  }

  public static <T> T fromJson(String json, Class<T> clazz) {
    return GsonSerializers.getJsonMapperFor(clazz).fromJson(json, clazz);
  }

  public static <T> T fromJson(Object json, Class<T> clazz) {
    return GsonSerializers.getJsonMapperFor(clazz).fromJson(json, clazz);
  }

  public static boolean isValidationError(Throwable e) {
    return e instanceof IllegalArgumentException;
  }

  public static String toString(Throwable t) {
    if (t == null) {
      return null;
    } else {
      StringWriter writer = new StringWriter();
      PrintWriter printer = new PrintWriter(writer);
      Throwable var3 = null;

      try {
        t.printStackTrace(printer);
      } catch (Throwable var12) {
        var3 = var12;
        throw var12;
      } finally {
        if (printer != null) {
          if (var3 != null) {
            try {
              printer.close();
            } catch (Throwable var11) {
              var3.addSuppressed(var11);
            }
          } else {
            printer.close();
          }
        }

      }

      return writer.toString();
    }
  }

  public static long fromNowMicrosUtc(long deltaMicros) {
    return getSystemNowMicrosUtc() + deltaMicros;
  }

  public static void registerCustomJsonMapper(Class<?> clazz, JsonMapper mapper) {
    GsonSerializers.registerCustomJsonMapper(clazz, mapper);
  }

  public static byte[] compressToGzip(byte[] textAsBytes) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      GZIPOutputStream os = new GZIPOutputStream(baos);
      Throwable var3 = null;

      try {
        os.write(textAsBytes);
      } catch (Throwable var13) {
        var3 = var13;
        throw var13;
      } finally {
        if (os != null) {
          if (var3 != null) {
            try {
              os.close();
            } catch (Throwable var12) {
              var3.addSuppressed(var12);
            }
          } else {
            os.close();
          }
        }

      }
    } catch (IOException var15) {
      return null;
    }

    return baos.toByteArray();
  }

  static {
    DEFAULT_TIME_DRIFT_THRESHOLD_MICROS = TimeUnit.SECONDS.toMicros(1L);
  }
}
