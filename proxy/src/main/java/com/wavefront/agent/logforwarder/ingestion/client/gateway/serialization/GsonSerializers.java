package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.FNVHash;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 *  Infrastructure use only.
 *  Entrypoint to Gson customization/internals.
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:32 AM
 */
public final class GsonSerializers {
  private static final JsonMapper JSON = new JsonMapper();
  private static final ConcurrentMap<Class<?>, JsonMapper> CUSTOM_JSON = new ConcurrentSkipListMap<>(
      Comparator.comparingInt((Class<?> c) -> c.hashCode()).<String>thenComparing(Class::getName));

  private GsonSerializers() {
  }

  public static JsonMapper getJsonMapperFor(Object instance) {
    return instance == null ? JSON : getJsonMapperFor(instance.getClass());
  }

  public static JsonMapper getJsonMapperFor(Class<?> type) {
    if (type.isArray() && type != byte[].class) {
      type = type.getComponentType();
    }

    return (JsonMapper)CUSTOM_JSON.getOrDefault(type, JSON);
  }

  public static JsonMapper getJsonMapperFor(Type type) {
    if (type instanceof Class) {
      return getJsonMapperFor((Class)type);
    } else if (type instanceof ParameterizedType) {
      Type rawType = ((ParameterizedType)type).getRawType();
      return getJsonMapperFor(rawType);
    } else {
      return JSON;
    }
  }

  public static void registerCustomJsonMapper(Class<?> clazz, JsonMapper mapper) {
    CUSTOM_JSON.putIfAbsent(clazz, mapper);
  }

  public static long hashJson(Object body, long hash) {
    if (body instanceof String) {
      return FNVHash.compute((String)body, hash);
    } else {
      JsonMapper mapper = getJsonMapperFor(body);
      return mapper.hashJson(body, hash);
    }
  }
}