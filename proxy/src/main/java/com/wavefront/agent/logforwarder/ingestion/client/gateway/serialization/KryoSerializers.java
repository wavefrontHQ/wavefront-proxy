package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.esotericsoftware.kryo.serializers.VersionFieldSerializer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 11:37 AM
 */
public final class KryoSerializers {
  private static final String PROPERTY_KRYO_HANDLE_BUILTIN_COLLECTIONS = "kryo.handleBuiltInCollections";
  private static boolean KRYO_HANDLE_BUILTIN_COLLECTIONS = true;
  private static final ThreadLocal<Kryo> kryoForObjectPerThread;
  private static final ThreadLocal<Kryo> kryoForDocumentPerThread;
  private static ThreadLocal<Kryo> kryoForObjectPerThreadCustom;
  private static ThreadLocal<Kryo> kryoForDocumentPerThreadCustom;
  public static final long THREAD_LOCAL_BUFFER_LIMIT_BYTES = 1048576L;
  private static final int DEFAULT_BUFFER_SIZE_BYTES = 4096;
  private static final BufferThreadLocal bufferPerThread;

  private KryoSerializers() {
  }

  public static Kryo create(boolean isObjectSerializer) {
    Kryo k = new Kryo();
    k.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    k.setDefaultSerializer(VersionFieldSerializer.class);
    k.addDefaultSerializer(ZonedDateTime.class, ZonedDateTimeSerializer.INSTANCE);
    k.addDefaultSerializer(Instant.class, InstantSerializer.INSTANCE);
    k.addDefaultSerializer(ZoneId.class, ZoneIdSerializer.INSTANCE);
    k.addDefaultSerializer(UUID.class, UUIDSerializer.INSTANCE);
    k.addDefaultSerializer(URI.class, URISerializer.INSTANCE);
    k.addDefaultSerializer(ByteBuffer.class, ByteBufferSerializer.INSTANCE);
    if (KRYO_HANDLE_BUILTIN_COLLECTIONS) {
      configureJdkCollections(k);
    }

    if (!isObjectSerializer) {
      k.setReferences(false);
      k.setCopyReferences(false);
    } else {
      k.setAutoReset(true);
    }

    return k;
  }

  private static void configureJdkCollections(Kryo kryo) {
    CollectionSerializer emptyOrSingletonSerializer = new CollectionSerializer() {
      protected Collection<?> create(Kryo kryo, Input input, Class<Collection> type) {
        return this.newCollection(type);
      }

      private Collection<?> newCollection(Class<Collection> origType) {
        if (NavigableSet.class.isAssignableFrom(origType)) {
          return new TreeSet();
        } else {
          return (Collection)(Set.class.isAssignableFrom(origType) ? new HashSet() : new ArrayList(1));
        }
      }

      protected Collection<?> createCopy(Kryo kryo, Collection original) {
        return this.newCollection((Class<Collection>) original.getClass());
      }
    };
    MapSerializer emptyOrSingletonMapSerializer = new MapSerializer() {
      protected Map<?, ?> create(Kryo kryo, Input input, Class<Map> type) {
        return this.newMap(type);
      }

      private Map<?, ?> newMap(Class<Map> origType) {
        return (Map)(NavigableMap.class.isAssignableFrom(origType) ? new TreeMap() : new HashMap());
      }

      protected Map<?, ?> createCopy(Kryo kryo, Map original) {
        return this.newMap((Class<Map>) original.getClass());
      }
    };
    kryo.addDefaultSerializer(Collections.EMPTY_LIST.getClass(), emptyOrSingletonSerializer);
    kryo.addDefaultSerializer(Collections.EMPTY_SET.getClass(), emptyOrSingletonSerializer);
    kryo.addDefaultSerializer(Collections.emptyNavigableSet().getClass(), emptyOrSingletonSerializer);
    kryo.addDefaultSerializer(Collections.EMPTY_MAP.getClass(), emptyOrSingletonMapSerializer);
    kryo.addDefaultSerializer(Collections.emptyNavigableMap().getClass(), emptyOrSingletonMapSerializer);
    kryo.addDefaultSerializer(Arrays.asList().getClass(), emptyOrSingletonSerializer);
    kryo.addDefaultSerializer(Collections.singleton(kryo).getClass(), emptyOrSingletonSerializer);
    kryo.addDefaultSerializer(Collections.singletonList(kryo).getClass(), emptyOrSingletonSerializer);
    kryo.addDefaultSerializer(Collections.singletonMap("", kryo).getClass(), emptyOrSingletonMapSerializer);
  }

  public static void register(ThreadLocal<Kryo> kryoThreadLocal, boolean isDocumentSerializer) {
    if (isDocumentSerializer) {
      kryoForDocumentPerThreadCustom = kryoThreadLocal;
    } else {
      kryoForObjectPerThreadCustom = kryoThreadLocal;
    }

  }

  private static Kryo getKryoThreadLocalForDocuments() {
    ThreadLocal<Kryo> tl = kryoForDocumentPerThreadCustom;
    if (tl == null) {
      tl = kryoForDocumentPerThread;
    }

    return (Kryo)tl.get();
  }

  private static Kryo getKryoThreadLocalForObjects() {
    ThreadLocal<Kryo> tl = kryoForObjectPerThreadCustom;
    if (tl == null) {
      tl = kryoForObjectPerThread;
    }

    return (Kryo)tl.get();
  }

  public static Output serializeAsDocument(Object o, int maxSize) {
    Kryo k = getKryoThreadLocalForDocuments();
    Output out = new Output(getBuffer(4096), maxSize);
    k.writeClassAndObject(out, o);
    return out;
  }

  public static Output serializeDocumentForIndexing(Object o, int maxSize) {
    Kryo k = getKryoThreadLocalForDocuments();
    byte[] buffer = getBuffer(4096);
    OutputWithRoot out = new OutputWithRoot(buffer, maxSize, o);
    k.writeClassAndObject(out, o);
    return out;
  }

  public static int serializeObject(Object o, byte[] buffer, int position) {
    Kryo k = getKryoThreadLocalForObjects();
    Output out = new Output(buffer, buffer.length);
    out.setPosition(position);
    k.writeClassAndObject(out, o);
    return out.position();
  }

  public static ByteBuffer serializeObject(Object o, int maxSize) {
    Kryo k = getKryoThreadLocalForObjects();
    Output out = new Output(4096, maxSize);
    k.writeClassAndObject(out, o);
    return ByteBuffer.wrap(out.getBuffer(), 0, out.position());
  }

  public static <T> T clone(T t) {
    Kryo k = getKryoThreadLocalForDocuments();
    return k.copy(t);
  }

  public static <T> T cloneObject(T t) {
    Kryo k = getKryoThreadLocalForObjects();
    T clone = k.copy(t);
    k.reset();
    return clone;
  }

  public static byte[] getBuffer(int capacity) {
    if ((long)capacity > 1048576L) {
      return new byte[capacity];
    } else {
      byte[] buffer = (byte[])bufferPerThread.get();
      if (buffer.length < capacity || buffer.length > capacity * 10) {
        buffer = new byte[capacity];
        bufferPerThread.set(buffer);
      }

      return buffer;
    }
  }

  public static Object deserializeObject(ByteBuffer bb) {
    return deserializeObject(bb.array(), bb.position(), bb.limit());
  }

  public static Object deserializeObject(byte[] bytes, int position, int length) {
    Kryo k = getKryoThreadLocalForObjects();
    Input in = new Input(bytes, position, length);
    return k.readClassAndObject(in);
  }

  public static Object deserializeDocument(byte[] bytes, int position, int length) {
    Kryo k = getKryoThreadLocalForDocuments();
    Input in = new Input(bytes, position, length);
    return k.readClassAndObject(in);
  }

  public static Object deserializeDocument(ByteBuffer bb) {
    return deserializeDocument(bb.array(), bb.position(), bb.limit());
  }

  static {
    String v = System.getProperty("kryo.handleBuiltInCollections");
    if (v != null) {
      KRYO_HANDLE_BUILTIN_COLLECTIONS = Boolean.valueOf(v);
    }

    kryoForObjectPerThread = new KryoSerializers.KryoForObjectThreadLocal();
    kryoForDocumentPerThread = new KryoSerializers.KryoForDocumentThreadLocal();
    bufferPerThread = new BufferThreadLocal();
  }

  public static class KryoForObjectThreadLocal extends ThreadLocal<Kryo> {
    public KryoForObjectThreadLocal() {
    }

    protected Kryo initialValue() {
      return KryoSerializers.create(true);
    }
  }

  public static class KryoForDocumentThreadLocal extends ThreadLocal<Kryo> {
    public KryoForDocumentThreadLocal() {
    }

    protected Kryo initialValue() {
      return KryoSerializers.create(false);
    }
  }
}