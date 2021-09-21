package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.internal.Streams;
import com.google.gson.internal.bind.JsonTreeWriter;
import com.google.gson.stream.JsonWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.Date;
import java.util.EnumSet;
import java.util.function.Consumer;

/**
 * TODO Is there something special. Can a general JsonMapper be used?
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 10:55 AM
 */
public class JsonMapper {
  private static boolean JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS = false;
  private static boolean DISABLE_OBJECT_COLLECTION_AND_MAP_JSON_ADAPTERS = false;
  private static final int MAX_SERIALIZATION_ATTEMPTS = 100;
  private static final String JSON_INDENT = "  ";
  private final Gson compact;
  private Gson hashing;
  private boolean jsonSuppressGsonSerializationErrors;
  private static final Logger logger = LoggerFactory.getLogger(JsonMapper.class);

  public JsonMapper() {
    this(createDefaultGson(EnumSet.of(JsonMapper.JsonOptions.COMPACT)));
  }

  public JsonMapper(Consumer<GsonBuilder> gsonConfigCallback) {
    this(createCustomGson(EnumSet.of(JsonMapper.JsonOptions.COMPACT), gsonConfigCallback));
    this.hashing = this.createHashingGson(gsonConfigCallback);
  }

  public JsonMapper(Gson compact) {
    this.jsonSuppressGsonSerializationErrors = JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS;
    this.compact = compact;
    this.hashing = this.createHashingGson((Consumer) null);
  }

  private Gson createHashingGson(Consumer<GsonBuilder> gsonConfigCallback) {
    GsonBuilder bldr = new GsonBuilder();
    registerCommonGsonTypeAdapters(bldr);
    bldr.disableHtmlEscaping();
    bldr.registerTypeAdapterFactory(new SortedKeysMapViewAdapterFactory());
    if (gsonConfigCallback != null) {
      gsonConfigCallback.accept(bldr);
    }

    return bldr.create();
  }

  public String toJson(Object body) {
    int i = 1;

    while (true) {
      try {
        return this.compact.toJson(body);
      } catch (IllegalStateException var4) {
        this.handleIllegalStateException(var4, i);
        ++i;
      }
    }
  }

  public void toJson(Object body, Appendable appendable) {
    int i = 1;

    while (true) {
      try {
        this.compact.toJson(body, appendable);
        return;
      } catch (IllegalStateException var5) {
        this.handleIllegalStateException(var5, i);
        ++i;
      }
    }
  }

  public JsonElement toJsonElement(Object body) {
    if (body == null) {
      return null;
    } else {
      int i = 1;

      while (true) {
        try {
          JsonTreeWriter writer = new JsonTreeWriter();
          this.compact.toJson(body, body.getClass(), writer);
          return writer.get();
        } catch (IllegalStateException var4) {
          this.handleIllegalStateException(var4, i);
          ++i;
        }
      }
    }
  }

  public String toJsonHtml(Object body) {
    if (body == null) {
      return this.compact.toJson((JsonElement) null);
    } else {
      int i = 1;

      while (true) {
        try {
          StringBuilder appendable = new StringBuilder();
          this.toJsonHtml(body, appendable);
          return appendable.toString();
        } catch (IllegalStateException var4) {
          this.handleIllegalStateException(var4, i);
          ++i;
        }
      }
    }
  }

  private void handleIllegalStateException(IllegalStateException e, int i) {
    if (e.getMessage() == null) {
      logger.warn("Failure serializing body because of GSON race (attempt {})", new Object[]{i});
      if (i >= 100) {
        throw e;
      } else {
        try {
          Thread.sleep(0L, 1000 * i);
        } catch (InterruptedException var4) {
        }

      }
    } else {
      throw e;
    }
  }

  public <T> T fromJson(Object json, Class<T> clazz) {
    return clazz.isInstance(json) ? clazz.cast(json) : this.fromJson(json, (Type) clazz);
  }

  public <T> T fromJson(Object json, Type type) {
    try {
      return json instanceof JsonElement ? this.compact.fromJson((JsonElement) json, type) : this.compact.fromJson(json.toString(), type);
    } catch (RuntimeException var4) {
      if (this.jsonSuppressGsonSerializationErrors) {
        throw new JsonSyntaxException("JSON body could not be parsed");
      } else {
        throw var4;
      }
    }
  }

  public void setJsonSuppressGsonSerializationErrors(boolean suppressErrors) {
    this.jsonSuppressGsonSerializationErrors = suppressErrors;
  }

  private static Gson createDefaultGson(EnumSet<JsonMapper.JsonOptions> options) {
    return createDefaultGsonBuilder(options).create();
  }

  private static Gson createCustomGson(EnumSet<JsonMapper.JsonOptions> options, Consumer<GsonBuilder> gsonConfigCallback) {
    GsonBuilder bldr = createDefaultGsonBuilder(options);
    gsonConfigCallback.accept(bldr);
    return bldr.create();
  }

  public static GsonBuilder createDefaultGsonBuilder(EnumSet<JsonMapper.JsonOptions> options) {
    GsonBuilder bldr = new GsonBuilder();
    registerCommonGsonTypeAdapters(bldr);
    if (!options.contains(JsonMapper.JsonOptions.COMPACT)) {
      bldr.setPrettyPrinting();
    }

    bldr.disableHtmlEscaping();
    return bldr;
  }

  private static void registerCommonGsonTypeAdapters(GsonBuilder bldr) {
    if (!DISABLE_OBJECT_COLLECTION_AND_MAP_JSON_ADAPTERS) {
      bldr.registerTypeAdapter(ObjectCollectionTypeConverter.TYPE_LIST, ObjectCollectionTypeConverter.INSTANCE);
      bldr.registerTypeAdapter(ObjectCollectionTypeConverter.TYPE_SET, ObjectCollectionTypeConverter.INSTANCE);
      bldr.registerTypeAdapter(ObjectCollectionTypeConverter.TYPE_COLLECTION, ObjectCollectionTypeConverter.INSTANCE);
    }

    bldr.registerTypeAdapter(ObjectMapTypeConverter.TYPE, ObjectMapTypeConverter.INSTANCE);
    bldr.registerTypeAdapter(InstantConverter.TYPE, InstantConverter.INSTANCE);
    bldr.registerTypeAdapter(ZonedDateTimeConverter.TYPE, ZonedDateTimeConverter.INSTANCE);
    bldr.registerTypeHierarchyAdapter(byte[].class, new ByteArrayToBase64TypeAdapter());
    bldr.registerTypeHierarchyAdapter(Path.class, PathConverter.INSTANCE);
    bldr.registerTypeHierarchyAdapter(Date.class, UtcDateTypeAdapter.INSTANCE);
  }

  public void toJsonHtml(Object body, Appendable appendable) {
    if (body == null) {
      this.compact.toJson((JsonElement) null, appendable);
    } else {
      int i = 1;

      while (true) {
        try {
          JsonWriter jsonWriter = this.makePrettyJsonWriter(appendable);
          this.compact.toJson(body, body.getClass(), jsonWriter);
          return;
        } catch (IllegalStateException var5) {
          this.handleIllegalStateException(var5, i);
          ++i;
        }
      }
    }
  }

  private JsonWriter makePrettyJsonWriter(Appendable appendable) {
    JsonWriter jsonWriter = new JsonWriter(Streams.writerForAppendable(appendable));
    jsonWriter.setIndent("  ");
    return jsonWriter;
  }

  public long hashJson(Object body, long seed) {
    if (body == null) {
      return seed;
    } else {
      int i = 1;

      while (true) {
        try {
          HashingJsonWriter w = new HashingJsonWriter(seed);
          this.hashing.toJson(body, body.getClass(), w);
          return w.getHash();
        } catch (IllegalStateException var6) {
          this.handleIllegalStateException(var6, i);
          ++i;
        }
      }
    }
  }

  static {
    String v = System.getProperty("json.suppressGsonSerializationErrors");
    if (v != null) {
      JSON_SUPPRESS_GSON_SERIALIZATION_ERRORS = Boolean.valueOf(v);
    }

    v = System.getProperty("jsonMapper.disableObjectCollectionAndMapJsonAdapters");
    if (v != null) {
      DISABLE_OBJECT_COLLECTION_AND_MAP_JSON_ADAPTERS = Boolean.valueOf(v);
    }

  }

  public static enum JsonOptions {
    COMPACT;

    private JsonOptions() {
    }
  }
}
