package com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 10:59 AM
 */
public final class SortedKeysMapViewAdapterFactory implements TypeAdapterFactory {
  private static final TypeToken<Map<String, ?>> TYPE = new TypeToken<Map<String, ?>>() {
  };
  private static final Comparator<Map.Entry<?, ?>> COMP = (a, b) -> {
    return ((Comparable)a.getKey()).compareTo(b.getKey());
  };

  public SortedKeysMapViewAdapterFactory() {
  }

  public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
    if (!Map.class.isAssignableFrom(type.getRawType())) {
      return null;
    } else {
      final TypeAdapter<Map<String, ?>> orig = gson.getDelegateAdapter(this, TYPE);
      return new TypeAdapter<T>() {
        public void write(JsonWriter out, T value) throws IOException {
          if (value == null) {
            orig.write(out, (Map<String, ?>) null);
          } else {
            Map<String, ?> map = (Map)value;
            if (map.size() <= 1) {
              orig.write(out, map);
            } else {
              orig.write(out, new SortedKeysMapViewAdapterFactory.SortedKeysView(map));
            }

          }
        }

        public T read(JsonReader in) throws IOException {
          return (T) orig.read(in);
        }
      };
    }
  }

  static final class SortedKeysView<K, V> extends AbstractMap<K, V> {
    private final Map<K, V> map;

    SortedKeysView(Map<K, V> map) {
      this.map = map;
    }

    public Set<Entry<K, V>> entrySet() {
      try {
        SortedSet<Entry<K, V>> res = new TreeSet(SortedKeysMapViewAdapterFactory.COMP);
        Iterator var2 = this.map.entrySet().iterator();

        while(var2.hasNext()) {
          Entry<K, V> entry = (Entry)var2.next();
          res.add(new SimpleImmutableEntry(entry));
        }

        return res;
      } catch (ClassCastException var4) {
        return this.map.entrySet();
      }
    }
  }
}

