package com.wavefront.common;

public class Pair<T, V> {
  public final T _1;
  public final V _2;

  public Pair(T t, V v) {
    this._1 = t;
    this._2 = v;
  }

  @Override
  public int hashCode() {
    return _1.hashCode() + 43 * _2.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Pair) {
      Pair pair = (Pair) obj;
      return _1.equals(pair._1) && _2.equals(pair._2);
    }
    return false;
  }

  public static <T, V> Pair<T, V> of(T t, V v) {
    return new Pair<T, V>(t, v);
  }
}
