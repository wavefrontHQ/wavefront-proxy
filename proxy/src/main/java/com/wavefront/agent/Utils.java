package com.wavefront.agent;

import java.util.function.Supplier;

/**
 * A placeholder class for miscellaneous utility methods.
 *
 * @author vasily@wavefront.com
 */
public abstract class Utils {

  /**
   * A lazy initialization wrapper for {@code Supplier}
   *
   * @param supplier {@code Supplier} to lazy-initialize
   * @return lazy wrapped supplier
   */
  public static <T> Supplier<T> lazySupplier(Supplier<T> supplier) {
    return new Supplier<T>() {
      private volatile T value = null;
      @Override
      public T get() {
        if (value == null) {
          synchronized (this) {
            if (value == null) {
              value = supplier.get();
            }
          }
        }
        return value;
      }
    };
  }
}
