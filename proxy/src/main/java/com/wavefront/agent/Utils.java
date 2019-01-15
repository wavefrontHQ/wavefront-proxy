package com.wavefront.agent;

import org.apache.commons.lang.StringUtils;

import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * A placeholder class for miscellaneous utility methods.
 *
 * @author vasily@wavefront.com
 */
public abstract class Utils {

  private static final Pattern patternUuid = Pattern.compile(
      "(\\w{8})(\\w{4})(\\w{4})(\\w{4})(\\w{12})");

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

  /**
   * Requires an input uuid string Encoded as 32 hex characters.
   * For example {@code cced093a76eea418ffdc9bb9a6453df3}
   *
   * @param uuid string encoded as 32 hex characters.
   * @return uuid string encoded in 8-4-4-4-12 (rfc4122) format.
   */
  public static String addHyphensToUuid(String uuid) {
    Matcher matcherUuid = patternUuid.matcher(uuid);
    return matcherUuid.replaceAll("$1-$2-$3-$4-$5");
  }

  /**
   * Method converts a string Id to {@code UUID}.
   * This Method specifically converts id's with less than 32 digit hex characters into UUID
   * format (See <a href="http://www.ietf.org/rfc/rfc4122.txt"> <i>RFC&nbsp;4122: A
   * Universally Unique IDentifier (UUID) URN Namespace</i></a>) by left padding id with Zeroes
   * and adding hyphens. It assumes that if the input id contains hyphens it is already an UUID.
   * Please don't use this method to validate/guarantee your id as an UUID.
   *
   * @param id a string encoded in hex characters.
   * @return a UUID string.
   */
  @Nullable
  public static String convertToUuidString(@Nullable String id) {
    if (id == null || id.contains("-")) {
      return id;
    }
    return addHyphensToUuid(StringUtils.leftPad(id, 32, '0'));
  }
}
