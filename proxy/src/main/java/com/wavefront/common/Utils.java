package com.wavefront.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A placeholder class for miscellaneous utility methods.
 *
 * @author vasily@wavefront.com
 */
public abstract class Utils {

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();
  private static final ResourceBundle buildProps = ResourceBundle.getBundle("build");
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
   * Requires an input uuid string Encoded as 32 hex characters. For example {@code
   * cced093a76eea418ffdc9bb9a6453df3}
   *
   * @param uuid string encoded as 32 hex characters.
   * @return uuid string encoded in 8-4-4-4-12 (rfc4122) format.
   */
  public static String addHyphensToUuid(String uuid) {
    Matcher matcherUuid = patternUuid.matcher(uuid);
    return matcherUuid.replaceAll("$1-$2-$3-$4-$5");
  }

  /**
   * Method converts a string Id to {@code UUID}. This Method specifically converts id's with less
   * than 32 digit hex characters into UUID format (See <a href="http://www.ietf.org/rfc/rfc4122.txt">
   * <i>RFC&nbsp;4122: A Universally Unique IDentifier (UUID) URN Namespace</i></a>) by left padding
   * id with Zeroes and adding hyphens. It assumes that if the input id contains hyphens it is
   * already an UUID. Please don't use this method to validate/guarantee your id as an UUID.
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

  /**
   * Creates an iterator over values a comma-delimited string.
   *
   * @param inputString input string
   * @return iterator
   */
  @Nonnull
  public static List<String> csvToList(@Nullable String inputString) {
    return inputString == null ?
        Collections.emptyList() :
        Splitter.on(",").omitEmptyStrings().trimResults().splitToList(inputString);
  }

  /**
   * Attempts to retrieve build.version from system build properties.
   *
   * @return build version as string
   */
  public static String getBuildVersion() {
    try {
      return buildProps.getString("build.version");
    } catch (MissingResourceException ex) {
      return "unknown";
    }
  }

  /**
   * Returns current java runtime version information (name/vendor/version) as a string.
   *
   * @return java runtime version as string
   */
  public static String getJavaVersion() {
    return System.getProperty("java.runtime.name", "(unknown runtime)") + " (" +
        System.getProperty("java.vendor", "") + ") " +
        System.getProperty("java.version", "(unknown version)");
  }

  /**
   * Makes a best-effort attempt to resolve the hostname for the local host.
   *
   * @return name for localhost
   */
  public static String getLocalHostName() {
    InetAddress localAddress = null;
    try {
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      while (nics.hasMoreElements()) {
        NetworkInterface network = nics.nextElement();
        if (!network.isUp() || network.isLoopback()) {
          continue;
        }
        for (Enumeration<InetAddress> addresses = network.getInetAddresses(); addresses.hasMoreElements(); ) {
          InetAddress address = addresses.nextElement();
          if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isMulticastAddress()) {
            continue;
          }
          if (address instanceof Inet4Address) { // prefer ipv4
            localAddress = address;
            break;
          }
          if (localAddress == null) {
            localAddress = address;
          }
        }
      }
    } catch (SocketException ex) {
      // ignore
    }
    if (localAddress != null) {
      return localAddress.getCanonicalHostName();
    }
    return "localhost";
  }

  /**
   * Check if the HTTP 407/408 response was actually received from Wavefront - if it's a
   * JSON object containing "code" key, with value equal to the HTTP response code,
   * it's most likely from us.
   *
   * @param response Response object.
   * @return whether we consider it a Wavefront response
   */
  public static boolean isWavefrontResponse(@Nonnull Response response) {
    try {
      Status res = JSON_PARSER.readValue(response.readEntity(String.class), Status.class);
      if (res.code == response.getStatus()) {
        return true;
      }
    } catch (Exception ex) {
      // ignore
    }
    return false;
  }

  /**
   * Use this to throw checked exceptions from iterator methods that do not declare that they throw
   * checked exceptions.
   */
  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public static <E extends Throwable> E throwAny(Throwable t) throws E {
    throw (E) t;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class Status {
    @JsonProperty
    String message;
    @JsonProperty
    int code;
  }
}