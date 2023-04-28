package com.wavefront.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.*;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;

/**
 * A placeholder class for miscellaneous utility methods.
 *
 * @author vasily@wavefront.com
 */
public abstract class Utils {

  private static final ObjectMapper JSON_PARSER = new ObjectMapper();
  private static final ResourceBundle buildProps = ResourceBundle.getBundle("build");
  private static final List<Integer> UUID_SEGMENTS = ImmutableList.of(8, 4, 4, 4, 12);

  private static final Logger log = Logger.getLogger(Utils.class.getCanonicalName());

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
    if (uuid.length() != 32) {
      return uuid;
    }
    StringBuilder result = new StringBuilder();
    int startOffset = 0;
    for (Integer segmentLength : UUID_SEGMENTS) {
      result.append(uuid, startOffset, startOffset + segmentLength);
      if (result.length() < 36) {
        result.append('-');
      }
      startOffset += segmentLength;
    }
    return result.toString();
  }

  /**
   * Method converts a string Id to {@code UUID}. This Method specifically converts id's with less
   * than 32 digit hex characters into UUID format (See <a
   * href="http://www.ietf.org/rfc/rfc4122.txt"><i>RFC&nbsp;4122: A Universally Unique IDentifier
   * (UUID) URN Namespace</i></a>) by left padding id with Zeroes and adding hyphens. It assumes
   * that if the input id contains hyphens it is already an UUID. Please don't use this method to
   * validate/guarantee your id as an UUID.
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
    return inputString == null
        ? Collections.emptyList()
        : Splitter.on(",").omitEmptyStrings().trimResults().splitToList(inputString);
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
   * Attempts to retrieve build.package from system build properties.
   *
   * @return package as string
   */
  public static String getPackage() {
    try {
      return buildProps.getString("build.package");
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
    return System.getProperty("java.runtime.name", "(unknown runtime)")
        + " ("
        + System.getProperty("java.vendor", "")
        + ") "
        + System.getProperty("java.version", "(unknown version)");
  }

  /**
   * Makes a best-effort attempt to resolve the hostname for the local host.
   *
   * @return name for localhost
   */
  private static String detectedHostName;

  public static String getLocalHostName() {
    if (detectedHostName == null) {
      detectedHostName = detectLocalHostName();
    }
    return detectedHostName;
  }

  public static String detectLocalHostName() {
    for (String env : Arrays.asList("COMPUTERNAME", "HOSTNAME", "PROXY_HOSTNAME")) {
      String hostname = System.getenv(env);
      if (StringUtils.isNotBlank(hostname)) {
        log.info("Hostname: '" + hostname + "' (detected using '" + env + "' env variable)");
        return hostname;
      }
    }

    try {
      String hostname =
          new BufferedReader(
                  new InputStreamReader(Runtime.getRuntime().exec("hostname").getInputStream()))
              .readLine();
      if (StringUtils.isNotBlank(hostname)) {
        log.info("Hostname: '" + hostname + "' (detected using 'hostname' command)");
        return hostname;
      }
    } catch (IOException e) {
      log.fine("Error running 'hostname' command. " + e.getMessage());
    }

    InetAddress localAddress = null;
    try {
      Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
      while (nics.hasMoreElements()) {
        NetworkInterface network = nics.nextElement();
        if (!network.isUp() || network.isLoopback()) {
          continue;
        }
        for (Enumeration<InetAddress> addresses = network.getInetAddresses();
            addresses.hasMoreElements(); ) {
          InetAddress address = addresses.nextElement();
          if (address.isAnyLocalAddress()
              || address.isLoopbackAddress()
              || address.isMulticastAddress()) {
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
      String hostname = localAddress.getCanonicalHostName();
      log.info("Hostname: '" + hostname + "' (detected using network interfaces)");
      return hostname;
    }

    log.info("Hostname not detected, using 'localhost')");
    return "localhost";
  }

  /**
   * Check if the HTTP 407/408 response was actually received from Wavefront - if it's a JSON object
   * containing "code" key, with value equal to the HTTP response code, it's most likely from us.
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
    @JsonProperty String message;
    @JsonProperty int code;
  }
}
