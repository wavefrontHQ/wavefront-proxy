package com.wavefront.agent.logforwarder.ingestion.util;

import com.vmware.xenon.common.ColorLogFormatter;
import com.vmware.xenon.common.LogFormatter;
import com.vmware.xenon.common.Utils;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;

/**
 * TODO This class should go away once log forwarder properties are ported to WFProxyConfig
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/20/21 3:05 PM
 */
public final class CommandLineArgumentParser {
  private static final Logger LOGGER = Logger.getLogger(com.vmware.xenon.common.CommandLineArgumentParser.class
      .getSimpleName());
  private static final AtomicBoolean IS_LOGGING_CONFIGURED = new AtomicBoolean(false);

  public static final String PROPERTY_PREFIX = Utils.PROPERTY_NAME_PREFIX;

  public static final String ARGUMENT_PREFIX = "--";
  public static final String ARGUMENT_ASSIGNMENT = "=";

  private CommandLineArgumentParser() {

  }

  public static void parse(Object objectToBind, String[] args) {
    Map<String, String> argumentValuePairs;

    argumentValuePairs = parsePairsFromProperties();
    bindPairs(objectToBind, argumentValuePairs);
    argumentValuePairs = parsePairsFromArguments(args);
    bindPairs(objectToBind, argumentValuePairs);
  }

  public static void parseFromProperties(Object objectToBind) {
    Map<String, String> argumentValuePairs = parsePairsFromProperties();
    bindPairs(objectToBind, argumentValuePairs);
  }

  public static void parseFromArguments(Object objectToBind, String[] args) {
    Map<String, String> argumentValuePairs = parsePairsFromArguments(args);
    bindPairs(objectToBind, argumentValuePairs);
  }

  public static void bindPairs(Object objectToBind, Map<String, String> pairs) {
    configureLogging();

    Class<?> type = objectToBind.getClass();
    // match parsed arguments with annotated fields and set field values
    for (Map.Entry<String, String> parsedArgument : pairs.entrySet()) {
      try {
        Field field = type.getField(parsedArgument.getKey());
        if (field == null) {
          continue;
        }

        String v = parsedArgument.getValue();
        if (field.getType().equals(boolean.class) || field.getType().equals(Boolean.class)) {
          field.set(objectToBind, safeConvertToBoolean(v));
        } else if (field.getType().equals(int.class)
            || field.getType().equals(Integer.class)) {
          field.set(objectToBind, safeConvertToInteger(v));
        } else if (field.getType().equals(long.class) || field.getType().equals(Long.class)) {
          field.set(objectToBind, safeConvertToLong(v));
        } else if (field.getType().equals(double.class)
            || field.getType().equals(Double.class)) {
          field.set(objectToBind, safeConvertToDouble(v));
        } else if (field.getType().equals(Path.class)) {
          field.set(objectToBind, safeConvertToPath(v));
        } else if (field.getType().equals(String.class)) {
          if (v.equals("null")) {
            v = null;
          }
          field.set(objectToBind, v);
        } else if (field.getType().equals(String[].class)) {
          if (v.equals("null")) {
            v = null;
            field.set(objectToBind, null);
          } else {
            field.set(objectToBind, safeConvertToStringArray(v));
          }
        } else if (field.getType().isEnum()) {
          if (v.equals("null")) {
            v = null;
            field.set(objectToBind, null);
          } else {
            field.set(objectToBind, safeConvertToEnumValue(field.getType(), v));
          }
        } else {
          LOGGER.severe(String.format("Unsupported type %s for field %s", field.getType()
              .toString(), field.getName()));
        }
      } catch (NoSuchFieldException e) {
        LOGGER.fine(String.format("Field not present for arg %s in type %s",
            parsedArgument.getKey(),
            objectToBind.getClass().getSimpleName()));
      } catch (Exception e) {
        LOGGER.severe(String.format("Error setting field for arg %s:%s",
            parsedArgument.getValue(),
            e.toString()));
      }
    }
  }

  public static Map<String, String> parsePairsFromProperties() {
    HashMap<String, String> pairs = new HashMap<>();
    Properties properties = System.getProperties();
    for (String name : properties.stringPropertyNames()) {
      if (!name.startsWith(PROPERTY_PREFIX)) {
        continue;
      }

      String key = name.substring(PROPERTY_PREFIX.length());
      pairs.put(key, properties.getProperty(name));
    }
    return pairs;
  }

  public static Map<String, String> parsePairsFromArguments(String[] args) {
    HashMap<String, String> pairs = new HashMap<>();
    for (String arg : args) {
      if (!arg.startsWith(ARGUMENT_PREFIX)) {
        throw new IllegalArgumentException("Arguments must start with " + ARGUMENT_PREFIX);
      }

      int spaceIndex = arg.indexOf(ARGUMENT_ASSIGNMENT);
      if ((spaceIndex == -1) || (spaceIndex == (arg.length() - 1))) {
        throw new IllegalArgumentException(
            "Arguments must be followed by an equals sign and a value:" + arg);
      }

      String argName = arg.substring(ARGUMENT_PREFIX.length(), spaceIndex);
      String value = arg.substring(spaceIndex + 1, arg.length());
      pairs.put(argName, value);
    }
    return pairs;
  }

  public static Integer safeConvertToInteger(String argumentValue) {
    try {
      return Integer.decode(argumentValue);
    } catch (Exception e) {
      return null;
    }
  }

  public static Long safeConvertToLong(String argumentValue) {
    try {
      return Long.decode(argumentValue);
    } catch (Exception e) {
      return null;
    }
  }

  public static Double safeConvertToDouble(String argumentValue) {
    try {
      return Double.parseDouble(argumentValue);
    } catch (Exception e) {
      return null;
    }
  }

  public static Boolean safeConvertToBoolean(String argumentValue) {
    try {
      return Boolean.parseBoolean(argumentValue);
    } catch (Exception e) {
      return false;
    }
  }

  private static String[] safeConvertToStringArray(String value) {
    String[] segments = value.split(",");
    return segments;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Object safeConvertToEnumValue(Class<?> type, String v) {
    try {
      return Enum.valueOf((Class<? extends Enum>) type.asSubclass(Enum.class), v);
    } catch (Exception e) {
      return null;
    }
  }

  private static Path safeConvertToPath(String v) {
    return Paths.get(v);
  }

  private static void configureLogging() {
    if (IS_LOGGING_CONFIGURED.getAndSet(true) == false) {
      for (java.util.logging.Handler h : LOGGER.getParent().getHandlers()) {
        if (h instanceof ConsoleHandler) {
          h.setFormatter(new ColorLogFormatter());
        } else {
          h.setFormatter(new LogFormatter());
        }
      }
    }
  }
}
