package com.wavefront.agent.preprocessor;

import java.util.Map;
import javax.annotation.Nullable;

/** Utility class for methods used by preprocessors. */
public abstract class PreprocessorUtil {

  /**
   * Enforce string max length limit - either truncate or truncate with "..." at the end.
   *
   * @param input Input string to truncate.
   * @param maxLength Truncate string at this length.
   * @param actionSubtype TRUNCATE or TRUNCATE_WITH_ELLIPSIS.
   * @return truncated string
   */
  public static String truncate(String input, int maxLength, LengthLimitActionType actionSubtype) {
    switch (actionSubtype) {
      case TRUNCATE:
        return input.substring(0, maxLength);
      case TRUNCATE_WITH_ELLIPSIS:
        return input.substring(0, maxLength - 3) + "...";
      default:
        throw new IllegalArgumentException(actionSubtype + " action is not allowed!");
    }
  }

  @Nullable
  public static String getString(Map<String, Object> ruleMap, String key) {
    Object value = ruleMap.get(key);
    if (value == null) return null;
    if (value instanceof String) return (String) value;
    if (value instanceof Number) return String.valueOf(value);
    return (String) ruleMap.get(key);
  }

  public static boolean getBoolean(Map<String, Object> ruleMap, String key, boolean defaultValue) {
    Object value = ruleMap.get(key);
    if (value == null) return defaultValue;
    if (value instanceof Boolean) return (Boolean) value;
    if (value instanceof String) return Boolean.parseBoolean((String) value);
    throw new IllegalArgumentException();
  }

  public static int getInteger(Map<String, Object> ruleMap, String key, int defaultValue) {
    Object value = ruleMap.get(key);
    if (value == null) return defaultValue;
    if (value instanceof Number) return ((Number) value).intValue();
    if (value instanceof String) return Integer.parseInt((String) value);
    throw new IllegalArgumentException();
  }
}
