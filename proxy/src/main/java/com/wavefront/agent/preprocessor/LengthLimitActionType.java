package com.wavefront.agent.preprocessor;

public enum LengthLimitActionType {
  DROP, TRUNCATE, TRUNCATE_WITH_ELLIPSIS;

  public static LengthLimitActionType fromString(String input) {
    for (LengthLimitActionType actionType : LengthLimitActionType.values()) {
      if (actionType.name().replace("_", "").equalsIgnoreCase(input)) {
        return actionType;
      }
    }
    throw new IllegalArgumentException(input + " is not a valid actionSubtype!");
  }
}
