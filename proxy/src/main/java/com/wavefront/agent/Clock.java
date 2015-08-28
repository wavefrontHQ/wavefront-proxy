package com.wavefront.agent;

/**
 * Clock to manage agent time synced with the server.
 *
 * @author Clement Pang (clement@wavefront.com).
 */
public abstract class Clock {
  private static Long serverTime;
  private static Long localTime;

  public static void set(long serverTime) {
    localTime = System.currentTimeMillis();
    Clock.serverTime = serverTime;
  }

  public static long now() {
    if (serverTime == null) return System.currentTimeMillis();
    else return (System.currentTimeMillis() - localTime) + serverTime;
  }
}
