package com.google.common.util.concurrent;

import com.google.common.math.LongMath;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * An alternative RateLimiter implementation that allows to "return" unused permits back to the pool to handle retries
 * gracefully and allow precise control over outgoing rate, plus allows accumulating "credits" for unused permits over
 * a time window other than 1 second.
 *
 * Created by: vasily@wavefront.com, with portions from Guava library source code
 */
public class RecyclableRateLimiter extends RateLimiter {
  /**
   * The currently stored permits.
   */
  private double storedPermits;

  /**
   * The maximum number of stored permits.
   */
  private double maxPermits;

  /**
   * The interval between two unit requests, at our stable rate. E.g., a stable rate of 5 permits
   * per second has a stable interval of 200ms.
   */
  private double stableIntervalMicros;

  /**
   * The time when the next request (no matter its size) will be granted. After granting a
   * request, this is pushed further in the future. Large requests push this further than small
   * requests.
   */
  private long nextFreeTicketMicros = 0L; // could be either in the past or future

  /** The work (permits) of how many seconds can be saved up if this RateLimiter is unused? */
  private final double maxBurstSeconds;

  private final SleepingStopwatch stopwatch;

  private final Object mutex;

  public static RecyclableRateLimiter create(double permitsPerSecond, double maxBurstSeconds) {
    return new RecyclableRateLimiter(
        SleepingStopwatch.createFromSystemTimer(),
        permitsPerSecond,
        maxBurstSeconds);
  }

  private RecyclableRateLimiter(SleepingStopwatch stopwatch, double permitsPerSecond, double maxBurstSeconds) {
    super(stopwatch);
    this.mutex = new Object();
    this.stopwatch = stopwatch;
    this.maxBurstSeconds = maxBurstSeconds;
    this.setRate(permitsPerSecond);
  }

  /**
   * Get the number of accumulated permits
   *
   * @return number of accumulated permits
   */
  public double getAvailablePermits() {
    synchronized (mutex) {
      resync(stopwatch.readMicros());
      return storedPermits;
    }
  }

  /**
   * Return the specified number of permits back to the pool
   *
   * @param permits number of permits to return
   */
  public void recyclePermits(int permits) {
    synchronized (mutex) {
      long nowMicros = stopwatch.readMicros();
      resync(nowMicros);
      long surplusPermits = permits - (long) ((nextFreeTicketMicros - nowMicros) / stableIntervalMicros);
      long waitMicros = -min((long) (surplusPermits * stableIntervalMicros), 0L);
      try {
        this.nextFreeTicketMicros = LongMath.checkedAdd(nowMicros, waitMicros);
      } catch (ArithmeticException e) {
        this.nextFreeTicketMicros = Long.MAX_VALUE;
      }
      storedPermits = min(maxPermits, storedPermits + max(surplusPermits, 0L));
    }
  }

  @Override
  final void doSetRate(double permitsPerSecond, long nowMicros) {
    synchronized (mutex) {
      resync(nowMicros);
      this.stableIntervalMicros = SECONDS.toMicros(1L) / permitsPerSecond;
      double oldMaxPermits = this.maxPermits;
      maxPermits = maxBurstSeconds * permitsPerSecond;
      storedPermits = (oldMaxPermits == Double.POSITIVE_INFINITY)
          ? maxPermits
          : (oldMaxPermits == 0.0)
          ? 0.0 // initial state
          : storedPermits * maxPermits / oldMaxPermits;
    }
  }

  @Override
  final double doGetRate() {
    return SECONDS.toMicros(1L) / stableIntervalMicros;
  }

  @Override
  final long queryEarliestAvailable(long nowMicros) {
    synchronized (mutex) {
      return nextFreeTicketMicros;
    }
  }

  @Override
  final long reserveEarliestAvailable(int requiredPermits, long nowMicros) {
    synchronized (mutex) {
      resync(nowMicros);
      long returnValue = nextFreeTicketMicros;
      double storedPermitsToSpend = min(requiredPermits, this.storedPermits);
      double freshPermits = requiredPermits - storedPermitsToSpend;
      long waitMicros = (long) (freshPermits * stableIntervalMicros);

      try {
        this.nextFreeTicketMicros = LongMath.checkedAdd(nextFreeTicketMicros, waitMicros);
      } catch (ArithmeticException e) {
        this.nextFreeTicketMicros = Long.MAX_VALUE;
      }
      this.storedPermits -= storedPermitsToSpend;
      return returnValue;
    }
  }

  private void resync(long nowMicros) {
    // if nextFreeTicket is in the past, resync to now
    if (nowMicros > nextFreeTicketMicros) {
      storedPermits = min(maxPermits,
          storedPermits
              + (nowMicros - nextFreeTicketMicros) / stableIntervalMicros);
      nextFreeTicketMicros = nowMicros;
    }
  }
}
