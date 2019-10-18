package com.wavefront.agent.histogram;

import com.squareup.tape.ObjectQueue;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import wavefront.report.ReportPoint;

/**
 * Dummy sender. Consumes TDigests from an ObjectQueue and sleeps for an amount of time to simulate sending
 *
 * @author Tim Schmidt (tim@wavefront.com).
 */
public class DroppingSender implements Runnable {
  private static final Logger logger = Logger.getLogger(DroppingSender.class.getCanonicalName());

  private final ObjectQueue<ReportPoint> input;
  private final Random r;

  public DroppingSender(ObjectQueue<ReportPoint> input) {
    this.input = input;
    r = new Random();
  }

  @Override
  public void run() {
    ReportPoint current;

    while ((current = input.peek()) != null) {
      input.remove();
      logger.log(Level.INFO, "Sent " + current);
    }

    try {
      Thread.sleep(100L + (long) (r.nextDouble() * 400D));
    } catch (InterruptedException e) {
      // eat
    }
  }
}
