package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.PointHandler;
import com.wavefront.agent.PointHandlerImpl;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import sunnylabs.report.ReportPoint;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class InteractiveLogsTester {

  private final Supplier<LogsIngestionConfig> logsIngestionConfigSupplier;
  private final String prefix;
  private final Scanner stdin;

  public InteractiveLogsTester(Supplier<LogsIngestionConfig> logsIngestionConfigSupplier, String prefix)
      throws ConfigurationException {
    this.logsIngestionConfigSupplier = logsIngestionConfigSupplier;
    this.prefix = prefix;
    stdin = new Scanner(System.in);
  }

  /**
   * Read one line of stdin and print a message to stdout.
   */
  public boolean interactiveTest() throws ConfigurationException {
    final AtomicBoolean reported = new AtomicBoolean(false);

    LogsIngester logsIngester = new LogsIngester(
        new PointHandler() {
          @Override
          public void reportPoint(ReportPoint point, @Nullable String debugLine) {
            reported.set(true);
            System.out.println(PointHandlerImpl.pointToString(point));
          }

          @Override
          public void reportPoints(List<ReportPoint> points) {
            for (ReportPoint point : points) reportPoint(point, "");
          }

          @Override
          public void handleBlockedPoint(@Nullable String pointLine) {
            System.out.println("Blocked point: " + pointLine);
          }
        },
        logsIngestionConfigSupplier, prefix, System::currentTimeMillis);

    String line = stdin.nextLine();
    logsIngester.ingestLog(new LogsMessage() {
      @Override
      public String getLogLine() {
        return line;
      }

      @Override
      public String hostOrDefault(String fallbackHost) {
        try {
          return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
          return "localhost";
        }
      }
    });

    logsIngester.flush();
    if (!reported.get()) {
      System.out.println("Input matched no groks.");
    }

    return stdin.hasNext();
  }
}
