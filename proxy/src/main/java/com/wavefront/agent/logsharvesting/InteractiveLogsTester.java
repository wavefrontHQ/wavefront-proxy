package com.wavefront.agent.logsharvesting;

import com.wavefront.agent.InteractiveTester;
import com.wavefront.agent.config.ConfigurationException;
import com.wavefront.agent.config.LogsIngestionConfig;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.ingester.ReportPointSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import wavefront.report.ReportPoint;

/**
 * @author Mori Bellamy (mori@wavefront.com)
 */
public class InteractiveLogsTester implements InteractiveTester {

  private final Supplier<LogsIngestionConfig> logsIngestionConfigSupplier;
  private final String prefix;
  private final Scanner stdin;

  public InteractiveLogsTester(Supplier<LogsIngestionConfig> logsIngestionConfigSupplier, String prefix) {
    this.logsIngestionConfigSupplier = logsIngestionConfigSupplier;
    this.prefix = prefix;
    stdin = new Scanner(System.in);
  }

  /**
   * Read one line of stdin and print a message to stdout.
   */
  @Override
  public boolean interactiveTest() throws ConfigurationException {
    final AtomicBoolean reported = new AtomicBoolean(false);

    ReportableEntityHandlerFactory factory = new ReportableEntityHandlerFactory() {
      @SuppressWarnings("unchecked")
      @Override
      public <T, U> ReportableEntityHandler<T, U> getHandler(HandlerKey handlerKey) {
        return (ReportableEntityHandler<T, U>) new ReportableEntityHandler<ReportPoint, String>() {
          @Override
          public void report(ReportPoint reportPoint) {
            reported.set(true);
            System.out.println(ReportPointSerializer.pointToString(reportPoint));
          }

          @Override
          public void block(ReportPoint reportPoint) {
            System.out.println("Blocked: " + reportPoint);
          }

          @Override
          public void block(@Nullable ReportPoint reportPoint, @Nullable String message) {
            System.out.println("Blocked: " + reportPoint);
          }

          @Override
          public void reject(@Nullable ReportPoint reportPoint, @Nullable String message) {
            System.out.println("Rejected: " + reportPoint);
          }

          @Override
          public void reject(@Nonnull String t, @Nullable String message) {
            System.out.println("Rejected: " + t);
          }

          @Override
          public void shutdown() {
          }
        };
      }

      @Override
      public void shutdown(@Nonnull String handle) {
      }
    };

    LogsIngester logsIngester = new LogsIngester(factory, logsIngestionConfigSupplier, prefix);

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
