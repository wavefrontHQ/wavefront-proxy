package com.wavefront.agent.preprocessor;

import static com.wavefront.agent.ProxyContext.queuesManager;

import com.wavefront.agent.InteractiveTester;
import com.wavefront.agent.core.handlers.ReportableEntityHandler;
import com.wavefront.agent.core.handlers.ReportableEntityHandlerFactory;
import com.wavefront.agent.core.queues.QueueInfo;
import com.wavefront.agent.formatter.DataFormat;
import com.wavefront.agent.listeners.WavefrontPortUnificationHandler;
import com.wavefront.agent.listeners.tracing.SpanUtils;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.ingester.*;
import java.util.List;
import java.util.Scanner;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import wavefront.report.ReportPoint;
import wavefront.report.Span;

/**
 * Interactive tester for preprocessor rules.
 *
 * @author vasily@wavefront.com
 */
public class InteractivePreprocessorTester implements InteractiveTester {
  private static final SpanSerializer SPAN_SERIALIZER = new SpanSerializer();
  private static final ReportableEntityDecoder<String, Span> SPAN_DECODER =
      new SpanDecoder("unknown");

  private final Scanner stdin = new Scanner(System.in);
  private final Supplier<ReportableEntityPreprocessor> preprocessorSupplier;
  private final ReportableEntityType entityType;
  private final int port;
  private final List<String> customSourceTags;

  private final ReportableEntityHandlerFactory factory =
      new ReportableEntityHandlerFactory() {
        @Override
        public ReportableEntityHandler getHandler(String handler, QueueInfo queue) {
          if (queue.getEntityType() == ReportableEntityType.TRACE) {
            return new ReportableEntityHandler<Span>() {
              @Override
              public void report(Span reportSpan) {
                System.out.println(SPAN_SERIALIZER.apply(reportSpan));
              }

              @Override
              public void block(Span reportSpan) {
                System.out.println("Blocked: " + reportSpan);
              }

              @Override
              public void block(@Nullable Span reportSpan, @Nullable String message) {
                System.out.println("Blocked: " + SPAN_SERIALIZER.apply(reportSpan));
              }

              @Override
              public void reject(@Nullable Span reportSpan, @Nullable String message) {
                System.out.println("Rejected: " + SPAN_SERIALIZER.apply(reportSpan));
              }

              @Override
              public void reject(@Nonnull String t, @Nullable String message) {
                System.out.println("Rejected: " + t);
              }

              @Override
              public void shutdown() {}
            };
          }
          return new ReportableEntityHandler<ReportPoint>() {
            @Override
            public void report(ReportPoint reportPoint) {
              System.out.println(ReportPointSerializer.pointToString(reportPoint));
            }

            @Override
            public void block(ReportPoint reportPoint) {
              System.out.println("Blocked: " + ReportPointSerializer.pointToString(reportPoint));
            }

            @Override
            public void block(@Nullable ReportPoint reportPoint, @Nullable String message) {
              System.out.println("Blocked: " + ReportPointSerializer.pointToString(reportPoint));
            }

            @Override
            public void reject(@Nullable ReportPoint reportPoint, @Nullable String message) {
              System.out.println("Rejected: " + ReportPointSerializer.pointToString(reportPoint));
            }

            @Override
            public void reject(@Nonnull String t, @Nullable String message) {
              System.out.println("Rejected: " + t);
            }

            @Override
            public void shutdown() {}
          };
        }

        @Override
        public void shutdown(@Nonnull int handle) {}
      };

  /**
   * @param preprocessorSupplier supplier for {@link ReportableEntityPreprocessor}
   * @param entityType entity type (to determine whether it's a span or a point)
   * @param port handler key
   * @param customSourceTags list of custom source tags (for parsing)
   */
  public InteractivePreprocessorTester(
      Supplier<ReportableEntityPreprocessor> preprocessorSupplier,
      ReportableEntityType entityType,
      int port,
      List<String> customSourceTags) {
    this.preprocessorSupplier = preprocessorSupplier;
    this.entityType = entityType;
    this.port = port;
    this.customSourceTags = customSourceTags;
  }

  @Override
  public boolean interactiveTest() {
    String line = stdin.nextLine();
    if (entityType == ReportableEntityType.TRACE) {
      ReportableEntityHandler<Span> handler =
          factory.getHandler(port, queuesManager.initQueue(entityType));
      SpanUtils.preprocessAndHandleSpan(
          line, SPAN_DECODER, handler, handler::report, preprocessorSupplier, null, x -> true);
    } else {
      ReportableEntityHandler<ReportPoint> handler =
          factory.getHandler(port, queuesManager.initQueue(entityType));
      ReportableEntityDecoder<String, ReportPoint> decoder;
      if (DataFormat.autodetect(line) == DataFormat.HISTOGRAM) {
        decoder = new ReportPointDecoderWrapper(new HistogramDecoder());
      } else {
        decoder = new ReportPointDecoder(() -> "unknown", customSourceTags);
      }
      WavefrontPortUnificationHandler.preprocessAndHandlePoint(
          line, decoder, handler, preprocessorSupplier, null, "");
    }
    return stdin.hasNext();
  }
}
