package com.wavefront.agent.listeners;

import com.google.common.annotations.VisibleForTesting;

import com.wavefront.agent.auth.TokenAuthenticator;
import com.wavefront.agent.logsharvesting.LogsIngester;
import com.wavefront.agent.logsharvesting.LogsMessage;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;

import org.apache.commons.lang.StringUtils;

import java.net.InetAddress;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;

import static com.wavefront.agent.channel.CachingHostnameLookupResolver.getRemoteAddress;

/**
 * Process incoming logs in raw plaintext format.
 *
 * @author vasily@wavefront.com
 */
public class RawLogsIngesterPortUnificationHandler extends PortUnificationHandler {
  private static final Logger logger = Logger.getLogger(RawLogsIngesterPortUnificationHandler.class.getCanonicalName());

  private final LogsIngester logsIngester;
  private final Function<InetAddress, String> hostnameResolver;
  private final ReportableEntityPreprocessor preprocessor;

  /**
   * Create new instance.
   *
   * @param handle              handle/port number.
   * @param ingester            log ingester.
   * @param hostnameResolver    rDNS lookup for remote clients ({@link InetAddress} to {@link String} resolver)
   * @param authenticator       {@link TokenAuthenticator} for incoming requests.
   * @param preprocessor        preprocessor.
   */
  public RawLogsIngesterPortUnificationHandler(String handle,
                                               @Nonnull LogsIngester ingester,
                                               @Nonnull Function<InetAddress, String> hostnameResolver,
                                               @Nonnull TokenAuthenticator authenticator,
                                               @Nullable ReportableEntityPreprocessor preprocessor) {
    super(authenticator, handle, true, true);
    this.logsIngester = ingester;
    this.hostnameResolver = hostnameResolver;
    this.preprocessor = preprocessor;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof TooLongFrameException) {
      logWarning("Received line is too long, consider increasing rawLogsMaxReceivedLength", cause, ctx);
      return;
    }
    if (cause instanceof DecoderException) {
      logger.log(Level.WARNING, "Unexpected exception in raw logs ingester", cause);
    }
    super.exceptionCaught(ctx, cause);
  }

  @VisibleForTesting
  @Override
  public void processLine(final ChannelHandlerContext ctx, String message) {
    if (message.isEmpty()) return;
    String processedMessage = preprocessor == null ?
        message :
        preprocessor.forPointLine().transform(message);
    if (preprocessor != null && !preprocessor.forPointLine().filter(message)) return;

    logsIngester.ingestLog(new LogsMessage() {
      @Override
      public String getLogLine() {
        return processedMessage;
      }

      @Override
      public String hostOrDefault(String fallbackHost) {
        String hostname = hostnameResolver.apply(getRemoteAddress(ctx));
        return StringUtils.isBlank(hostname) ? fallbackHost : hostname;
      }
    });
  }
}
