package com.wavefront.agent.listeners;

import com.wavefront.common.logger.MessageDedupingLogger;
import com.yammer.metrics.core.Counter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Constants and utility methods for validating feature subscriptions.
 *
 * @author vasily@wavefront.com
 */
public abstract class FeatureCheckUtils {
  private static final Logger logger = Logger.getLogger(FeatureCheckUtils.class.getCanonicalName());

  private static final Logger featureDisabledLogger = new MessageDedupingLogger(logger, 3, 0.2);
  public static final String HISTO_DISABLED =
      "Ingested point discarded because histogram "
          + "feature has not been enabled for your account";
  public static final String SPAN_DISABLED =
      "Ingested span discarded because distributed "
          + "tracing feature has not been enabled for your account.";
  public static final String SPANLOGS_DISABLED =
      "Ingested span log discarded because "
          + "this feature has not been enabled for your account.";
  public static final String LOGS_DISABLED =
      "Ingested logs discarded because " + "this feature has not been enabled for your account.";

  public static final String LOGS_SERVER_DETAILS_MISSING =
      "Ingested logs discarded because "
          + "configuration is missing either "
          + "logServerIngestionToken/logServerIngestionURL or both.";

  /**
   * Check whether feature disabled flag is set, log a warning message, increment the counter by 1.
   *
   * @param featureDisabledFlag Supplier for feature disabled flag.
   * @param message Warning message to log if feature is disabled.
   * @param discardedCounter Optional counter for discarded items.
   * @return true if feature is disabled
   */
  public static boolean isFeatureDisabled(
      Supplier<Boolean> featureDisabledFlag, String message, @Nullable Counter discardedCounter) {
    return isFeatureDisabled(featureDisabledFlag, message, discardedCounter, null, null);
  }

  /**
   * Check whether feature disabled flag is set, log a warning message, increment the counter by 1.
   *
   * @param featureDisabledFlag Supplier for feature disabled flag.
   * @param message Warning message to log if feature is disabled.
   * @param discardedCounter Optional counter for discarded items.
   * @param output Optional stringbuilder for messages
   * @return true if feature is disabled
   */
  public static boolean isFeatureDisabled(
      Supplier<Boolean> featureDisabledFlag,
      String message,
      @Nullable Counter discardedCounter,
      @Nullable StringBuilder output) {
    return isFeatureDisabled(featureDisabledFlag, message, discardedCounter, output, null);
  }

  /**
   * Check whether feature disabled flag is set, log a warning message, increment the counter either
   * by 1 or by number of \n characters in request payload, if provided.
   *
   * @param featureDisabledFlag Supplier for feature disabled flag.
   * @param message Warning message to log if feature is disabled.
   * @param discardedCounter Optional counter for discarded items.
   * @param output Optional stringbuilder for messages
   * @param request Optional http request to use payload size
   * @return true if feature is disabled
   */
  public static boolean isFeatureDisabled(
      Supplier<Boolean> featureDisabledFlag,
      String message,
      @Nullable Counter discardedCounter,
      @Nullable StringBuilder output,
      @Nullable FullHttpRequest request) {
    return isFeatureDisabled(featureDisabledFlag, message, discardedCounter, 1, output, request);
  }

  /**
   * Check whether feature disabled flag is set, log a warning message, increment the counter by
   * increment.
   *
   * @param featureDisabledFlag Supplier for feature disabled flag.
   * @param message Warning message to log if feature is disabled.
   * @param discardedCounter Counter for discarded items.
   * @param increment The amount by which the counter will be increased.
   * @return true if feature is disabled
   */
  public static boolean isFeatureDisabled(
      Supplier<Boolean> featureDisabledFlag,
      String message,
      Counter discardedCounter,
      long increment) {
    return isFeatureDisabled(featureDisabledFlag, message, discardedCounter, increment, null, null);
  }

  /**
   * Check whether feature disabled flag is set, log a warning message, increment the counter either
   * by increment or by number of \n characters in request payload, if provided.
   *
   * @param featureDisabledFlag Supplier for feature disabled flag.
   * @param message Warning message to log if feature is disabled.
   * @param discardedCounter Optional counter for discarded items.
   * @param increment The amount by which the counter will be increased.
   * @param output Optional stringbuilder for messages
   * @param request Optional http request to use payload size
   * @return true if feature is disabled
   */
  public static boolean isFeatureDisabled(
      Supplier<Boolean> featureDisabledFlag,
      String message,
      @Nullable Counter discardedCounter,
      long increment,
      @Nullable StringBuilder output,
      @Nullable FullHttpRequest request) {
    if (featureDisabledFlag.get()) {
      featureDisabledLogger.warning(message);
      if (output != null) {
        output.append(message);
      }
      if (discardedCounter != null) {
        discardedCounter.inc(
            request == null
                ? increment
                : StringUtils.countMatches(request.content().toString(CharsetUtil.UTF_8), "\n")
                    + 1);
      }
      return true;
    }
    return false;
  }

  /**
   * Check whether log server details are missing for a converged CSP tenant.
   *
   * @param receivedLogServerDetails boolean that indicates availability of log server URL & token
   * @param enableHyperlogsConvergedCsp boolean that indicates converged CSP tenant setting
   * @param message Warning message to log if feature is disabled.
   * @param discardedCounter Optional counter for discarded items.
   * @return true if log server details are missing
   */
  public static boolean isMissingLogServerInfoForAConvergedCSPTenant(
      boolean receivedLogServerDetails,
      boolean enableHyperlogsConvergedCsp,
      String message,
      @Nullable Counter discardedCounter) {
    if (enableHyperlogsConvergedCsp && !receivedLogServerDetails) {
      featureDisabledLogger.warning(message);
      if (discardedCounter != null) {
        discardedCounter.inc();
      }
      return true;
    }
    return false;
  }
}
