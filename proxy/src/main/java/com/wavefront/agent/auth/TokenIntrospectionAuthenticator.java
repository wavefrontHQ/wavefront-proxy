package com.wavefront.agent.auth;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link TokenAuthenticator} that uses an external webservice for validating tokens. Responses are
 * cached and re-validated every {@code authResponseRefreshInterval} seconds; if the service is not
 * available, a cached last valid response may be used until {@code authResponseMaxTtl} expires.
 *
 * @author vasily@wavefront.com
 */
abstract class TokenIntrospectionAuthenticator implements TokenAuthenticator {
  private static final Logger logger =
      Logger.getLogger(TokenIntrospectionAuthenticator.class.getCanonicalName());

  private final long authResponseMaxTtlMillis;

  private volatile Long lastSuccessfulCallTs = null;

  private final Counter serviceCalls = Metrics.newCounter(new MetricName("auth", "", "api-calls"));
  private final Counter errorCount = Metrics.newCounter(new MetricName("auth", "", "api-errors"));

  private final LoadingCache<String, Boolean> tokenValidityCache;

  TokenIntrospectionAuthenticator(
      int authResponseRefreshInterval,
      int authResponseMaxTtl,
      @Nonnull Supplier<Long> timeSupplier) {
    this.authResponseMaxTtlMillis =
        TimeUnit.MILLISECONDS.convert(authResponseMaxTtl, TimeUnit.SECONDS);

    this.tokenValidityCache =
        Caffeine.newBuilder()
            .maximumSize(50_000)
            .refreshAfterWrite(
                Math.min(authResponseRefreshInterval, authResponseMaxTtl), TimeUnit.SECONDS)
            .ticker(() -> timeSupplier.get() * 1_000_000) // millisecond precision is fine
            .build(
                new CacheLoader<String, Boolean>() {
                  @Override
                  public Boolean load(@Nonnull String key) {
                    serviceCalls.inc();
                    boolean result;
                    try {
                      result = callAuthService(key);
                      lastSuccessfulCallTs = timeSupplier.get();
                    } catch (Exception e) {
                      errorCount.inc();
                      logger.log(Level.WARNING, "Error during Token Introspection Service call", e);
                      return null;
                    }
                    return result;
                  }

                  @Override
                  public Boolean reload(@Nonnull String key, @Nonnull Boolean oldValue) {
                    serviceCalls.inc();
                    boolean result;
                    try {
                      result = callAuthService(key);
                      lastSuccessfulCallTs = timeSupplier.get();
                    } catch (Exception e) {
                      errorCount.inc();
                      logger.log(Level.WARNING, "Error during Token Introspection Service call", e);
                      if (lastSuccessfulCallTs != null
                          && timeSupplier.get() - lastSuccessfulCallTs > authResponseMaxTtlMillis) {
                        return null;
                      }
                      return oldValue;
                    }
                    return result;
                  }
                });
  }

  abstract boolean callAuthService(@Nonnull String token) throws Exception;

  /**
   * Percent-encodes a token for safe substitution into a URL template (e.g. replacing a {@code
   * {{token}}} placeholder). Tokens are taken verbatim from inbound requests and may contain
   * characters that are meaningful in a URL (such as {@code / ? # & = %}); encoding them as an
   * opaque value prevents a malicious token from altering the target host, path, or query string
   * of the introspection request.
   */
  static String urlEncodeToken(@Nonnull String token) {
    try {
      // URLEncoder escapes everything except [A-Za-z0-9.\-_*], encoding space as '+'; normalize
      // '+' to '%20' since we're encoding a URL path/query component, not a form field.
      return URLEncoder.encode(token, "UTF-8").replace("+", "%20");
    } catch (UnsupportedEncodingException e) {
      // UTF-8 is guaranteed to be supported by every JVM.
      throw new AssertionError(e);
    }
  }

  @Override
  public boolean authorize(@Nullable String token) {
    if (token == null) {
      return false;
    }
    Boolean tokenResult = tokenValidityCache.get(token);
    return tokenResult == null ? false : tokenResult;
  }

  @Override
  public boolean authRequired() {
    return true;
  }
}
