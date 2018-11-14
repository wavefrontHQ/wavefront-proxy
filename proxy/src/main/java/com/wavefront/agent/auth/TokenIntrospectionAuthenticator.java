package com.wavefront.agent.auth;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * {@link TokenAuthenticator} that uses an external webservice for validating tokens.
 * Responses are cached and re-validated every {@code authResponseRefreshInterval} seconds; if the service is not
 * available, a cached last valid response may be used until {@code authResponseMaxTtl} expires.
 *
 * @author vasily@wavefront.com
 */
abstract class TokenIntrospectionAuthenticator implements TokenAuthenticator {
  private static final Logger logger = Logger.getLogger(TokenIntrospectionAuthenticator.class.getCanonicalName());

  private final long authResponseMaxTtlMillis;

  private volatile Long lastSuccessfulCallTs = null;

  private final Counter serviceCalls = Metrics.newCounter(new MetricName("auth", "", "api-calls"));
  private final Counter errorCount = Metrics.newCounter(new MetricName("auth", "", "api-errors"));

  private final LoadingCache<String, Boolean> tokenValidityCache;

  TokenIntrospectionAuthenticator(int authResponseRefreshInterval, int authResponseMaxTtl,
                                  @Nonnull Supplier<Long> timeSupplier) {
    this.authResponseMaxTtlMillis = TimeUnit.MILLISECONDS.convert(authResponseMaxTtl, TimeUnit.SECONDS);

    this.tokenValidityCache = Caffeine.newBuilder()
        .maximumSize(50_000)
        .refreshAfterWrite(Math.min(authResponseRefreshInterval, authResponseMaxTtl), TimeUnit.SECONDS)
        .ticker(() -> timeSupplier.get() * 1_000_000) // millisecond precision is fine
        .build(new CacheLoader<String, Boolean>() {
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
          public Boolean reload(@Nonnull String key,
                                @Nonnull Boolean oldValue) {
            serviceCalls.inc();
            boolean result;
            try {
              result = callAuthService(key);
              lastSuccessfulCallTs = timeSupplier.get();
            } catch (Exception e) {
              errorCount.inc();
              logger.log(Level.WARNING, "Error during Token Introspection Service call", e);
              if (lastSuccessfulCallTs != null &&
                  timeSupplier.get() - lastSuccessfulCallTs > authResponseMaxTtlMillis) {
                return null;
              }
              return oldValue;
            }
            return result;
          }
        });
  }

  abstract boolean callAuthService(@Nonnull String token) throws Exception;

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
