package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import org.apache.commons.codec.binary.Hex;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import wavefront.report.ReportPoint;

/**
 * Obfuscate metric or tags using AES-ECB
 *
 * Created by German Laullon on 4/26/2021.
 */
public class ReportPointObfuscateTransformer implements Function<ReportPoint, ReportPoint> {

  private final String scope;
  private final PreprocessorRuleMetrics ruleMetrics;
  private final Cipher cipher;
  private final Pattern compiledPattern;

  public ReportPointObfuscateTransformer(final String key,
                                         final String scope,
                                         final String patternMatch,
                                         final PreprocessorRuleMetrics ruleMetrics) {
    Preconditions.checkNotNull(key, "[key] can't be null");
    Preconditions.checkArgument((key.length() == 16) || (key.length() == 32), "[key] have to be 16 " +
        "or 32 characters");
    Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    Preconditions.checkNotNull(patternMatch, "[match] can't be null");
    Preconditions.checkArgument(!patternMatch.isEmpty(), "[match] can't be blank");

    this.ruleMetrics = ruleMetrics;
    this.scope = scope;
    this.compiledPattern = Pattern.compile(patternMatch);

    try {
      byte[] aesKey = key.getBytes(StandardCharsets.UTF_8);
      SecretKeySpec secretKey = new SecretKeySpec(aesKey, "AES");
      cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
    } catch (NoSuchPaddingException | InvalidKeyException | NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Nullable
  @Override
  public ReportPoint apply(@Nullable ReportPoint reportPoint) {
    long startNanos = ruleMetrics.ruleStart();
    try {
      switch (scope) {
        case "metricName":
          String metric = reportPoint.getMetric();
          String encMetric = encode(metric);
          reportPoint.setMetric(encMetric);
          ruleMetrics.incrementRuleAppliedCounter();
          break;

        case "sourceName":
          String source = reportPoint.getHost();
          String encSource = encode(source);
          reportPoint.setHost(encSource);
          ruleMetrics.incrementRuleAppliedCounter();
          break;

        default:
          if (reportPoint.getAnnotations() != null) {
            String tagValue = reportPoint.getAnnotations().get(scope);
            if (tagValue != null) {
              String encTagValue = encode(tagValue);
              reportPoint.getAnnotations().put(scope, encTagValue);
              ruleMetrics.incrementRuleAppliedCounter();
            }
          }
      }
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new RuntimeException("Error running Obfuscate rule on '" + scope + "' scope", e);
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
    return reportPoint;
  }

  private String encode(@Nonnull String value) throws IllegalBlockSizeException, BadPaddingException {
    if (compiledPattern.matcher(value).matches()) {
      return Hex.encodeHexString(
          cipher.doFinal(value.getBytes(StandardCharsets.UTF_8))
      );
    }
    return value;
  }
}
