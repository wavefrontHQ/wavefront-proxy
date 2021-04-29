package com.wavefront.agent.preprocessor;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

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

  public ReportPointObfuscateTransformer(final String key,
                                         final String scope,
                                         final PreprocessorRuleMetrics ruleMetrics) {
    Preconditions.checkNotNull(key, "[key] can't be null");
    this.scope = Preconditions.checkNotNull(scope, "[scope] can't be null");
    Preconditions.checkArgument(!key.isEmpty(), "[key] can't be blank");
    Preconditions.checkArgument((key.length() == 16), "[key] have to be 16 characters");
    Preconditions.checkArgument(!scope.isEmpty(), "[scope] can't be blank");
    this.ruleMetrics = ruleMetrics;

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
          metric = encode(metric);
          reportPoint.setMetric(metric);
          ruleMetrics.incrementRuleAppliedCounter();
          break;

        case "sourceName":
        case "hostName":
          String source = reportPoint.getHost();
          source = encode(source);
          reportPoint.setHost(source);
          ruleMetrics.incrementRuleAppliedCounter();
          break;

        default:
          if (reportPoint.getAnnotations() != null) {
            String tagValue = reportPoint.getAnnotations().get(scope);
            if (tagValue != null) {
              tagValue = encode(tagValue);
              reportPoint.getAnnotations().put(scope, tagValue);
              ruleMetrics.incrementRuleAppliedCounter();
            }
          }
      }
    } catch (IllegalBlockSizeException | BadPaddingException | UnsupportedEncodingException e) {
      throw new RuntimeException("Error running Obfuscate rule on '" + scope + "' scope", e);
    } finally {
      ruleMetrics.ruleEnd(startNanos);
    }
    return reportPoint;
  }

  private String encode(@Nonnull String value) throws UnsupportedEncodingException, IllegalBlockSizeException, BadPaddingException {
    return Base64.getEncoder().encodeToString(
        cipher.doFinal(value.getBytes(StandardCharsets.UTF_8))
    );
  }
}
