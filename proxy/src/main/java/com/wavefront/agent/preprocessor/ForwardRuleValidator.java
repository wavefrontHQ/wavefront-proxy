package com.wavefront.agent.preprocessor;

import com.wavefront.api.agent.preprocessor.ReportLogForwardTransformer;
import com.wavefront.api.agent.preprocessor.ReportPointForwardTransformer;
import com.wavefront.api.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.api.agent.preprocessor.SpanForwardTransformer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Stateless utility that validates forward-rule targets for the central/default tenant's rule set,
 * ensuring every tenant name referenced in a forward/spanForward/logForward rule is registered in
 * {@link com.wavefront.agent.TokenManager}.
 *
 * <p>All methods are package-private and stateless; the class cannot be instantiated.
 */
final class ForwardRuleValidator {

  private ForwardRuleValidator() {
    throw new UnsupportedOperationException("ForwardRuleValidator is a stateless utility class");
  }

  /**
   * Validates that every tenant name referenced inside forward, spanForward, or logForward
   * transformers in {@code preprocessorsByPort} is contained in {@code validTenantNames}.
   *
   * <p>Two sets are accepted so the validation logic and the error-message display can show
   * different information:
   * <ul>
   *   <li>{@code validTenantNames} — the full acceptance set used for the boolean check.
   *       Includes the internal {@code "central"} key, the {@code defaultTenant} alias, and
   *       every multicastingTenant name.
   *   <li>{@code displayTenantNames} — shown in the error message. Should list only names
   *       that are meaningful to operators (e.g., multicastingTenant names), omitting internal
   *       aliases that refer to the same primary cluster.
   * </ul>
   *
   * @param preprocessorsByPort map from port handle to preprocessor (central-tenant rule set)
   * @param validTenantNames    full set of names accepted in forward rules
   * @param displayTenantNames  human-readable set shown in the error message
   * @throws IllegalArgumentException when an unknown tenant name is referenced by a forward rule
   */
  static void validateForwardRuleTargetTenants(
      Map<String, ReportableEntityPreprocessor> preprocessorsByPort,
      Set<String> validTenantNames,
      Set<String> displayTenantNames) {
    for (Map.Entry<String, ReportableEntityPreprocessor> portEntry : preprocessorsByPort.entrySet()) {
      String portHandle = portEntry.getKey();
      ReportableEntityPreprocessor portPreprocessor = portEntry.getValue();
      validateForwardTransformerTargets(
          portPreprocessor.forReportPoint().getTransformers(), portHandle,
          validTenantNames, displayTenantNames);
      validateForwardTransformerTargets(
          portPreprocessor.forSpan().getTransformers(), portHandle,
          validTenantNames, displayTenantNames);
      validateForwardTransformerTargets(
          portPreprocessor.forReportLog().getTransformers(), portHandle,
          validTenantNames, displayTenantNames);
    }
  }

  @SuppressWarnings("unchecked")
  private static <DataType> void validateForwardTransformerTargets(
      List<Function<DataType, DataType>> transformers,
      String portHandle,
      Set<String> validTenantNames,
      Set<String> displayTenantNames) {
    for (Function<DataType, DataType> transformer : transformers) {
      List<String> forwardTargetTenants = extractForwardTargetTenants(transformer);
      if (forwardTargetTenants != null) {
        for (String targetTenant : forwardTargetTenants) {
          String trimmedTargetTenant = targetTenant.trim();
          if (!validTenantNames.contains(trimmedTargetTenant)) {
            throw new IllegalArgumentException(
                "Forward rule for port "
                    + portHandle
                    + " references unknown tenant '"
                    + trimmedTargetTenant
                    + "'. Registered multicastingTenants: "
                    + displayTenantNames);
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static <DataType> List<String> extractForwardTargetTenants(
      Function<DataType, DataType> transformer) {
    if (transformer instanceof ReportPointForwardTransformer) {
      return ((ReportPointForwardTransformer) transformer).getTenantList();
    } else if (transformer instanceof SpanForwardTransformer) {
      return ((SpanForwardTransformer) transformer).getTenantList();
    } else if (transformer instanceof ReportLogForwardTransformer) {
      return ((ReportLogForwardTransformer) transformer).getTenantList();
    }
    return null;
  }
}
