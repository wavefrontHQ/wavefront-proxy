package com.wavefront.agent.preprocessor;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;

import wavefront.report.ReportPoint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class AgentConfigurationTest {

  @Test
  public void testLoadInvalidRules() {
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    try {
      InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_invalid.yaml");
      config.loadFromStream(stream);
      fail("Invalid rules did not cause an exception");
    } catch (RuntimeException ex) {
      Assert.assertEquals(0, config.totalValidRules);
      Assert.assertEquals(128, config.totalInvalidRules);
    }
  }

  @Test
  public void testLoadValidRules() {
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules.yaml");
    config.loadFromStream(stream);
    Assert.assertEquals(0, config.totalInvalidRules);
    Assert.assertEquals(49, config.totalValidRules);
  }

  @Test
  public void testPreprocessorRulesOrder() {
    // test that system rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_order_test.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager(null, stream,
        System::currentTimeMillis, 5000);
    config.getSystemPreprocessor("2878").forReportPoint().addTransformer(
        new ReportPointAddPrefixTransformer("fooFighters"));
    ReportPoint point = new ReportPoint("foometric", System.currentTimeMillis(), 10L, "host", "table", new HashMap<>());
    config.get("2878").get().forReportPoint().transform(point);
    assertEquals("barFighters.barmetric", point.getMetric());
  }
}
