package com.wavefront.agent.preprocessor;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
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
      Assert.assertEquals(136, config.totalInvalidRules);
    }
  }

  @Test
  public void testLoadValidRules() {
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules.yaml");
    config.loadFromStream(stream);
    Assert.assertEquals(0, config.totalInvalidRules);
    Assert.assertEquals(57, config.totalValidRules);
  }

  @Test
  public void testPreprocessorRulesOrder() {
    // test that system rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_order_test.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    config.loadFromStream(stream);
    config.getSystemPreprocessor("2878").forReportPoint().addTransformer(
        new ReportPointAddPrefixTransformer("fooFighters"));
    ReportPoint point = new ReportPoint("foometric", System.currentTimeMillis(), 10L, "host", "table", new HashMap<>());
    config.get("2878").get().forReportPoint().transform(point);
    assertEquals("barFighters.barmetric", point.getMetric());
  }

  @Test
  public void testMultiPortPreprocessorRules() {
    // test that preprocessor rules kick in before user rules
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_multiport.yaml");
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    config.loadFromStream(stream);
    ReportPoint point = new ReportPoint("foometric", System.currentTimeMillis(), 10L, "host", "table", new HashMap<>());
    config.get("2879").get().forReportPoint().transform(point);
    assertEquals("bar1metric", point.getMetric());
    assertEquals(1, point.getAnnotations().size());
    assertEquals("multiTagVal", point.getAnnotations().get("multiPortTagKey"));

    ReportPoint point1 = new ReportPoint("foometric", System.currentTimeMillis(), 10L, "host",
        "table", new HashMap<>());
    config.get("1111").get().forReportPoint().transform(point1);
    assertEquals("foometric", point1.getMetric());
    assertEquals(1, point1.getAnnotations().size());
    assertEquals("multiTagVal", point1.getAnnotations().get("multiPortTagKey"));
  }

  @Test
  public void testEmptyRules() {
    InputStream stream = new ByteArrayInputStream("".getBytes());
    PreprocessorConfigManager config = new PreprocessorConfigManager();
    config.loadFromStream(stream);
  }
}
