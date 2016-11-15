package com.wavefront.agent.preprocessor;

import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

import static org.junit.Assert.fail;

public class AgentConfigurationTest {

  @Test
  public void testLoadInvalidRules() {
    AgentPreprocessorConfiguration config = new AgentPreprocessorConfiguration();
    try {
      InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules_invalid.yaml");
      config.loadFromStream(stream);
      fail("Invalid rules did not cause an exception");
    } catch (RuntimeException ex) {
      Assert.assertEquals(0, config.totalValidRules);
      Assert.assertEquals(100, config.totalInvalidRules);
    }
  }

  @Test
  public void testLoadValidRules() {
    AgentPreprocessorConfiguration config = new AgentPreprocessorConfiguration();
    InputStream stream = PreprocessorRulesTest.class.getResourceAsStream("preprocessor_rules.yaml");
    config.loadFromStream(stream);
    Assert.assertEquals(0, config.totalInvalidRules);
    Assert.assertEquals(28, config.totalValidRules);
  }
}
