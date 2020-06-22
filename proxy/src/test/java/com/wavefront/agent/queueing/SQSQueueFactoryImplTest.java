package com.wavefront.agent.queueing;

import com.wavefront.agent.ProxyConfig;
import com.wavefront.agent.handlers.HandlerKey;
import com.wavefront.data.ReportableEntityType;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * @author mike@wavefront.com
 */
public class SQSQueueFactoryImplTest {
  @Test
  public void testQueueTemplate() {
    // we have to have all three
    assertTrue(SQSQueueFactoryImpl.isValidSQSTemplate("{{id}}{{entity}}{{port}}"));
    assertTrue(SQSQueueFactoryImpl.isValidSQSTemplate(new ProxyConfig().getSqsQueueNameTemplate()));

    // Typo or missing one (or all) of the three keys in some fashion
    assertFalse(SQSQueueFactoryImpl.isValidSQSTemplate("{{id}{{entity}}{{port}}"));
    assertFalse(SQSQueueFactoryImpl.isValidSQSTemplate("{{id}}{entity}}{{port}}"));
    assertFalse(SQSQueueFactoryImpl.isValidSQSTemplate("{{id}}{{entity}}{port}}"));
    assertFalse(SQSQueueFactoryImpl.isValidSQSTemplate(""));
    assertFalse(SQSQueueFactoryImpl.isValidSQSTemplate("{{id}}"));
    assertFalse(SQSQueueFactoryImpl.isValidSQSTemplate("{{entity}}"));
    assertFalse(SQSQueueFactoryImpl.isValidSQSTemplate("{{port}}"));
  }

  @Test
  public void testQueueNameGeneration() {
    SQSQueueFactoryImpl queueFactory = new SQSQueueFactoryImpl(
        new ProxyConfig().getSqsQueueNameTemplate(),"us-west-2","myid", false);
    assertEquals("wf-proxy-myid-points-2878",
        queueFactory.getQueueName(HandlerKey.of(ReportableEntityType.POINT, "2878")));
  }
}
