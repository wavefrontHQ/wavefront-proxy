package com.wavefront.common;

import com.wavefront.data.ReportableEntityType;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author vasily@wavefront.com
 */
public class SamplingLoggerTest {

  @Test
  public void testAlwaysActiveLogger() {
    Logger mockLogger = EasyMock.createMock(Logger.class);
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    expect(mockLogger.isLoggable(anyObject())).andReturn(false).anyTimes();
    replay(mockLogger);
    Logger testLog1 = new SamplingLogger(ReportableEntityType.POINT, mockLogger, 1.0, true, null);
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    expect(mockLogger.isLoggable(anyObject())).andReturn(false).anyTimes();
    mockLogger.log(anyObject());
    expectLastCall().times(1000);
    replay(mockLogger);
    for (int i = 0; i < 1000; i++) {
      testLog1.info("test");
    }
    verify(mockLogger);
  }

  @Test
  public void test75PercentSamplingLogger() {
    Logger mockLogger = EasyMock.createMock(Logger.class);
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    expect(mockLogger.isLoggable(anyObject())).andReturn(false).anyTimes();
    replay(mockLogger);
    SamplingLogger testLog1 = new SamplingLogger(ReportableEntityType.POINT, mockLogger, 0.75,
        false, System.out::println);
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    expect(mockLogger.isLoggable(anyObject())).andReturn(false).anyTimes();
    replay(mockLogger);
    for (int i = 0; i < 1000; i++) {
      testLog1.info("test");
    }
    verify(mockLogger); // no calls should be made by default
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    expect(mockLogger.isLoggable(anyObject())).andReturn(true).anyTimes();
    mockLogger.log(anyObject());
    expectLastCall().times(700, 800);
    replay(mockLogger);
    testLog1.refreshLoggerState();
    for (int i = 0; i < 1000; i++) {
      testLog1.info("test");
    }
    verify(mockLogger); // approx ~750 calls should be made
  }

  @Test
  public void test25PercentSamplingThroughIsLoggable() {
    Logger mockLogger = EasyMock.createMock(Logger.class);
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    expect(mockLogger.isLoggable(anyObject())).andReturn(true).anyTimes();
    replay(mockLogger);
    SamplingLogger testLog1 = new SamplingLogger(ReportableEntityType.POINT, mockLogger, 0.25,
        false, null);
    int count = 0;
    for (int i = 0; i < 1000; i++) {
      if (testLog1.isLoggable(Level.FINEST)) count++;
    }
    assertTrue(count < 300);
    assertTrue(count > 200);
    count = 0;
    for (int i = 0; i < 1000; i++) {
      if (testLog1.isLoggable(Level.FINER)) count++;
    }
    assertEquals(1000, count);
  }
}
