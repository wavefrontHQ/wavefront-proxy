package com.wavefront.common;

import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

/**
 * @author vasily@wavefront.com
 */
public class MessageDedupingLoggerTest {

  @Test
  public void testLogger() {
    Logger mockLogger = EasyMock.createMock(Logger.class);
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    replay(mockLogger);
    Logger log = new MessageDedupingLogger(mockLogger, 1000, 0.1);
    reset(mockLogger);
    expect(mockLogger.getName()).andReturn("loggerName").anyTimes();
    Capture<LogRecord> logs = Capture.newInstance(CaptureType.ALL);
    mockLogger.log(capture(logs));
    expectLastCall().times(4);
    replay(mockLogger);
    log.severe("msg1");
    log.severe("msg1");
    log.warning("msg1");
    log.info("msg1");
    log.config("msg1");
    log.fine("msg1");
    log.finer("msg1");
    log.finest("msg1");
    log.warning("msg2");
    log.info("msg3");
    log.info("msg3");
    log.severe("msg4");
    log.info("msg3");
    log.info("msg3");
    log.severe("msg4");
    verify(mockLogger);
    assertEquals(4, logs.getValues().size());
    assertEquals("msg1", logs.getValues().get(0).getMessage());
    assertEquals(Level.SEVERE, logs.getValues().get(0).getLevel());
    assertEquals("msg2", logs.getValues().get(1).getMessage());
    assertEquals(Level.WARNING, logs.getValues().get(1).getLevel());
    assertEquals("msg3", logs.getValues().get(2).getMessage());
    assertEquals(Level.INFO, logs.getValues().get(2).getLevel());
    assertEquals("msg4", logs.getValues().get(3).getMessage());
    assertEquals(Level.SEVERE, logs.getValues().get(3).getLevel());
  }
}