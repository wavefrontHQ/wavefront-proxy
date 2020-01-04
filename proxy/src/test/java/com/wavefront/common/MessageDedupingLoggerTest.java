package com.wavefront.common;

import org.easymock.EasyMock;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;

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
    mockLogger.log(Level.SEVERE, "msg1");
    expectLastCall().once();
    mockLogger.log(Level.WARNING, "msg2");
    expectLastCall().once();
    mockLogger.log(Level.INFO, "msg3");
    expectLastCall().once();
    mockLogger.log(Level.SEVERE, "msg4");
    expectLastCall().once();
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
  }
}