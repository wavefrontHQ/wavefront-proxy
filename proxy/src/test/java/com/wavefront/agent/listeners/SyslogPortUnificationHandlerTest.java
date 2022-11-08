package com.wavefront.agent.listeners;

import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.handlers.ReportableEntityHandlerFactory;
import com.wavefront.common.Clock;
import com.wavefront.ingester.SyslogDecoder;
import java.util.Arrays;
import java.util.Collections;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.ReportLog;

public class SyslogPortUnificationHandlerTest {
  private SyslogPortUnificationHandler subject;
  private ReportableEntityHandlerFactory factory;
  private ReportableEntityHandler<Object, Object> mockLogsHandler =
      EasyMock.createMock(ReportableEntityHandler.class);

  private static final String TRADITIONAL_FRAME_SYSLOG =
      "<44>1 2022-10-26T10:10:24+00:00 "
          + "some-hostname process-name proc-id message-id "
          + "[instance@12345 "
          + "key1=\"val1\" "
          + "key2=\"val2\" "
          + "key3=\"val3\"] "
          + "Some message";

  private static final String OCTET_COUNT_FRAME_SYSLOG =
      "129 <14>1 2021-12-24T22:20:01.438069+00:00 "
          + "another-hostname another-app-name ABC message-id "
          + "[tags@12345 "
          + "key1=\"val1\"] "
          + "Another message";

  @Before
  public void setup() {
    factory = EasyMock.createMock(ReportableEntityHandlerFactory.class);
    String port = "6514";
    EasyMock.expect(factory.getHandler(EasyMock.anyObject(), EasyMock.anyString()))
        .andReturn(mockLogsHandler);
    EasyMock.replay(factory);

    subject =
        new SyslogPortUnificationHandler(
            port,
            null,
            new SyslogDecoder(null, null, null, null, null, null, null, null),
            factory,
            null,
            null);
  }

  @After
  public void verify() {
    EasyMock.verify(mockLogsHandler);
  }

  @Test
  public void testTraditionalFrameTypeSyslogMessage() {
    ReportLog expected =
        new ReportLog(
            1666779024000L,
            "Some message",
            "some-hostname",
            Arrays.asList(
                new Annotation("syslog_host", "some-hostname"),
                new Annotation("key1", "val1"),
                new Annotation("key2", "val2"),
                new Annotation("key3", "val3")));
    mockLogsHandler.report(expected);
    EasyMock.expectLastCall();
    EasyMock.replay(mockLogsHandler);

    subject.handlePlainTextMessage(null, TRADITIONAL_FRAME_SYSLOG);
  }

  @Test
  public void testOctetCountFrameTypeSyslogMessage() {
    ReportLog expected =
        new ReportLog(
            1640384401438L,
            "Another message",
            "another-hostname",
            Arrays.asList(
                new Annotation("syslog_host", "another-hostname"), new Annotation("key1", "val1")));
    mockLogsHandler.report(expected);
    EasyMock.expectLastCall();
    EasyMock.replay(mockLogsHandler);

    subject.handlePlainTextMessage(null, OCTET_COUNT_FRAME_SYSLOG);
  }

  @Test
  public void testCustomTags() {
    factory = EasyMock.createMock(ReportableEntityHandlerFactory.class);
    String port = "6514";
    EasyMock.expect(factory.getHandler(EasyMock.anyObject(), EasyMock.anyString()))
        .andReturn(mockLogsHandler);
    EasyMock.replay(factory);

    subject =
        new SyslogPortUnificationHandler(
            port,
            null,
            new SyslogDecoder(
                () -> "",
                Collections.singletonList("customHost"),
                Collections.singletonList("customTimestamp"),
                Collections.singletonList("customMessage"),
                Collections.singletonList("customApplication"),
                Collections.singletonList("customService"),
                Collections.singletonList("customLevel"),
                Collections.singletonList("customException")),
            factory,
            null,
            null);

    long curTime = Clock.now();
    ReportLog expected =
        new ReportLog(
            // Will be the syslog timestamp due to the syslog parsing library not accepting null for
            // timestamps
            1640384401438L,
            // Will be the syslog message due to the syslog parsing library not accepting null for
            // messages
            "Base message",
            "custom host",
            Arrays.asList(
                new Annotation("syslog_host", "-"),
                // Unlike other custom tags, the custom host tag doesn't get removed when turning it
                // into the top level host field
                new Annotation("customHost", "custom host"),
                new Annotation("customTimestamp", Long.toString(curTime)),
                new Annotation("customMessage", "custom message"),
                new Annotation("application", "custom application"),
                new Annotation("service", "custom service"),
                new Annotation("log_level", "custom level"),
                new Annotation("error_name", "custom exception")));

    String customTagsSyslog =
        "<14>1 2021-12-24T22:20:01.438069+00:00 "
            + "- app-name ABC message-id "
            + "[tags@12345 "
            + "customHost=\"custom host\" "
            + "customTimestamp=\""
            + curTime
            + "\" "
            + "customMessage=\"custom message\" "
            + "customApplication=\"custom application\" "
            + "customService=\"custom service\" "
            + "customLevel=\"custom level\" "
            + "customException=\"custom exception\"] "
            + "Base message";

    mockLogsHandler.report(expected);
    EasyMock.expectLastCall();
    EasyMock.replay(mockLogsHandler);

    subject.handlePlainTextMessage(null, customTagsSyslog);
  }
}
