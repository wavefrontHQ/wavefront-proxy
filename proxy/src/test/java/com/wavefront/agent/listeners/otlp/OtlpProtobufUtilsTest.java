package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Pair;

import org.apache.commons.compress.utils.Lists;
import org.easymock.EasyMock;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import wavefront.report.Annotation;

import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.assertWFSpanEquals;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.parentSpanIdPair;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({OtlpProtobufUtilsTest.AnnotationAndAttributeTests.class,
    OtlpProtobufUtilsTest.TransformTests.class,
    OtlpProtobufUtilsTest.WasFilteredByPreprocessorTests.class})
public class OtlpProtobufUtilsTest {
  @NotNull
  private static Map<String, String> getWfAnnotationAsMap(List<Annotation> wfAnnotations) {
    Map<String, String> wfAnnotationAsMap = Maps.newHashMap();
    for (Annotation annotation : wfAnnotations) {
      wfAnnotationAsMap.put(annotation.getKey(), annotation.getValue());
    }
    return wfAnnotationAsMap;
  }

  public static class AnnotationAndAttributeTests {
    @Test
    public void testAttributesToWFAnnotations() {
      List<KeyValue> attributes = Lists.newArrayList();
      KeyValue emptyAttr = KeyValue.newBuilder().setKey("empty").build();
      KeyValue booleanAttr = KeyValue.newBuilder().setKey("a-boolean")
          .setValue(AnyValue.newBuilder().setBoolValue(true).build())
          .build();
      KeyValue stringAttr = KeyValue.newBuilder().setKey("a-string")
          .setValue(AnyValue.newBuilder().setStringValue("a-value").build())
          .build();
      KeyValue intAttr = KeyValue.newBuilder().setKey("a-int64")
          .setValue(AnyValue.newBuilder().setIntValue(Long.MAX_VALUE).build())
          .build();
      KeyValue doubleAttr = KeyValue.newBuilder().setKey("a-double")
          .setValue(AnyValue.newBuilder().setDoubleValue(2.1138).build())
          .build();
      attributes.add(emptyAttr);
      attributes.add(booleanAttr);
      attributes.add(stringAttr);
      attributes.add(intAttr);
      attributes.add(doubleAttr);

      List<Annotation> wfAnnotations = OtlpProtobufUtils.attributesToWFAnnotations(attributes);
      Map<String, String> wfAnnotationAsMap = getWfAnnotationAsMap(wfAnnotations);

      assertEquals(attributes.size(), wfAnnotationAsMap.size());
      assertEquals(wfAnnotationAsMap.get("empty"), "");
      assertEquals(wfAnnotationAsMap.get("a-boolean"), "true");
      assertEquals(wfAnnotationAsMap.get("a-string"), "a-value");
      assertEquals(wfAnnotationAsMap.get("a-int64"), Long.toString(Long.MAX_VALUE));
      assertEquals(wfAnnotationAsMap.get("a-double"), "2.1138");
    }

    @Test
    public void testRequiredTags() {
      List<Annotation> wfAnnotations = OtlpProtobufUtils.setRequiredTags(Collections.emptyList());
      Map<String, String> annotations = getWfAnnotationAsMap(wfAnnotations);

      assertEquals(4, wfAnnotations.size());
      assertFalse(annotations.containsKey(SERVICE_NAME.getKey()));
      assertEquals("defaultApplication", annotations.get(APPLICATION_TAG_KEY));
      assertEquals("defaultService", annotations.get(SERVICE_TAG_KEY));
      assertEquals(NULL_TAG_VAL, annotations.get(CLUSTER_TAG_KEY));
      assertEquals(NULL_TAG_VAL, annotations.get(SHARD_TAG_KEY));
    }

    @Test
    public void testSetRequiredTagsOtlpServiceNameTagIsUsed() {
      Annotation serviceName = Annotation.newBuilder().setKey(SERVICE_NAME.getKey())
          .setValue("a-service").build();

      List<Annotation> wfAnnotations =
          OtlpProtobufUtils.setRequiredTags(Collections.singletonList(serviceName));
      Map<String, String> annotations = getWfAnnotationAsMap(wfAnnotations);

      assertFalse(annotations.containsKey(SERVICE_NAME.getKey()));
      assertEquals("a-service", annotations.get(SERVICE_TAG_KEY));
    }

    @Test
    public void testSetRequireTagsOtlpServiceNameTagIsDroppedIfServiceIsSet() {
      Annotation serviceName = Annotation.newBuilder().setKey(SERVICE_NAME.getKey())
          .setValue("otlp-service").build();
      Annotation wfService = Annotation.newBuilder().setKey(SERVICE_TAG_KEY)
          .setValue("wf-service").build();

      List<Annotation> wfAnnotations =
          OtlpProtobufUtils.setRequiredTags(Arrays.asList(serviceName, wfService));
      Map<String, String> annotations = getWfAnnotationAsMap(wfAnnotations);

      assertFalse(annotations.containsKey(SERVICE_NAME.getKey()));
      assertEquals("wf-service", annotations.get(SERVICE_TAG_KEY));
    }
  }

  public static class TransformTests {
    @Test
    public void transformsMinimalSpan() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(null).build();

      wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList(), null);

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void transformHandlesZeroDuration() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().setEndTimeUnixNano(0).build();
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(null).setDuration(0).build();

      wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList(), null);

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void transformHandlesSpanAttributes() {
      Pair<ByteString, String> parentSpanIdPair = parentSpanIdPair();
      KeyValue booleanAttr = KeyValue.newBuilder().setKey("a-boolean")
          .setValue(AnyValue.newBuilder().setBoolValue(true).build())
          .build();
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().addAttributes(booleanAttr)
          .setParentSpanId(parentSpanIdPair._1).build();
      List<Annotation> wfAttrs = Arrays.asList(
          Annotation.newBuilder().setKey("parent").setValue(parentSpanIdPair._2).build(),
          Annotation.newBuilder().setKey("a-boolean").setValue("true").build()
      );
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(wfAttrs).build();

      wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList(), null);

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void transformAppliesPreprocessorRules() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
      List<Annotation> wfAttrs = Collections.singletonList(
          Annotation.newBuilder().setKey("my-key").setValue("my-value").build()
      );
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(wfAttrs)
          .build();

      wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList(),
          OtlpTestHelpers.addTagIfNotExistsPreprocessor(wfAttrs));

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void transformAppliesPreprocessorBeforeSettingRequiredTags() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
      List<Annotation> wfAttrs = Collections.singletonList(
          Annotation.newBuilder().setKey("application").setValue("an-app").build()
      );
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(wfAttrs)
          .build();

      wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList(),
          OtlpTestHelpers.addTagIfNotExistsPreprocessor(wfAttrs));

      assertWFSpanEquals(expected, actual);
    }
  }

  public static class WasFilteredByPreprocessorTests {
    private final ReportableEntityHandler<wavefront.report.Span, String> mockSpanHandler =
        MockReportableEntityHandlerFactory.getMockTraceHandler();
    private final wavefront.report.Span wfSpan = OtlpTestHelpers.wfSpanGenerator(null).build();

    @Before
    public void setup() {
      EasyMock.reset(mockSpanHandler);
    }

    @Test
    public void testNullPreprocessor() {
      ReportableEntityPreprocessor preprocessor = null;

      assertFalse(OtlpProtobufUtils.wasFilteredByPreprocessor(wfSpan, mockSpanHandler, preprocessor));
    }

    @Test
    public void testRejectWorks() {
      ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.rejectSpanPreprocessor();
      mockSpanHandler.reject(wfSpan, "span rejected for testing purpose");
      EasyMock.expectLastCall();
      EasyMock.replay(mockSpanHandler);

      assertTrue(OtlpProtobufUtils.wasFilteredByPreprocessor(wfSpan, mockSpanHandler,
          preprocessor));
      EasyMock.verify(mockSpanHandler);
    }

    @Test
    public void testBlockWorks() {
      ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.blockSpanPreprocessor();
      mockSpanHandler.block(wfSpan);
      EasyMock.expectLastCall();
      EasyMock.replay(mockSpanHandler);

      assertTrue(OtlpProtobufUtils.wasFilteredByPreprocessor(wfSpan, mockSpanHandler,
          preprocessor));
      EasyMock.verify(mockSpanHandler);
    }
  }
}