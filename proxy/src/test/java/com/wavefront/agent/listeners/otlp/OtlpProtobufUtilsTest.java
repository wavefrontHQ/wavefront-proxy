package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.common.Pair;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import wavefront.report.Annotation;

import static com.wavefront.agent.listeners.otlp.OtlpProtobufUtils.OTEL_STATUS_DESCRIPTION_KEY;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.assertWFSpanEquals;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.otlpAttribute;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.parentSpanIdPair;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({OtlpProtobufUtilsTest.AnnotationAndAttributeTests.class,
    OtlpProtobufUtilsTest.SourceFromAttributesTests.class,
    OtlpProtobufUtilsTest.TransformTests.class,
    OtlpProtobufUtilsTest.WasFilteredByPreprocessorTests.class})
public class OtlpProtobufUtilsTest {

  private final static List<KeyValue> emptyAttrs = Collections.unmodifiableList(new ArrayList<>());

  private static Map<String, String> getWfAnnotationAsMap(List<Annotation> wfAnnotations) {
    Map<String, String> wfAnnotationAsMap = Maps.newHashMap();
    for (Annotation annotation : wfAnnotations) {
      wfAnnotationAsMap.put(annotation.getKey(), annotation.getValue());
    }
    return wfAnnotationAsMap;
  }

  public static class AnnotationAndAttributeTests {
    @Test
    public void testAnnotationsFromSimpleAttributes() {
      KeyValue emptyAttr = KeyValue.newBuilder().setKey("empty").build();
      KeyValue booleanAttr = KeyValue.newBuilder().setKey("a-boolean")
          .setValue(AnyValue.newBuilder().setBoolValue(true).build()).build();
      KeyValue stringAttr = KeyValue.newBuilder().setKey("a-string")
          .setValue(AnyValue.newBuilder().setStringValue("a-value").build()).build();
      KeyValue intAttr = KeyValue.newBuilder().setKey("a-int")
          .setValue(AnyValue.newBuilder().setIntValue(1234).build()).build();
      KeyValue doubleAttr = KeyValue.newBuilder().setKey("a-double")
          .setValue(AnyValue.newBuilder().setDoubleValue(2.1138).build()).build();
      KeyValue bytesAttr = KeyValue.newBuilder().setKey("a-bytes")
          .setValue(AnyValue.newBuilder().setBytesValue(
              ByteString.copyFromUtf8("any + old & data")).build())
          .build();
      KeyValue noValueAttr = KeyValue.newBuilder().setKey("no-value")
          .setValue(AnyValue.newBuilder().build()).build();

      List<KeyValue> attributes = Arrays.asList(emptyAttr, booleanAttr, stringAttr, intAttr,
          doubleAttr, noValueAttr, bytesAttr);

      List<Annotation> wfAnnotations = OtlpProtobufUtils.annotationsFromAttributes(attributes);
      Map<String, String> wfAnnotationAsMap = getWfAnnotationAsMap(wfAnnotations);

      assertEquals(attributes.size(), wfAnnotationAsMap.size());
      assertEquals("", wfAnnotationAsMap.get("empty"));
      assertEquals("true", wfAnnotationAsMap.get("a-boolean"));
      assertEquals("a-value", wfAnnotationAsMap.get("a-string"));
      assertEquals("1234", wfAnnotationAsMap.get("a-int"));
      assertEquals("2.1138", wfAnnotationAsMap.get("a-double"));
      assertEquals("YW55ICsgb2xkICYgZGF0YQ==", wfAnnotationAsMap.get("a-bytes"));
      assertEquals("<Unknown OpenTelemetry attribute value type VALUE_NOT_SET>",
          wfAnnotationAsMap.get("no-value"));
    }

    @Test
    public void testAnnotationsFromArrayAttributes() {
      KeyValue intArrayAttr = KeyValue.newBuilder().setKey("int-array")
          .setValue(
              AnyValue.newBuilder().setArrayValue(
                  ArrayValue.newBuilder()
                      .addAllValues(Arrays.asList(
                              AnyValue.newBuilder().setIntValue(-1).build(),
                              AnyValue.newBuilder().setIntValue(0).build(),
                              AnyValue.newBuilder().setIntValue(1).build()
                          )
                      ).build()
              ).build()
          ).build();

      KeyValue boolArrayAttr = KeyValue.newBuilder().setKey("bool-array")
          .setValue(
              AnyValue.newBuilder().setArrayValue(
                  ArrayValue.newBuilder()
                      .addAllValues(Arrays.asList(
                              AnyValue.newBuilder().setBoolValue(true).build(),
                              AnyValue.newBuilder().setBoolValue(false).build(),
                              AnyValue.newBuilder().setBoolValue(true).build()
                          )
                      ).build()
              ).build()
          ).build();

      KeyValue dblArrayAttr = KeyValue.newBuilder().setKey("dbl-array")
          .setValue(
              AnyValue.newBuilder().setArrayValue(
                  ArrayValue.newBuilder()
                      .addAllValues(Arrays.asList(
                              AnyValue.newBuilder().setDoubleValue(-3.14).build(),
                              AnyValue.newBuilder().setDoubleValue(0.0).build(),
                              AnyValue.newBuilder().setDoubleValue(3.14).build()
                          )
                      ).build()
              ).build()
          ).build();

      List<KeyValue> attributes = Arrays.asList(intArrayAttr, boolArrayAttr, dblArrayAttr);

      List<Annotation> wfAnnotations = OtlpProtobufUtils.annotationsFromAttributes(attributes);
      Map<String, String> wfAnnotationAsMap = getWfAnnotationAsMap(wfAnnotations);

      assertEquals("[-1, 0, 1]", wfAnnotationAsMap.get("int-array"));
      assertEquals("[true, false, true]", wfAnnotationAsMap.get("bool-array"));
      assertEquals("[-3.14, 0.0, 3.14]", wfAnnotationAsMap.get("dbl-array"));
    }

    @Test
    public void handlesSpecialCaseAnnotations() {
      /*
       A `source` tag at the span-level will override an explicit source that is set via
       `wfSpanBuilder.setSource(...)`, which arguably seems like a bug. Since we determine the WF
       source in `sourceAndResourceAttrs()`, rename any remaining OTLP Attribute to `_source`.
       */
      List<KeyValue> attrs = Collections.singletonList(otlpAttribute("source", "a-source"));

      List<Annotation> actual = OtlpProtobufUtils.annotationsFromAttributes(attrs);

      assertThat(actual, hasItem(new Annotation("_source", "a-source")));
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

    @Test
    public void testSetRequiredTagsDeduplicatesAnnotations() {
      Annotation.Builder dupeBuilder = Annotation.newBuilder().setKey("shared-key");
      Annotation first = dupeBuilder.setValue("first").build();
      Annotation middle = dupeBuilder.setValue("middle").build();
      Annotation last = dupeBuilder.setValue("last").build();
      List<Annotation> duplicates = Arrays.asList(first, middle, last);

      List<Annotation> actual = OtlpProtobufUtils.setRequiredTags(duplicates);

      // We care that the last item "wins" and is preserved when de-duping
      assertThat(actual, hasItem(last));
      assertThat(actual, not(hasItems(first, middle)));
    }
  }

  public static class TransformTests {

    private wavefront.report.Span actual;

    @Before
    public void setup() {
      actual = null;
    }

    @Test
    public void transformsMinimalSpan() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(null).build();

      actual = OtlpProtobufUtils.transform(otlpSpan, emptyAttrs, null, "test-source");

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void transformHandlesZeroDuration() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().setEndTimeUnixNano(0).build();
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(null).setDuration(0).build();

      actual = OtlpProtobufUtils.transform(otlpSpan, emptyAttrs, null, "test-source");

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

      actual = OtlpProtobufUtils.transform(otlpSpan, emptyAttrs, null, "test-source");

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void convertsResourceAttributesToAnnotations() {
      List<KeyValue> resourceAttrs = Collections.singletonList(otlpAttribute("r-key", "r-value"));
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(
          Collections.singletonList(new Annotation("r-key", "r-value"))
      ).build();

      actual = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanGenerator().build(), resourceAttrs, null, "test-source"
      );

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void spanAttributesHaveHigherPrecedenceThanResourceAttributes() {
      String key = "the-key";
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator()
          .addAttributes(otlpAttribute(key, "span-value")).build();
      List<KeyValue> resourceAttrs = Collections.singletonList(otlpAttribute(key, "rsrc-value"));

      actual = OtlpProtobufUtils.transform(otlpSpan, resourceAttrs, null, "test-source");

      assertThat(actual.getAnnotations(), not(hasItem(new Annotation(key, "rsrc-value"))));
      assertThat(actual.getAnnotations(), hasItem(new Annotation(key, "span-value")));
    }


    @Test
    public void transformAppliesPreprocessorRules() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
      List<Annotation> wfAttrs = Collections.singletonList(
          Annotation.newBuilder().setKey("my-key").setValue("my-value").build()
      );
      ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.addTagIfNotExistsPreprocessor(wfAttrs);
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(wfAttrs).build();

      actual = OtlpProtobufUtils.transform(otlpSpan, emptyAttrs, preprocessor, "test-source");

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void transformAppliesPreprocessorBeforeSettingRequiredTags() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
      List<Annotation> wfAttrs = Collections.singletonList(
          Annotation.newBuilder().setKey(APPLICATION_TAG_KEY).setValue("an-app").build()
      );
      ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.addTagIfNotExistsPreprocessor(wfAttrs);
      wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(wfAttrs).build();

      actual = OtlpProtobufUtils.transform(otlpSpan, emptyAttrs, preprocessor, "test-source");

      assertWFSpanEquals(expected, actual);
    }

    @Test
    public void transformTranslatesSpanKindToAnnotation() {
      wavefront.report.Span clientSpan = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_CLIENT),
          emptyAttrs, null, "test-source");
      assertThat(clientSpan.getAnnotations(), hasItem(new Annotation("span.kind", "client")));

      wavefront.report.Span consumerSpan = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_CONSUMER),
          emptyAttrs, null, "test-source");
      assertThat(consumerSpan.getAnnotations(), hasItem(new Annotation("span.kind", "consumer")));

      wavefront.report.Span internalSpan = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_INTERNAL),
          emptyAttrs, null, "test-source");
      assertThat(internalSpan.getAnnotations(), hasItem(new Annotation("span.kind", "internal")));

      wavefront.report.Span producerSpan = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_PRODUCER),
          emptyAttrs, null, "test-source");
      assertThat(producerSpan.getAnnotations(), hasItem(new Annotation("span.kind", "producer")));

      wavefront.report.Span serverSpan = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_SERVER),
          emptyAttrs, null, "test-source");
      assertThat(serverSpan.getAnnotations(), hasItem(new Annotation("span.kind", "server")));

      wavefront.report.Span unspecifiedSpan = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_UNSPECIFIED),
          emptyAttrs, null, "test-source");
      assertThat(unspecifiedSpan.getAnnotations(),
          hasItem(new Annotation("span.kind", "unspecified")));

      wavefront.report.Span noKindSpan = OtlpProtobufUtils.transform(
          OtlpTestHelpers.otlpSpanGenerator().build(),
          emptyAttrs, null, "test-source");
      assertThat(noKindSpan.getAnnotations(),
          hasItem(new Annotation("span.kind", "unspecified")));
    }

    @Test
    public void handlesSpanStatusIfError() {
      // Error Status without Message
      Span errorSpan = OtlpTestHelpers.otlpSpanWithStatus(Status.StatusCode.STATUS_CODE_ERROR, "");

      actual = OtlpProtobufUtils.transform(errorSpan, emptyAttrs, null, "test-source");

      assertThat(actual.getAnnotations(), hasItem(new Annotation(ERROR_TAG_KEY, ERROR_SPAN_TAG_VAL)));
      assertFalse(actual.getAnnotations().stream()
          .anyMatch(annotation -> annotation.getKey().equals(OTEL_STATUS_DESCRIPTION_KEY)));

      // Error Status with Message
      Span errorSpanWithMessage = OtlpTestHelpers.otlpSpanWithStatus(
          Status.StatusCode.STATUS_CODE_ERROR, "a description");

      actual = OtlpProtobufUtils.transform(errorSpanWithMessage, emptyAttrs, null, "test-source");

      assertThat(actual.getAnnotations(), hasItem(new Annotation(ERROR_TAG_KEY, ERROR_SPAN_TAG_VAL)));
      assertThat(actual.getAnnotations(),
          hasItem(new Annotation(OTEL_STATUS_DESCRIPTION_KEY, "a description")));
    }

    @Test
    public void ignoresSpanStatusIfNotError() {
      // Ok Status
      Span okSpan = OtlpTestHelpers.otlpSpanWithStatus(Status.StatusCode.STATUS_CODE_OK, "");

      actual = OtlpProtobufUtils.transform(okSpan, emptyAttrs, null, "test-source");

      assertFalse(actual.getAnnotations().stream()
          .anyMatch(annotation -> annotation.getKey().equals(ERROR_TAG_KEY)));
      assertFalse(actual.getAnnotations().stream()
          .anyMatch(annotation -> annotation.getKey().equals(OTEL_STATUS_DESCRIPTION_KEY)));

      // Unset Status
      Span unsetSpan = OtlpTestHelpers.otlpSpanWithStatus(Status.StatusCode.STATUS_CODE_UNSET, "");

      actual = OtlpProtobufUtils.transform(unsetSpan, emptyAttrs, null, "test-source");

      assertFalse(actual.getAnnotations().stream()
          .anyMatch(annotation -> annotation.getKey().equals(ERROR_TAG_KEY)));
      assertFalse(actual.getAnnotations().stream()
          .anyMatch(annotation -> annotation.getKey().equals(OTEL_STATUS_DESCRIPTION_KEY)));
    }

    @Test
    public void sourceSetFromResourceAttributesNotSpanAttributes() {
      List<KeyValue> resourceAttrs = Collections.singletonList(otlpAttribute("source", "a-src"));
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator()
          .addAttributes(otlpAttribute("source", "span-level")).build();

      actual = OtlpProtobufUtils.transform(otlpSpan, resourceAttrs, null, "ignored");

      assertEquals("a-src", actual.getSource());
      assertThat(actual.getAnnotations(), not(hasItem(new Annotation("source", "a-src"))));
      assertThat(actual.getAnnotations(), hasItem(new Annotation("_source", "span-level")));
    }

    @Test
    public void sourceHasDefaultWhenNoAttributesMatch() {
      Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();

      actual = OtlpProtobufUtils.transform(otlpSpan, emptyAttrs, null, "defaultSource");

      assertEquals("defaultSource", actual.getSource());
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

  public static class SourceFromAttributesTests {
    @Test
    public void sourceSetAccordingToPrecedenceRules() {
      Pair<String, List<KeyValue>> actual;

      // "source" attribute has highest precedence
      actual = OtlpProtobufUtils.sourceFromAttributes(
          Arrays.asList(
              otlpAttribute("hostname", "a-hostname"),
              otlpAttribute("host.id", "a-host.id"),
              otlpAttribute("source", "a-src"),
              otlpAttribute("host.name", "a-host.name")
          ), "ignore");
      assertEquals("a-src", actual._1);

      // "host.name" next highest
      actual = OtlpProtobufUtils.sourceFromAttributes(
          Arrays.asList(
              otlpAttribute("hostname", "a-hostname"),
              otlpAttribute("host.id", "a-host.id"),
              otlpAttribute("host.name", "a-host.name")
          ), "ignore");
      assertEquals("a-host.name", actual._1);

      // "hostname" next highest
      actual = OtlpProtobufUtils.sourceFromAttributes(
          Arrays.asList(
              otlpAttribute("hostname", "a-hostname"),
              otlpAttribute("host.id", "a-host.id")
          ), "ignore");
      assertEquals("a-hostname", actual._1);

      // "host.id" has lowest precedence
      actual = OtlpProtobufUtils.sourceFromAttributes(
          Arrays.asList(otlpAttribute("host.id", "a-host.id")), "ignore"
      );
      assertEquals("a-host.id", actual._1);
    }

    @Test
    public void defaultUsedWhenNoCandidateExists() {
      Pair<String, List<KeyValue>> actual = OtlpProtobufUtils.sourceFromAttributes(
          emptyAttrs, "a-default"
      );

      assertEquals("a-default", actual._1);
      assertEquals(emptyAttrs, actual._2);
    }

    @Test
    public void deletesCandidateUsedAsSource() {
      List<KeyValue> attrs = Arrays.asList(
          otlpAttribute("hostname", "a-hostname"),
          otlpAttribute("some-key", "some-val"),
          otlpAttribute("host.id", "a-host.id")
      );

      Pair<String, List<KeyValue>> actual = OtlpProtobufUtils.sourceFromAttributes(attrs, "ignore");

      assertEquals("a-hostname", actual._1);

      List<KeyValue> expectedAttrs = Arrays.asList(
          otlpAttribute("some-key", "some-val"),
          otlpAttribute("host.id", "a-host.id")
      );
      assertEquals(expectedAttrs, actual._2);
    }
  }
}
