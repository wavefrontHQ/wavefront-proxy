package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

import com.wavefront.agent.handlers.MockReportableEntityHandlerFactory;
import com.wavefront.agent.handlers.ReportableEntityHandler;
import com.wavefront.agent.preprocessor.ReportableEntityPreprocessor;
import com.wavefront.agent.sampler.SpanSampler;
import com.wavefront.internal.SpanDerivedMetricsUtils;
import com.wavefront.internal.reporter.WavefrontInternalReporter;
import com.wavefront.sdk.common.Pair;
import com.yammer.metrics.core.Counter;

import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import wavefront.report.Annotation;
import wavefront.report.SpanLogs;

import static com.wavefront.agent.listeners.otlp.OtlpProtobufUtils.OTEL_STATUS_DESCRIPTION_KEY;
import static com.wavefront.agent.listeners.otlp.OtlpProtobufUtils.transformAll;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.assertWFSpanEquals;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.hasKey;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.otlpAttribute;
import static com.wavefront.agent.listeners.otlp.OtlpTestHelpers.parentSpanIdPair;
import static com.wavefront.internal.SpanDerivedMetricsUtils.ERROR_SPAN_TAG_VAL;
import static com.wavefront.sdk.common.Constants.APPLICATION_TAG_KEY;
import static com.wavefront.sdk.common.Constants.CLUSTER_TAG_KEY;
import static com.wavefront.sdk.common.Constants.COMPONENT_TAG_KEY;
import static com.wavefront.sdk.common.Constants.ERROR_TAG_KEY;
import static com.wavefront.sdk.common.Constants.NULL_TAG_VAL;
import static com.wavefront.sdk.common.Constants.SERVICE_TAG_KEY;
import static com.wavefront.sdk.common.Constants.SHARD_TAG_KEY;
import static io.opentelemetry.semconv.resource.attributes.ResourceAttributes.SERVICE_NAME;
import static org.easymock.EasyMock.anyLong;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.captureBoolean;
import static org.easymock.EasyMock.eq;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SpanDerivedMetricsUtils.class, OtlpProtobufUtils.class})
public class OtlpProtobufUtilsTest {

  private final static List<KeyValue> emptyAttrs = Collections.unmodifiableList(new ArrayList<>());
  private final SpanSampler mockSampler = EasyMock.createMock(SpanSampler.class);
  private final ReportableEntityHandler<wavefront.report.Span, String> mockSpanHandler =
      MockReportableEntityHandlerFactory.getMockTraceHandler();
  private final wavefront.report.Span wfMinimalSpan = OtlpTestHelpers.wfSpanGenerator(null).build();
  private wavefront.report.Span actualSpan;

  private static Map<String, String> getWfAnnotationAsMap(List<Annotation> wfAnnotations) {
    Map<String, String> wfAnnotationAsMap = Maps.newHashMap();
    for (Annotation annotation : wfAnnotations) {
      wfAnnotationAsMap.put(annotation.getKey(), annotation.getValue());
    }
    return wfAnnotationAsMap;
  }

  @Before
  public void setup() {
    actualSpan = null;
    EasyMock.reset(mockSampler, mockSpanHandler);
  }

  @Test
  public void exportToWavefrontDoesNotReportIfPreprocessorFilteredSpan() {
    // Arrange
    ReportableEntityPreprocessor mockPreprocessor =
        EasyMock.createMock(ReportableEntityPreprocessor.class);
    ExportTraceServiceRequest otlpRequest =
        OtlpTestHelpers.otlpTraceRequest(OtlpTestHelpers.otlpSpanGenerator().build());

    PowerMock.mockStaticPartial(
        OtlpProtobufUtils.class, "fromOtlpRequest", "wasFilteredByPreprocessor"
    );
    EasyMock.expect(
        OtlpProtobufUtils.fromOtlpRequest(otlpRequest, mockPreprocessor, "test-source")
    ).andReturn(Arrays.asList(Pair.of(wfMinimalSpan, null)));
    EasyMock.expect(
        OtlpProtobufUtils.wasFilteredByPreprocessor(eq(wfMinimalSpan), eq(mockSpanHandler),
            eq(mockPreprocessor))
    ).andReturn(true);

    EasyMock.replay(mockPreprocessor, mockSpanHandler);
    PowerMock.replay(OtlpProtobufUtils.class);

    // Act
    OtlpProtobufUtils.exportToWavefront(otlpRequest, mockSpanHandler, null, () -> mockPreprocessor,
        null, "test-source", null, null, null);

    // Assert
    EasyMock.verify(mockPreprocessor, mockSpanHandler);
    PowerMock.verify(OtlpProtobufUtils.class);
  }

  @Test
  public void exportToWavefrontReportsSpanIfSamplerReturnsTrue() {
    // Arrange
    Counter mockCounter = EasyMock.createMock(Counter.class);
    Capture<wavefront.report.Span> samplerCapture = EasyMock.newCapture();
    EasyMock.expect(mockSampler.sample(capture(samplerCapture), eq(mockCounter)))
        .andReturn(true);

    Capture<wavefront.report.Span> handlerCapture = EasyMock.newCapture();
    mockSpanHandler.report(capture(handlerCapture));
    EasyMock.expectLastCall();

    PowerMock.mockStaticPartial(OtlpProtobufUtils.class, "reportREDMetrics");
    Pair<Map<String, String>, String> heartbeat = Pair.of(ImmutableMap.of("foo", "bar"), "src");
    EasyMock.expect(OtlpProtobufUtils.reportREDMetrics(anyObject(), anyObject(), anyObject()))
        .andReturn(heartbeat);

    EasyMock.replay(mockCounter, mockSampler, mockSpanHandler);
    PowerMock.replay(OtlpProtobufUtils.class);

    // Act
    ExportTraceServiceRequest otlpRequest =
        OtlpTestHelpers.otlpTraceRequest(OtlpTestHelpers.otlpSpanGenerator().build());
    Set<Pair<Map<String, String>, String>> discoveredHeartbeats = Sets.newConcurrentHashSet();

    OtlpProtobufUtils.exportToWavefront(otlpRequest, mockSpanHandler, null, null,
        Pair.of(mockSampler, mockCounter), "test-source", discoveredHeartbeats, null, null);

    // Assert
    EasyMock.verify(mockCounter, mockSampler, mockSpanHandler);
    PowerMock.verify(OtlpProtobufUtils.class);
    assertEquals(samplerCapture.getValue(), handlerCapture.getValue());
    assertTrue(discoveredHeartbeats.contains(heartbeat));
  }

  @Test
  public void exportToWavefrontReportsREDMetricsEvenWhenSpanNotSampled() {
    // Arrange
    EasyMock.expect(mockSampler.sample(anyObject(), anyObject()))
        .andReturn(false);

    PowerMock.mockStaticPartial(OtlpProtobufUtils.class, "reportREDMetrics");
    Pair<Map<String, String>, String> heartbeat = Pair.of(ImmutableMap.of("foo", "bar"), "src");
    EasyMock.expect(OtlpProtobufUtils.reportREDMetrics(anyObject(), anyObject(), anyObject()))
        .andReturn(heartbeat);

    EasyMock.replay(mockSampler, mockSpanHandler);
    PowerMock.replay(OtlpProtobufUtils.class);

    // Act
    ExportTraceServiceRequest otlpRequest =
        OtlpTestHelpers.otlpTraceRequest(OtlpTestHelpers.otlpSpanGenerator().build());
    Set<Pair<Map<String, String>, String>> discoveredHeartbeats = Sets.newConcurrentHashSet();

    OtlpProtobufUtils.exportToWavefront(otlpRequest, mockSpanHandler, null, null,
        Pair.of(mockSampler, null), "test-source", discoveredHeartbeats, null, null);

    // Assert
    EasyMock.verify(mockSampler, mockSpanHandler);
    PowerMock.verify(OtlpProtobufUtils.class);
    assertTrue(discoveredHeartbeats.contains(heartbeat));
  }

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

  @Test
  public void transformSpanHandlesMinimalSpan() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
    wavefront.report.Span expectedSpan = OtlpTestHelpers.wfSpanGenerator(null).build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, null, null, "test-source");

    assertWFSpanEquals(expectedSpan, actualSpan);
  }

  @Test
  public void transformSpanHandlesZeroDuration() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().setEndTimeUnixNano(0).build();
    wavefront.report.Span expectedSpan =
        OtlpTestHelpers.wfSpanGenerator(null).setDuration(0).build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, null, null, "test-source");

    assertWFSpanEquals(expectedSpan, actualSpan);
  }

  @Test
  public void transformSpanHandlesSpanAttributes() {
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
    wavefront.report.Span expectedSpan = OtlpTestHelpers.wfSpanGenerator(wfAttrs).build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, null, null, "test-source");

    assertWFSpanEquals(expectedSpan, actualSpan);
  }

  @Test
  public void transformSpanConvertsResourceAttributesToAnnotations() {
    List<KeyValue> resourceAttrs = Collections.singletonList(otlpAttribute("r-key", "r-value"));
    wavefront.report.Span expectedSpan = OtlpTestHelpers.wfSpanGenerator(
        Collections.singletonList(new Annotation("r-key", "r-value"))
    ).build();

    actualSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanGenerator().build(), resourceAttrs, null, null, "test-source"
    );

    assertWFSpanEquals(expectedSpan, actualSpan);
  }

  @Test
  public void transformSpanGivesSpanAttributesHigherPrecedenceThanResourceAttributes() {
    String key = "the-key";
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator()
        .addAttributes(otlpAttribute(key, "span-value")).build();
    List<KeyValue> resourceAttrs = Collections.singletonList(otlpAttribute(key, "rsrc-value"));

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, resourceAttrs, null, null, "test-source");

    assertThat(actualSpan.getAnnotations(), not(hasItem(new Annotation(key, "rsrc-value"))));
    assertThat(actualSpan.getAnnotations(), hasItem(new Annotation(key, "span-value")));
  }

  @Test
  public void transformSpanHandlesInstrumentationLibrary() {
    InstrumentationLibrary library = InstrumentationLibrary.newBuilder()
        .setName("grpc").setVersion("1.0").build();
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, library, null, "test-source");

    assertThat(actualSpan.getAnnotations(), hasItem(new Annotation("otel.library.name", "grpc")));
    assertThat(actualSpan.getAnnotations(), hasItem(new Annotation("otel.library.version", "1.0")));
  }

  @Test
  public void transformSpanAddsDroppedCountTags() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().setDroppedEventsCount(1).build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, null, null, "test-source");

    assertThat(actualSpan.getAnnotations(),
        hasItem(new Annotation("otel.dropped_events_count", "1")));
  }

  @Test
  public void transformSpanAppliesPreprocessorRules() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
    List<Annotation> wfAttrs = Collections.singletonList(
        Annotation.newBuilder().setKey("my-key").setValue("my-value").build()
    );
    ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.addTagIfNotExistsPreprocessor(wfAttrs);
    wavefront.report.Span expectedSpan = OtlpTestHelpers.wfSpanGenerator(wfAttrs).build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, null, preprocessor, "test-source");

    assertWFSpanEquals(expectedSpan, actualSpan);
  }

  @Test
  public void transformSpanAppliesPreprocessorBeforeSettingRequiredTags() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
    List<Annotation> wfAttrs = Collections.singletonList(
        Annotation.newBuilder().setKey(APPLICATION_TAG_KEY).setValue("an-app").build()
    );
    ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.addTagIfNotExistsPreprocessor(wfAttrs);
    wavefront.report.Span expectedSpan = OtlpTestHelpers.wfSpanGenerator(wfAttrs).build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, null, preprocessor, "test-source");

    assertWFSpanEquals(expectedSpan, actualSpan);
  }

  @Test
  public void transformSpanTranslatesSpanKindToAnnotation() {
    wavefront.report.Span clientSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_CLIENT),
        emptyAttrs, null, null, "test-source");
    assertThat(clientSpan.getAnnotations(), hasItem(new Annotation("span.kind", "client")));

    wavefront.report.Span consumerSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_CONSUMER),
        emptyAttrs, null, null, "test-source");
    assertThat(consumerSpan.getAnnotations(), hasItem(new Annotation("span.kind", "consumer")));

    wavefront.report.Span internalSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_INTERNAL),
        emptyAttrs, null, null, "test-source");
    assertThat(internalSpan.getAnnotations(), hasItem(new Annotation("span.kind", "internal")));

    wavefront.report.Span producerSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_PRODUCER),
        emptyAttrs, null, null, "test-source");
    assertThat(producerSpan.getAnnotations(), hasItem(new Annotation("span.kind", "producer")));

    wavefront.report.Span serverSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_SERVER),
        emptyAttrs, null, null, "test-source");
    assertThat(serverSpan.getAnnotations(), hasItem(new Annotation("span.kind", "server")));

    wavefront.report.Span unspecifiedSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanWithKind(Span.SpanKind.SPAN_KIND_UNSPECIFIED),
        emptyAttrs, null, null, "test-source");
    assertThat(unspecifiedSpan.getAnnotations(),
        hasItem(new Annotation("span.kind", "unspecified")));

    wavefront.report.Span noKindSpan = OtlpProtobufUtils.transformSpan(
        OtlpTestHelpers.otlpSpanGenerator().build(),
        emptyAttrs, null, null, "test-source");
    assertThat(noKindSpan.getAnnotations(),
        hasItem(new Annotation("span.kind", "unspecified")));
  }

  @Test
  public void transformSpanHandlesSpanStatusIfError() {
    // Error Status without Message
    Span errorSpan = OtlpTestHelpers.otlpSpanWithStatus(Status.StatusCode.STATUS_CODE_ERROR, "");

    actualSpan = OtlpProtobufUtils.transformSpan(errorSpan, emptyAttrs, null, null, "test-source");

    assertThat(actualSpan.getAnnotations(), hasItem(new Annotation(ERROR_TAG_KEY, ERROR_SPAN_TAG_VAL)));
    assertThat(actualSpan.getAnnotations(), not(hasKey(OTEL_STATUS_DESCRIPTION_KEY)));

    // Error Status with Message
    Span errorSpanWithMessage = OtlpTestHelpers.otlpSpanWithStatus(
        Status.StatusCode.STATUS_CODE_ERROR, "a description");

    actualSpan = OtlpProtobufUtils.transformSpan(errorSpanWithMessage, emptyAttrs, null, null, "test-source");

    assertThat(actualSpan.getAnnotations(), hasItem(new Annotation(ERROR_TAG_KEY, ERROR_SPAN_TAG_VAL)));
    assertThat(actualSpan.getAnnotations(),
        hasItem(new Annotation(OTEL_STATUS_DESCRIPTION_KEY, "a description")));
  }

  @Test
  public void transformSpanIgnoresSpanStatusIfNotError() {
    // Ok Status
    Span okSpan = OtlpTestHelpers.otlpSpanWithStatus(Status.StatusCode.STATUS_CODE_OK, "");

    actualSpan = OtlpProtobufUtils.transformSpan(okSpan, emptyAttrs, null, null, "test-source");

    assertThat(actualSpan.getAnnotations(), not(hasKey(ERROR_TAG_KEY)));
    assertThat(actualSpan.getAnnotations(), not(hasKey(OTEL_STATUS_DESCRIPTION_KEY)));

    // Unset Status
    Span unsetSpan = OtlpTestHelpers.otlpSpanWithStatus(Status.StatusCode.STATUS_CODE_UNSET, "");

    actualSpan = OtlpProtobufUtils.transformSpan(unsetSpan, emptyAttrs, null, null, "test-source");

    assertThat(actualSpan.getAnnotations(), not(hasKey(ERROR_TAG_KEY)));
    assertThat(actualSpan.getAnnotations(), not(hasKey(OTEL_STATUS_DESCRIPTION_KEY)));
  }
//  }

  @Test
  public void transformSpanSetsSourceFromResourceAttributesNotSpanAttributes() {
    List<KeyValue> resourceAttrs = Collections.singletonList(otlpAttribute("source", "a-src"));
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator()
        .addAttributes(otlpAttribute("source", "span-level")).build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, resourceAttrs, null, null, "ignored");

    assertEquals("a-src", actualSpan.getSource());
    assertThat(actualSpan.getAnnotations(), not(hasItem(new Annotation("source", "a-src"))));
    assertThat(actualSpan.getAnnotations(), hasItem(new Annotation("_source", "span-level")));
  }
//  }

  @Test
  public void transformSpanUsesDefaultSourceWhenNoAttributesMatch() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();

    actualSpan = OtlpProtobufUtils.transformSpan(otlpSpan, emptyAttrs, null, null, "defaultSource");

    assertEquals("defaultSource", actualSpan.getSource());
  }

  @Test
  public void transformEventsConvertsToWFSpanLogs() {
    int droppedAttrsCount = 1;
    Span.Event otlpEvent = OtlpTestHelpers.otlpSpanEvent(droppedAttrsCount);
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().addEvents(otlpEvent).build();

    wavefront.report.SpanLogs expected =
        OtlpTestHelpers.wfSpanLogsGenerator(wfMinimalSpan, droppedAttrsCount).build();

    SpanLogs actual = OtlpProtobufUtils.transformEvents(otlpSpan, wfMinimalSpan);

    assertEquals(expected, actual);
  }

  @Test
  public void transformAllSetsAttributeWhenOtlpEventsExists() {
    Span.Event otlpEvent = OtlpTestHelpers.otlpSpanEvent(0);
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().addEvents(otlpEvent).build();

    Pair<wavefront.report.Span, SpanLogs> actual =
        transformAll(otlpSpan, emptyAttrs, null, null, "test-source");

    assertThat(actual._1.getAnnotations(), hasKey("_spanLogs"));
    assertThat(actual._2.getLogs(), not(empty()));
  }

  @Test
  public void transformAllDoesNotSetAttributeWhenNoOtlpEventsExists() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
    assertThat(otlpSpan.getEventsList(), empty());

    Pair<wavefront.report.Span, SpanLogs> actual =
        transformAll(otlpSpan, emptyAttrs, null, null, "test-source");

    assertThat(actual._1.getAnnotations(), not(hasKey("_spanLogs")));
    assertThat(actual._2.getLogs(), empty());
  }

  @Test
  public void wasFilteredByPreprocessorHandlesNullPreprocessor() {
    ReportableEntityPreprocessor preprocessor = null;

    assertFalse(OtlpProtobufUtils.wasFilteredByPreprocessor(wfMinimalSpan, mockSpanHandler, preprocessor));
  }

  @Test
  public void wasFilteredByPreprocessorCanReject() {
    ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.rejectSpanPreprocessor();
    mockSpanHandler.reject(wfMinimalSpan, "span rejected for testing purpose");
    EasyMock.expectLastCall();
    EasyMock.replay(mockSpanHandler);

    assertTrue(OtlpProtobufUtils.wasFilteredByPreprocessor(wfMinimalSpan, mockSpanHandler, preprocessor));
    EasyMock.verify(mockSpanHandler);
  }

  @Test
  public void wasFilteredByPreprocessorCanBlock() {
    ReportableEntityPreprocessor preprocessor = OtlpTestHelpers.blockSpanPreprocessor();
    mockSpanHandler.block(wfMinimalSpan);
    EasyMock.expectLastCall();
    EasyMock.replay(mockSpanHandler);

    assertTrue(OtlpProtobufUtils.wasFilteredByPreprocessor(wfMinimalSpan, mockSpanHandler, preprocessor));
    EasyMock.verify(mockSpanHandler);
  }

  @Test
  public void sourceFromAttributesSetsSourceAccordingToPrecedenceRules() {
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
  public void sourceFromAttributesUsesDefaultWhenNoCandidateExists() {
    Pair<String, List<KeyValue>> actual = OtlpProtobufUtils.sourceFromAttributes(
        emptyAttrs, "a-default"
    );

    assertEquals("a-default", actual._1);
    assertEquals(emptyAttrs, actual._2);
  }

  @Test
  public void sourceFromAttributesDeletesCandidateUsedAsSource() {
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

  @Test
  public void reportREDMetricsCallsDerivedMetricsUtils() {
    PowerMock.mockStatic(SpanDerivedMetricsUtils.class);
    WavefrontInternalReporter mockInternalReporter =
        EasyMock.niceMock(WavefrontInternalReporter.class);

    List<Annotation> wfAttrs = Arrays.asList(
        new Annotation(APPLICATION_TAG_KEY, "app"),
        new Annotation(SERVICE_TAG_KEY, "svc"),
        new Annotation(CLUSTER_TAG_KEY, "east1"),
        new Annotation(SHARD_TAG_KEY, "az1"),
        new Annotation(COMPONENT_TAG_KEY, "comp"),
        new Annotation(ERROR_TAG_KEY, "true")
    );
    wavefront.report.Span wfSpan = OtlpTestHelpers.wfSpanGenerator(wfAttrs).build();

    List<Pair<String, String>> spanTags = wfSpan.getAnnotations().stream()
        .map(a -> Pair.of(a.getKey(), a.getValue())).collect(Collectors.toList());
    HashSet<String> customKeys = Sets.newHashSet("a", "b", "c");
    Pair<Map<String, String>, String> mockReturn = Pair.of(ImmutableMap.of("key", "val"), "foo");

    EasyMock.expect(
        SpanDerivedMetricsUtils.reportWavefrontGeneratedData(
            eq(mockInternalReporter), eq("root"), eq("app"), eq("svc"), eq("east1"), eq("az1"),
            eq("test-source"), eq("comp"), eq(true),
            eq(TimeUnit.MILLISECONDS.toMicros(wfSpan.getDuration())), eq(customKeys), eq(spanTags)
        )
    ).andReturn(mockReturn);
    PowerMock.replay(SpanDerivedMetricsUtils.class);

    Pair<Map<String, String>, String> actual =
        OtlpProtobufUtils.reportREDMetrics(wfSpan, mockInternalReporter, customKeys);

    assertEquals(mockReturn, actual);
    PowerMock.verify(SpanDerivedMetricsUtils.class);
  }

  @Test
  public void reportREDMetricsProvidesDefaults() {
    PowerMock.mockStatic(SpanDerivedMetricsUtils.class);

    Capture<Boolean> isError = EasyMock.newCapture();
    Capture<String> componentTag = EasyMock.newCapture();
    EasyMock.expect(
        SpanDerivedMetricsUtils.reportWavefrontGeneratedData(
            anyObject(), anyObject(), anyObject(), anyObject(), anyObject(), anyObject(),
            anyObject(), capture(componentTag), captureBoolean(isError), anyLong(), anyObject(),
            anyObject()
        )
    ).andReturn(Pair.of(null, null));
    PowerMock.replay(SpanDerivedMetricsUtils.class);

    assertThat(wfMinimalSpan.getAnnotations(), not(hasKey(ERROR_TAG_KEY)));
    assertThat(wfMinimalSpan.getAnnotations(), not(hasKey(COMPONENT_TAG_KEY)));
    OtlpProtobufUtils.reportREDMetrics(wfMinimalSpan, null, null);

    assertFalse(isError.getValue());
    assertEquals(NULL_TAG_VAL, componentTag.getValue());
    PowerMock.verify(SpanDerivedMetricsUtils.class);
  }

  @Test
  public void annotationsFromInstrumentationLibraryWithNullOrEmptyLibrary() {
    assertEquals(Collections.emptyList(),
        OtlpProtobufUtils.annotationsFromInstrumentationLibrary(null));

    InstrumentationLibrary emptyLibrary = InstrumentationLibrary.newBuilder().build();
    assertEquals(Collections.emptyList(),
        OtlpProtobufUtils.annotationsFromInstrumentationLibrary(emptyLibrary));
  }

  @Test
  public void annotationsFromInstrumentationLibraryWithLibraryData() {
    InstrumentationLibrary library =
        InstrumentationLibrary.newBuilder().setName("net/http").build();

    assertEquals(Collections.singletonList(new Annotation("otel.library.name", "net/http")),
        OtlpProtobufUtils.annotationsFromInstrumentationLibrary(library));

    library = library.toBuilder().setVersion("1.0.0").build();

    assertEquals(
        Arrays.asList(new Annotation("otel.library.name", "net/http"),
            new Annotation("otel.library.version", "1.0.0")),
        OtlpProtobufUtils.annotationsFromInstrumentationLibrary(library)
    );
  }

  @Test
  public void annotationsFromDroppedCountsWithZeroValues() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();

    assertEquals(0, otlpSpan.getDroppedAttributesCount());
    assertEquals(0, otlpSpan.getDroppedEventsCount());
    assertEquals(0, otlpSpan.getDroppedLinksCount());

    assertThat(OtlpProtobufUtils.annotationsFromDroppedCounts(otlpSpan), empty());
  }

  @Test
  public void annotationsFromDroppedCountsWithNonZeroValues() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator()
        .setDroppedAttributesCount(1)
        .setDroppedEventsCount(2)
        .setDroppedLinksCount(3)
        .build();

    List<Annotation> actual = OtlpProtobufUtils.annotationsFromDroppedCounts(otlpSpan);
    assertThat(actual, hasSize(3));
    assertThat(actual, hasItem(new Annotation("otel.dropped_attributes_count", "1")));
    assertThat(actual, hasItem(new Annotation("otel.dropped_events_count", "2")));
    assertThat(actual, hasItem(new Annotation("otel.dropped_links_count", "3")));
  }
}
