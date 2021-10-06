package com.wavefront.agent.listeners.otlp;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.wavefront.common.Pair;

import org.apache.commons.compress.utils.Lists;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

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

/**
 * @author Xiaochen Wang (xiaochenw@vmware.com).
 * @author Glenn Oppegard (goppegard@vmware.com).
 */
public class OtlpProtobufUtilsTest {

  @Test
  public void transformsMinimalSpan() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().build();
    wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(null).build();

    wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList());

    assertWFSpanEquals(expected, actual);
  }

  @Test
  public void transformHandlesZeroDuration() {
    Span otlpSpan = OtlpTestHelpers.otlpSpanGenerator().setEndTimeUnixNano(0).build();
    wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(null).setDuration(0).build();

    wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList());

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
    wavefront.report.Span expected = OtlpTestHelpers.wfSpanGenerator(wfAttrs)
        .build();

    wavefront.report.Span actual = OtlpProtobufUtils.transform(otlpSpan, Lists.newArrayList());

    assertWFSpanEquals(expected, actual);
  }

  @Test
  public void testAttributesToWFAnnotations() {
//    TODO: test that max sizes, e.g. should we truncate to 256 chars or something
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

    assertEquals(attributes.size() + 4, wfAnnotationAsMap.size());
    assertEquals(wfAnnotationAsMap.get("empty"), "");
    assertEquals(wfAnnotationAsMap.get("a-boolean"), "true");
    assertEquals(wfAnnotationAsMap.get("a-string"), "a-value");
    assertEquals(wfAnnotationAsMap.get("a-int64"), Long.toString(Long.MAX_VALUE));
    assertEquals(wfAnnotationAsMap.get("a-double"), "2.1138");

    assertFalse(wfAnnotationAsMap.containsKey(SERVICE_NAME.getKey()));
    assertEquals(wfAnnotationAsMap.get(SERVICE_TAG_KEY), "defaultService");
    assertEquals(wfAnnotationAsMap.get(APPLICATION_TAG_KEY), "defaultApplication");
    assertEquals(wfAnnotationAsMap.get(CLUSTER_TAG_KEY), NULL_TAG_VAL);
    assertEquals(wfAnnotationAsMap.get(SHARD_TAG_KEY), NULL_TAG_VAL);
  }

  @Test
  public void testRequiredTags() {
    List<Annotation> wfAnnotations = OtlpProtobufUtils.attributesToWFAnnotations(Collections.emptyList());
    Map<String, String> annotations = getWfAnnotationAsMap(wfAnnotations);

    assertEquals(4, wfAnnotations.size());
    assertEquals("defaultApplication", annotations.get(APPLICATION_TAG_KEY));
    assertEquals("defaultService", annotations.get(SERVICE_TAG_KEY));
    assertEquals(NULL_TAG_VAL, annotations.get(CLUSTER_TAG_KEY));
    assertEquals(NULL_TAG_VAL, annotations.get(SHARD_TAG_KEY));
  }

  @Test
  public void testOtlpServiceNameTagIsUsed() {
    KeyValue serviceName = KeyValue.newBuilder().setKey(SERVICE_NAME.getKey())
        .setValue(AnyValue.newBuilder().setStringValue("a-service").build())
        .build();

    List<Annotation> wfAnnotations =
        OtlpProtobufUtils.attributesToWFAnnotations(Collections.singletonList(serviceName));
    Map<String, String> annotations = getWfAnnotationAsMap(wfAnnotations);

    assertFalse(annotations.containsKey(SERVICE_NAME.getKey()));
    assertEquals("a-service", annotations.get(SERVICE_TAG_KEY));
  }

  @Test
  public void testOtlpServiceNameTagIsUntouchedIfServiceIsSet() {
    KeyValue serviceName = KeyValue.newBuilder().setKey(SERVICE_NAME.getKey())
        .setValue(AnyValue.newBuilder().setStringValue("otlp-service").build())
        .build();
    KeyValue wfService = KeyValue.newBuilder().setKey(SERVICE_TAG_KEY)
        .setValue(AnyValue.newBuilder().setStringValue("wf-service").build())
        .build();

    List<Annotation> wfAnnotations =
        OtlpProtobufUtils.attributesToWFAnnotations(Arrays.asList(serviceName, wfService));
    Map<String, String> annotations = getWfAnnotationAsMap(wfAnnotations);

    assertEquals("otlp-service", annotations.get(SERVICE_NAME.getKey()));
    assertEquals("wf-service", annotations.get(SERVICE_TAG_KEY));
  }

  @NotNull
  private Map<String, String> getWfAnnotationAsMap(List<Annotation> wfAnnotations) {
    Map<String, String> wfAnnotationAsMap = Maps.newHashMap();
    for (Annotation annotation : wfAnnotations) {
      wfAnnotationAsMap.put(annotation.getKey(), annotation.getValue());
    }
    return wfAnnotationAsMap;
  }
}