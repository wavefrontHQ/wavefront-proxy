package com.wavefront.agent.queueing;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wavefront.agent.data.DataSubmissionTask;
import com.wavefront.agent.data.DefaultEntityPropertiesForTesting;
import com.wavefront.agent.data.EventDataSubmissionTask;
import com.wavefront.agent.data.LineDelimitedDataSubmissionTask;
import com.wavefront.agent.data.LogDataSubmissionTask;
import com.wavefront.agent.data.SourceTagSubmissionTask;
import com.wavefront.data.ReportableEntityType;
import com.wavefront.dto.Event;
import com.wavefront.dto.Log;
import com.wavefront.dto.SourceTag;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;
import wavefront.report.Annotation;
import wavefront.report.ReportEvent;
import wavefront.report.ReportLog;
import wavefront.report.ReportSourceTag;
import wavefront.report.SourceOperationType;
import wavefront.report.SourceTagAction;

/**
 * Guards against a repeat of the ca5729de regression: RetryTaskConverter restricts Jackson's
 * default typing to a fixed allowlist of concrete collection classes
 * (RetryTaskConverter.ALLOWED_COLLECTION_TYPES) so a crafted queue entry can't name a
 * gadget-chain class. That protection only holds if every List/Map/Set-typed field this codebase
 * actually persists resolves, at runtime, to a class on that allowlist - otherwise the field
 * silently fails to deserialize and the whole task is dropped (logged as a WARNING, not a
 * crash).
 *
 * <p>Two independent checks, so a new field can't slip through either gap:
 *
 * <p>1. {@link #testNoUnvettedAmbiguousFields()} reflects over every known DataSubmissionTask
 * subtype and DTO's declared fields, without needing a populated instance, and fails if an
 * interface/abstract-typed field isn't in {@link #VETTED_AMBIGUOUS_FIELDS}. This catches a new
 * field the moment it's declared, even if nobody remembers to give it a value in a test.
 *
 * <p>2. {@link #testAllTaskTypesRoundTripAndUseOnlyAllowedCollectionTypes()} builds one
 * fully-populated instance of every subtype, round-trips it through RetryTaskConverter, and
 * walks the object graph asserting every ambiguous field's actual runtime value class is on
 * RetryTaskConverter.ALLOWED_COLLECTION_TYPES. This catches drift - e.g. a known field switching
 * from {@code new ArrayList<>(x)} to some other concrete type - and covers values nested inside
 * a vetted field (e.g. Event#dimensions is a Map whose values are themselves Lists).
 *
 * <p>When either check fails after adding a field: confirm the field's runtime value is (or is
 * changed to be) one of RetryTaskConverter.ALLOWED_COLLECTION_TYPES, add it to a sample instance
 * below, then add "SimpleClassName#fieldName" to VETTED_AMBIGUOUS_FIELDS.
 */
public class RetryTaskConverterFieldCoverageTest {

  // Every concrete DataSubmissionTask subtype plus the DTOs they carry. Update when adding a new
  // subtype or a new DTO type to one of them.
  private static final List<Class<?>> SCANNED_CLASSES =
      Arrays.asList(
          LineDelimitedDataSubmissionTask.class,
          EventDataSubmissionTask.class,
          SourceTagSubmissionTask.class,
          LogDataSubmissionTask.class,
          Event.class,
          SourceTag.class,
          Log.class);

  // "SimpleClassName#fieldName" for every field (on the classes above, or their superclasses)
  // whose declared type is an interface/abstract class - i.e. ambiguous under Jackson's default
  // typing. Extend this - and the sample data in buildSamples() - when adding a new one.
  private static final Set<String> VETTED_AMBIGUOUS_FIELDS =
      new HashSet<>(
          Arrays.asList(
              "LineDelimitedDataSubmissionTask#payload",
              "EventDataSubmissionTask#events",
              "LogDataSubmissionTask#logs",
              "Event#annotations",
              "Event#dimensions",
              "Event#hosts",
              "Event#tags",
              "SourceTag#annotations",
              "Log#annotations"));

  @Test
  public void testNoUnvettedAmbiguousFields() {
    for (Class<?> clazz : SCANNED_CLASSES) {
      for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
        for (Field field : c.getDeclaredFields()) {
          if (Modifier.isStatic(field.getModifiers()) || Modifier.isTransient(field.getModifiers())) {
            continue;
          }
          if (!isAmbiguousType(field.getType())) {
            continue;
          }
          String key = clazz.getSimpleName() + "#" + field.getName();
          assertTrue(
              "New ambiguous-typed field '"
                  + key
                  + "' (declared type "
                  + field.getType()
                  + ") isn't vetted for RetryTaskConverter's restricted default typing - see "
                  + "this test's class-level javadoc for what to do next.",
              VETTED_AMBIGUOUS_FIELDS.contains(key));
        }
      }
    }
  }

  @Test
  public void testAllTaskTypesRoundTripAndUseOnlyAllowedCollectionTypes() throws Exception {
    for (DataSubmissionTask<?> task : buildSamples()) {
      assertRoundTrips(task);
      assertOnlyAllowedCollectionTypesInVettedFields(task);
    }
  }

  private static boolean isAmbiguousType(Class<?> type) {
    if (type.isEnum() || type.isPrimitive() || type == String.class) {
      return false;
    }
    return type.isInterface() || Modifier.isAbstract(type.getModifiers());
  }

  private List<DataSubmissionTask<?>> buildSamples() {
    UUID proxyId = UUID.randomUUID();
    List<DataSubmissionTask<?>> samples = new ArrayList<>();
    samples.add(
        new LineDelimitedDataSubmissionTask(
            null,
            proxyId,
            new DefaultEntityPropertiesForTesting(),
            null,
            "wavefront",
            ReportableEntityType.POINT,
            "2878",
            ImmutableList.of("item1", "item2", "item3"),
            () -> 1L));
    samples.add(
        new EventDataSubmissionTask(
            null,
            proxyId,
            new DefaultEntityPropertiesForTesting(),
            null,
            "2878",
            ImmutableList.of(
                new Event(
                    ReportEvent.newBuilder()
                        .setStartTime(1L)
                        .setEndTime(2L)
                        .setName("event")
                        .setHosts(ImmutableList.of("host1", "host2"))
                        .setDimensions(ImmutableMap.of("multi", ImmutableList.of("bar", "baz")))
                        .setAnnotations(ImmutableMap.of("severity", "INFO"))
                        .setTags(ImmutableList.of("tag1"))
                        .build())),
            () -> 1L));
    samples.add(
        new SourceTagSubmissionTask(
            null,
            new DefaultEntityPropertiesForTesting(),
            null,
            "2878",
            new SourceTag(
                ReportSourceTag.newBuilder()
                    .setOperation(SourceOperationType.SOURCE_TAG)
                    .setAction(SourceTagAction.SAVE)
                    .setSource("testSource")
                    .setAnnotations(ImmutableList.of("newtag1", "newtag2"))
                    .build()),
            () -> 1L));
    samples.add(
        new LogDataSubmissionTask(
            null,
            proxyId,
            new DefaultEntityPropertiesForTesting(),
            null,
            "2878",
            ImmutableList.of(
                new Log(
                    ReportLog.newBuilder()
                        .setTimestamp(1L)
                        .setMessage("log message")
                        .setHost("host1")
                        .setAnnotations(
                            ImmutableList.of(new Annotation("severity", "INFO")))
                        .build())),
            () -> 1L));
    return samples;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void assertRoundTrips(DataSubmissionTask<?> task) throws Exception {
    RetryTaskConverter converter =
        new RetryTaskConverter("2878", RetryTaskConverter.CompressionType.NONE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    converter.serializeToStream(task, out);
    Object restored = converter.fromBytes(out.toByteArray());
    assertNotNull(
        task.getClass().getSimpleName() + " failed to round-trip through RetryTaskConverter",
        restored);
  }

  private void assertOnlyAllowedCollectionTypesInVettedFields(Object root) throws Exception {
    for (Class<?> clazz : SCANNED_CLASSES) {
      Object instance = unwrapIfTask(root, clazz);
      if (!clazz.isInstance(instance)) {
        continue;
      }
      for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
        for (Field field : c.getDeclaredFields()) {
          String key = clazz.getSimpleName() + "#" + field.getName();
          if (!VETTED_AMBIGUOUS_FIELDS.contains(key)) {
            continue;
          }
          field.setAccessible(true);
          assertValueUsesAllowedCollectionTypes(key, field.get(instance));
        }
      }
    }
  }

  // The sample DataSubmissionTask instances embed Event/SourceTag/Log; find the nested instance
  // of the requested type so its own fields can be checked too, without a generic deep walk.
  @SuppressWarnings("unchecked")
  private Object unwrapIfTask(Object root, Class<?> target) throws ReflectiveOperationException {
    if (target.isInstance(root)) {
      return root;
    }
    if (target == Event.class && root instanceof EventDataSubmissionTask) {
      List<Event> events = ((EventDataSubmissionTask) root).payload();
      return events.isEmpty() ? null : events.get(0);
    }
    if (target == SourceTag.class && root instanceof SourceTagSubmissionTask) {
      return ((SourceTagSubmissionTask) root).payload();
    }
    if (target == Log.class && root instanceof LogDataSubmissionTask) {
      List<Log> logs = (List<Log>) getFieldValue(root, "logs");
      return logs.isEmpty() ? null : logs.get(0);
    }
    return null;
  }

  private Object getFieldValue(Object instance, String fieldName)
      throws ReflectiveOperationException {
    for (Class<?> c = instance.getClass(); c != null; c = c.getSuperclass()) {
      try {
        Field field = c.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
      } catch (NoSuchFieldException e) {
        // field lives on a different class in the hierarchy - keep looking.
      }
    }
    throw new NoSuchFieldException(
        fieldName + " not found on " + instance.getClass() + " or its superclasses");
  }

  private void assertValueUsesAllowedCollectionTypes(String fieldKey, Object value) {
    if (value == null) {
      return;
    }
    if (value instanceof Map) {
      assertAllowed(fieldKey, value.getClass());
      for (Object v : ((Map<?, ?>) value).values()) {
        assertValueUsesAllowedCollectionTypes(fieldKey + " value", v);
      }
    } else if (value instanceof Collection) {
      assertAllowed(fieldKey, value.getClass());
      for (Object element : (Collection<?>) value) {
        assertValueUsesAllowedCollectionTypes(fieldKey + " element", element);
      }
    }
  }

  private void assertAllowed(String fieldKey, Class<?> clazz) {
    assertTrue(
        fieldKey
            + " holds a "
            + clazz.getName()
            + ", which is not in RetryTaskConverter.ALLOWED_COLLECTION_TYPES "
            + RetryTaskConverter.ALLOWED_COLLECTION_TYPES
            + " - see this test's class-level javadoc for what to do next.",
        RetryTaskConverter.ALLOWED_COLLECTION_TYPES.contains(clazz));
  }
}
