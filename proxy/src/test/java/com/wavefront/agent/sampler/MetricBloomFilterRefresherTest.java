package com.wavefront.agent.sampler;

import com.wavefront.api.BloomFilterAPI;
import com.wavefront.api.agent.BloomFilterDTO;
import org.junit.Test;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MetricBloomFilterRefresherTest {

  @Test
  public void testRefreshPassesLookbackAndUpdatesSampler() {
    List<Long> requestedEpochDays = new ArrayList<>();
    List<Integer> requestedLookbacks = new ArrayList<>();
    List<String> requestedNames = new ArrayList<>();
    RecordingSampler sampler = new RecordingSampler();

    BloomFilterAPI api =
        (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> {
          requestedEpochDays.add(epochDay);
          requestedLookbacks.add(lookbackDays);
          requestedNames.add(bloomFilterName);
          return new BloomFilterDTO();
        };

    MetricBloomFilterRefresher refresher =
        new MetricBloomFilterRefresher(
            api, UUID.randomUUID(), "Bearer test", "CUSTOMER_SERIES", 5, 3, sampler);

    refresher.refresh();

    assertEquals(1, sampler.updateCount);
    assertNotNull(sampler.lastBloomFilterDTO);
    assertEquals(1, requestedEpochDays.size());
    assertEquals(1, requestedLookbacks.size());
    assertEquals(1, requestedNames.size());
    assertEquals(3, requestedLookbacks.get(0).intValue());
    assertEquals("CUSTOMER_SERIES", requestedNames.get(0));
  }

  @Test
  public void testRefreshDisablesEndpointOn404() {
    AtomicInteger calls = new AtomicInteger(0);
    RecordingSampler sampler = new RecordingSampler();
    BloomFilterAPI api =
        (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> {
          calls.incrementAndGet();
          throw new ClientErrorException(Response.status(404).build());
        };

    MetricBloomFilterRefresher refresher =
        new MetricBloomFilterRefresher(
            api, UUID.randomUUID(), "Bearer test", "CUSTOMER_SERIES", 5, 3, sampler);

    refresher.refresh();
    refresher.refresh();

    assertEquals(1, calls.get());
    assertEquals(0, sampler.updateCount);
  }

  @Test
  public void testRefreshDoesNotDisableEndpointOnNon404ClientErrors() {
    AtomicInteger calls = new AtomicInteger(0);
    RecordingSampler sampler = new RecordingSampler();
    BloomFilterAPI api =
        (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> {
          calls.incrementAndGet();
          throw new ClientErrorException(Response.status(401).build());
        };

    MetricBloomFilterRefresher refresher =
        new MetricBloomFilterRefresher(
            api, UUID.randomUUID(), "Bearer test", "CUSTOMER_SERIES", 5, 3, sampler);

    refresher.refresh();
    refresher.refresh();

    assertEquals(2, calls.get());
    assertEquals(0, sampler.updateCount);
  }

  @Test
  public void testRefreshHandlesRuntimeExceptionWithoutDisabling() {
    AtomicInteger calls = new AtomicInteger(0);
    RecordingSampler sampler = new RecordingSampler();
    BloomFilterAPI api =
        (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> {
          calls.incrementAndGet();
          throw new RuntimeException("boom");
        };

    MetricBloomFilterRefresher refresher =
        new MetricBloomFilterRefresher(
            api, UUID.randomUUID(), "Bearer test", "CUSTOMER_SERIES", 5, 3, sampler);

    refresher.refresh();
    refresher.refresh();

    assertEquals(2, calls.get());
    assertEquals(0, sampler.updateCount);
  }

  @Test
  public void testConstructorStoresProvidedValues() throws Exception {
    MetricBloomFilterRefresher refresher =
        new MetricBloomFilterRefresher(
            (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> new BloomFilterDTO(),
            UUID.randomUUID(),
            "Bearer test",
            "CUSTOMER_SERIES",
            1,
            1,
            new RecordingSampler());

    assertEquals(MetricBloomFilterRefresher.DEFAULT_REFRESH_MINUTES, getIntField(refresher, "refreshMinutes"));
    assertEquals(MetricBloomFilterRefresher.DEFAULT_LOOKBACK_DAYS, getIntField(refresher, "lookbackDays"));
  }

  @Test
  public void testConstructorsSetBloomFilterName() throws Exception {
    MetricBloomFilterRefresher withDefaultName =
        new MetricBloomFilterRefresher(
            (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> new BloomFilterDTO(),
            UUID.randomUUID(),
            "Bearer test",
            5,
            2,
            new RecordingSampler());

    assertEquals("CUSTOMER_SERIES", getStringField(withDefaultName, "bloomFilterName"));

    MetricBloomFilterRefresher withExplicitName =
        new MetricBloomFilterRefresher(
            (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> new BloomFilterDTO(),
            UUID.randomUUID(),
            "Bearer test",
            "EXPLICIT_FILTER",
            5,
            2,
            new RecordingSampler());

    assertEquals("EXPLICIT_FILTER", getStringField(withExplicitName, "bloomFilterName"));
  }

  @Test
  public void testStartAndShutdownLifecycle() throws Exception {
    AtomicInteger calls = new AtomicInteger(0);
    RecordingSampler sampler = new RecordingSampler();
    BloomFilterAPI api =
        (proxyId, authorization, epochDay, lookbackDays, bloomFilterName) -> {
          calls.incrementAndGet();
          return new BloomFilterDTO();
        };

    MetricBloomFilterRefresher refresher =
        new MetricBloomFilterRefresher(
            api, UUID.randomUUID(), "Bearer test", "CUSTOMER_SERIES", 5, 2, sampler);

    refresher.start();
    try {
      long deadlineMillis = System.currentTimeMillis() + 2000;
      while (System.currentTimeMillis() < deadlineMillis && calls.get() == 0) {
        Thread.sleep(20);
      }
      assertTrue(calls.get() > 0);
    } finally {
      refresher.shutdown();
    }

    ScheduledExecutorService executor = getExecutor(refresher);
    assertNotNull(executor);
    assertTrue(executor.isShutdown());
    assertFalse(executor.isTerminated() && calls.get() == 0);
  }

  private static int getIntField(Object target, String fieldName) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.getInt(target);
  }

  private static String getStringField(Object target, String fieldName) throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    return (String) field.get(target);
  }

  private static ScheduledExecutorService getExecutor(MetricBloomFilterRefresher refresher) throws Exception {
    Field field = MetricBloomFilterRefresher.class.getDeclaredField("executor");
    field.setAccessible(true);
    return (ScheduledExecutorService) field.get(refresher);
  }

  private static final class RecordingSampler extends MetricBloomFilterSampler {
    private int updateCount;
    private BloomFilterDTO lastBloomFilterDTO;

    @Override
    public void updateBloomFilters(BloomFilterDTO bloomFilterDTO) {
      updateCount++;
      lastBloomFilterDTO = bloomFilterDTO;
    }
  }
}
