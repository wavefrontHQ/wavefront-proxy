package com.wavefront.dropwizard.app;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.wavefront.DropwizardApplicationReporter;
import com.wavefront.WavefrontJerseyFilter;

import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;

public class SampleApp extends Application<Configuration> {

  private final LoadingCache<String, AtomicInteger> cache = Caffeine.newBuilder().
      build(key -> new AtomicInteger(0));

  @Override
  public void run(Configuration configuration, Environment environment) {
    environment.jersey().register(new SampleResource());
    environment.getApplicationContext().setContextPath("/sample");
    environment.jersey().register(new WavefrontJerseyFilter(new DropwizardApplicationReporter() {
      @Override
      public void incrementCounter(String metricName) {
        cache.get(metricName).incrementAndGet();
      }

      @Override
      public void updateHistogram(String metricName, long latencyMillis) {
        cache.get(metricName).incrementAndGet();
      }
    }));
  }

  public int reportedValue(String metricName) {
    return cache.get(metricName).get();
  }

  @Path("/sample/foo")
  @Produces(MediaType.TEXT_PLAIN)
  public class SampleResource {

    // CRUD operations

    // C => create
    @POST
    @Path("/bar")
    public void barCreate() {
      // no-op
    }

    // R => read
    @GET
    @Path("/bar/{id}")
    public String barGet() {
      return "don't care";
    }

    // R => getAll
    @GET
    @Path("/bar")
    public String getAll() {
      return "don't care";
    }

    // U => update
    @PUT
    @Path("/bar/{id}")
    public void barUpdate() {
      // no-op
    }

    // D => delete
    @DELETE
    @Path("/bar/{id}")
    public void barDelete() {
      // no-op
    }
  }
}
