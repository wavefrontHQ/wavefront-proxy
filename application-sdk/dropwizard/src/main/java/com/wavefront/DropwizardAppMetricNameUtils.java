package com.wavefront;

import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.model.Resource;

import javax.annotation.Nullable;
import javax.ws.rs.container.ContainerResponseContext;

import okhttp3.Request;

/**
 * A utils class to generate metric name for Dropwizard (Jersey) apps requests/responses
 *
 * @author Sushant Dewan (sushant@wavefront.com).
 */
public abstract class DropwizardAppMetricNameUtils {

  private static final String REQUEST_PREFIX = "request";
  private static final String RESPONSE_PREFIX = "response";

  /**
   * Util to generate metric name from the jersey container request
   * Will return null if no resource is matched for the URL
   *
   * @param request           jersey container request
   * @return generated metric name from the jersey container request
   */
  @Nullable
  public static String metricName(ContainerRequest request) {
    return metricName(request, REQUEST_PREFIX);
  }

  /**
   *
   * Util to generate metric name from the jersey container response
   * Will return null if no resource is matched for the URL
   *
   * @param request            jersey container request
   * @param response           jersey container response
   * @return generated metric name from the jersey container request/response
   */
  @Nullable
  public static String metricName(ContainerRequest request, ContainerResponseContext response) {
    String metricName = metricName(request, RESPONSE_PREFIX);
    return metricName == null ? null : metricName + "." + response.getStatus();
  }

  @Nullable
  private static String metricName(ContainerRequest request, String prefix) {
    Resource matchedResource = request.getUriInfo().getMatchedModelResource();
    if (matchedResource != null) {
      String matchingPath = stripLeadingAndTrailingSlashes(matchedResource.getPath());
      // prepend the path for every parent
      while (matchedResource.getParent() != null) {
        matchedResource = matchedResource.getParent();
        matchingPath = stripLeadingAndTrailingSlashes(matchedResource.getPath()) + "/" +
            matchingPath;
      }
      return prefix + "." + metricName(request.getMethod(), matchingPath);
    }
    return null;
  }

  /**
   * Accept a resource method and extract the path and turns slashes into dots to be more
   * metric friendly. Might return empty metric name if all the original characters in the string
   * are not metric friendly
   *
   * @param httpMethod     Jersey API HTTP request method
   * @param path           Jersey API request relative path
   * @return generated metric name from the original request
   */
  private static String metricName(String httpMethod, String path) {
    String metricId = stripLeadingAndTrailingSlashes(path);
    // prevents metrics from trying to create object names with weird characters
    // swagger-ui introduces a route: api-docs/{route: .+} and the colon must be removed
    metricId = metricId.replace('/', '.').replace(":", "").
        replace("{", "_").replace("}", "_");
    if (StringUtils.isBlank(metricId)) {
      return "";
    }

    metricId += "." + httpMethod;
    return metricId;
  }

  private static String stripLeadingAndTrailingSlashes(String path) {
    if (path == null) {
      return "";
    } else {
      return StringUtils.strip(path, "/");
    }
  }
}
