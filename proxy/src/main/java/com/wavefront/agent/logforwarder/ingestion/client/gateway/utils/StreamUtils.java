package com.wavefront.agent.logforwarder.ingestion.client.gateway.utils;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 10:03 AM
 */

import java.net.URI;

import com.vmware.xenon.common.UriUtils;

public class StreamUtils {
  private StreamUtils() {
  }

  public static String getStreamName(URI uri) {
    return getStreamName(uri.getPath());
  }

  public static String getStreamName(String path) {
    String streamPath = path.substring("/le-mans/v1/streams".length() + 1);
    if (streamPath.indexOf(47) > 0) {
      streamPath = streamPath.substring(0, streamPath.indexOf(47));
    }

    return streamPath;
  }

  public static String getStreamName(URI uri, String prefixLink) {
    return getStreamName(uri.getPath(), prefixLink);
  }

  public static String getStreamName(String path, String prefixLink) {
    String streamPath = path.substring(UriUtils.buildUriPath(new String[]{prefixLink, "/streams"}).length() + 1);
    if (streamPath.indexOf(47) > 0) {
      streamPath = streamPath.substring(0, streamPath.indexOf(47));
    }

    return streamPath;
  }
}