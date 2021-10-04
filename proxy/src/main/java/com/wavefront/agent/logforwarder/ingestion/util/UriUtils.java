package com.wavefront.agent.logforwarder.ingestion.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 4:22 PM
 */
public class UriUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(UriUtils.class);
  public static final String URI_PATH_CHAR = "/";
  private static final char URI_PATH_CHAR_CONST = '/';
  private static final char URI_QUERY_CHAR_CONST = '?';
  private static final char URI_QUERY_PARAM_LINK_CHAR_CONST = '&';
  private static final char URI_QUERY_PARAM_KV_CHAR_CONST = '=';

  private UriUtils() {
  }

  public static String buildUriPath(String... segments) {
    if (segments.length == 1) {
      return normalizeUriPath(segments[0]);
    } else {
      StringBuilder sb = new StringBuilder();
      String[] var2 = segments;
      int var3 = segments.length;

      for(int var4 = 0; var4 < var3; ++var4) {
        String s = var2[var4];
        normalizeUriPathTo(s, sb);
      }

      return sb.toString();
    }
  }

  public static URI buildUri(String uri) {
    try {
      return new URI(uri);
    } catch (Exception var2) {
      throw new IllegalStateException(var2);
    }
  }

  public static String normalizeUriPath(String path) {
    if (path == null) {
      return "";
    } else if (path.isEmpty()) {
      return path;
    } else {
      if (path.endsWith("/")) {
        path = path.substring(0, path.length() - 1);
      }

      return path.startsWith("/") ? path : "/" + path;
    }
  }

  private static void normalizeUriPathTo(String path, StringBuilder sb) {
    if (path != null && !path.isEmpty()) {
      if (path.charAt(0) != '/') {
        sb.append('/');
      }

      sb.append(path);
      int lastIndex = sb.length() - 1;
      if (sb.charAt(lastIndex) == '/') {
        sb.setLength(lastIndex);
      }

    }
  }

  public static URI createURIFromSourceAndPort(String source, int port) {
    return URI.create("http://" + source + ":" + port);
  }

  public static URI createURI(String url) {
    URI uri = null;

    try {
      uri = new URI(url);
      return uri;
    } catch (Exception var3) {
      return null;
    }
  }

  public static URI appendQueryParam(URI uri, String param, String value) {
    return extendUriWithQuery(uri, param, value);
  }

  public static URI extendUri(URI uri, String path) {
    String query = null;
    if (path != null) {
      int indexOfFirstQueryChar = path.indexOf(63);
      if (indexOfFirstQueryChar >= 0) {
        if (indexOfFirstQueryChar < path.length() - 1) {
          query = path.substring(indexOfFirstQueryChar + 1);
        }

        path = path.substring(0, indexOfFirstQueryChar);
      }
    }

    return buildUri(uri.getScheme(), uri.getHost(), uri.getPort(), buildUriPath(uri.getPath(), path), query);
  }

  public static URI extendUriWithQuery(URI u, String... keyValues) {
    String query = u.getQuery();
    if (query != null && !query.isEmpty()) {
      query = query + '&' + buildUriQuery(keyValues);
    } else {
      query = buildUriQuery(keyValues);
    }

    try {
      return new URI(u.getScheme(), (String)null, u.getHost(), u.getPort(), u.getPath(), query, (String)null);
    } catch (URISyntaxException var4) {
      LOGGER.warn("Failed to extend URI" + var4.getMessage());
      return null;
    }
  }

  public static String buildUriQuery(String... keyValues) {
    if (keyValues.length % 2 != 0) {
      throw new IllegalArgumentException("keyValues array length must be even, with key and value pairs interleaved");
    } else {
      StringBuilder sb = new StringBuilder();
      boolean doKey = true;
      boolean isFirst = true;
      String[] var4 = keyValues;
      int var5 = keyValues.length;

      for(int var6 = 0; var6 < var5; ++var6) {
        String s = var4[var6];
        if (doKey) {
          if (!isFirst) {
            sb.append('&');
          } else {
            isFirst = false;
          }

          sb.append(s).append('=');
        } else {
          sb.append(s);
        }

        doKey = !doKey;
      }

      return sb.toString();
    }
  }

  public static URI buildUri(URI baseUri, String... path) {
    if (path != null && path.length != 0) {
      String query = null;
      StringBuilder buildPath = null;
      String[] var4 = path;
      int var5 = path.length;

      for(int var6 = 0; var6 < var5; ++var6) {
        String p = var4[var6];
        if (p != null) {
          int indexOfFirstQueryChar = p.indexOf(63);
          if (indexOfFirstQueryChar >= 0) {
            if (indexOfFirstQueryChar < p.length() - 1) {
              String curQuery = p.substring(indexOfFirstQueryChar + 1);
              if (query == null) {
                query = curQuery;
              } else {
                query = query + curQuery;
              }
            }

            p = p.substring(0, indexOfFirstQueryChar);
          }

          if (buildPath == null) {
            buildPath = new StringBuilder();
          }

          normalizeUriPathTo(p, buildPath);
        }
      }

      try {
        return (new URI(baseUri.getScheme(), baseUri.getUserInfo(), baseUri.getHost(), baseUri.getPort(), buildPath == null ? null : buildPath.toString(), query, (String)null)).normalize();
      } catch (Exception var10) {
        LOGGER.warn(String.format("Failure building uri %s, %s: %s", baseUri, path, var10.getMessage()));
        return null;
      }
    } else {
      return baseUri;
    }
  }

  public static URI buildUri(String scheme, String host, int port, String path, String query) {
    return buildUri(scheme, host, port, path, query, (String)null);
  }

  public static URI buildUri(String scheme, String host, int port, String path, String query, String userInfo) {
    try {
      if (path != null) {
        int indexOfFirstQueryChar = path.indexOf(63);
        if (indexOfFirstQueryChar >= 0) {
          if (indexOfFirstQueryChar < path.length() - 1) {
            query = path.substring(indexOfFirstQueryChar + 1);
          }

          path = path.substring(0, indexOfFirstQueryChar);
        }
      }

      return (new URI(scheme, userInfo, host, port, normalizeUriPath(path), query, (String)null)).normalize();
    } catch (URISyntaxException var7) {
      LOGGER.warn(String.format("Failure building uri %s", var7.getMessage()));
      return null;
    }
  }

  public static String getLastPathSegment(URI uri) {
    String path = uri.getPath();
    return getLastPathSegment(path);
  }

  public static String getLastPathSegment(String link) {
    if (link == null) {
      throw new IllegalArgumentException("link is required");
    } else {
      return link.endsWith("/") ? "" : link.substring(link.lastIndexOf("/") + 1);
    }
  }

  public static String createURI(URI address, String extraPath) {
    return extendUri(address, extraPath).toString();
  }

  public static String createURI(URI address, String extraPath, String query) {
    return createURI(address, extraPath, query, address.getUserInfo(), address.getFragment());
  }

  public static String createUriFromExistingUri(URI address, String extraPath, URI inUri) {
    return createURI(address, extraPath, inUri.getQuery(), inUri.getUserInfo(), inUri.getFragment());
  }

  public static String createURI(URI address, String extraPath, String query, String userInfo, String fragment) {
    URI uri = extendUri(address, extraPath);

    try {
      return (new URI(uri.getScheme(), userInfo, uri.getHost(), uri.getPort(), uri.getPath(), query, fragment)).toString();
    } catch (URISyntaxException var7) {
      LOGGER.info("Exception occurred in creating URI: " + var7.getMessage());
      return "";
    }
  }
}
