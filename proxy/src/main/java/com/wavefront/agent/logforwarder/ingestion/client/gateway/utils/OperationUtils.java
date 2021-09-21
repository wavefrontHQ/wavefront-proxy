//package com.wavefront.agent.logforwarder.ingestion.client.gateway.utils;
//
//import com.wavefront.agent.logforwarder.ingestion.client.gateway.model.ServiceErrorResponse;
//
//import org.apache.commons.lang3.StringUtils;
//
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.EnumSet;
//import java.util.Map;
//import java.util.Objects;
//import java.util.function.BiConsumer;
//import java.util.function.Function;
//import java.util.logging.Level;
//
///**
// * @author Manoj Ramakrishnan (rmanoj@vmware.com).
// * @since 9/17/21 4:49 PM
// */
//  public class OperationUtils {
//
//    private static final String URI_LEGAL_CHARS = "-_.~?#[]@!$&'()*+,;=:";
//
//    private OperationUtils() {
//    }
//
//    public static ServiceErrorResponse createServiceErrorResponse(
//        int statusCode, String msgFormat, Object... args) {
//
//      ServiceErrorResponse rsp = new ServiceErrorResponse();
//      rsp.message = String.format(msgFormat, args);
//      rsp.statusCode = statusCode;
//      return rsp;
//    }
//
//    /**
//     * Fails a request. The http status code of the request is set to
//     * the passed-in status code. The response body is set to an object
//     * of type {@link ServiceErrorResponse} created based on passed message args.
//     */
//    public static void failRequest(Operation op, int statusCode, String msgFormat, Object... args) {
//      failRequest(op, createServiceErrorResponse(statusCode, msgFormat, args));
//    }
//
//    /**
//     * Fails the request with status code 405 and a message mentioning that the action
//     * is not supported
//     */
//    public static void failRequest(Operation op, ServiceErrorResponse rsp) {
//      op.fail(rsp.statusCode, new IllegalStateException(rsp.message), rsp);
//    }
//
//    /**
//     * Very similar to {@link #failRequest(Operation, int, String, Object...)}... only this method
//     * accepts a {@code runnable} parameter to allow code to run before failing the operation.
//     */
//    public static void failRequestActionNotSupported(Operation request) {
//      failRequestActionNotSupported(request, null);
//    }
//
//    /**
//     * Fails the request with status code 405 and a message mentioning that the action
//     * is not supported. Also returns a response header containing the allowed headers.
//     */
//    public static void failRequestActionNotSupported(
//        Operation request, EnumSet<Service.Action> allowedActions) {
//      if (allowedActions != null) {
//        request.addResponseHeader("Allow", allowedActions.toString());
//      }
//      OperationUtils.failRequest(request,
//          Operation.STATUS_CODE_BAD_METHOD, "Action not supported: ", request.getAction());
//    }
//
//    /**
//     * Helper method to return a message associated with {@code t}. If {@code t.getMessage()} is
//     * non-empty, return that message. Otherwise, return the stack trace associated with {@code t}
//     *
//     * @param t the error to return String details about
//     * @return a String capturing the details of {@code t}
//     */
//    public static String getThrowableMessage(Throwable t) {
//      if (t.getMessage() != null && t.getMessage().trim() != "") {
//        return t.getMessage();
//      }
//      return Utils.toString(t);
//    }
//
//    /**
//     * This method mimics the UriUtils.isValidDocumentId() in xenon 1.6.0 for validating
//     * document ids before creation. Creating it here as we are still in xenon 1.5.x
//     *
//     * @param suffix id of the document
//     * @param factoryLink link to factory under which document is being created
//     * @return
//     */
//    public static boolean isValidDocumentId(String suffix, String factoryLink) {
//      // Skip validation for core services
//      if (suffix.startsWith(ServiceUriPaths.CORE + UriUtils.URI_PATH_CHAR)) {
//        return true;
//      }
//
//      int index = 0;
//      if (UriUtils.isChildPath(suffix, factoryLink)) {
//        index = factoryLink.length() + 1;
//      }
//
//      final int len = suffix.length();
//      if (index < len && suffix.charAt(index) == UriUtils.URI_PATH_CHAR.charAt(0)) {
//        index = index + 1;
//      }
//
//      if (index >= len) {
//        return false;
//      }
//
//      for (int i = index; i < len; i++) {
//        char ch = suffix.charAt(i);
//        if (ch >= '0' && ch <= '9') {
//          continue;
//        }
//        if (ch >= 'a' && ch <= 'z') {
//          continue;
//        }
//        if (ch >= 'A' && ch <= 'Z') {
//          continue;
//        }
//        if (URI_LEGAL_CHARS.indexOf(ch) >= 0) {
//          continue;
//        }
//        return false;
//      }
//
//      return true;
//    }
//
//    public static <T extends Document> BiConsumer<T, Throwable> updateHandler(Operation op, StatelessService service) {
//      return (updated, t) -> {
//        if (t != null) {
//          service.logSevere(t);
//          op.fail(t);
//          return;
//        }
//        updated.kind = Utils.buildKind(updated.getClass());
//        op.setBody(updated).complete();
//      };
//    }
//
//    public static <T> BiConsumer<T, Throwable> deleteHandler(Operation op, StatelessService service) {
//      return (updated, t) -> {
//        if (t != null) {
//          service.logSevere(t);
//          op.fail(t);
//          return;
//        }
//        op.setBody(updated).complete();
//      };
//    }
//
//    public static <T> Function<Throwable, T> exceptionHandler(Operation op, StatelessService service) {
//      return t -> {
//        if (t != null) {
//          service.logSevere(t);
//          op.fail(t);
//        }
//        return null;
//      };
//    }
//
//    public static boolean isLemansDocumentSyncRequest(Operation operation) {
//      return operation.hasPragmaDirective(Operation.PRAGMA_DIRECTIVE_FROM_MIGRATION_TASK);
//    }
//
//    /**
//     * Custom equals which checks for equality only if input is not null
//     * if input is null, it returns true
//     */
//    public static boolean equalsOnMerge(Object existing, Object input) {
//      if (input == null) {
//        return true;
//      }
//      return Objects.equals(existing, input);
//    }
//
//    public static String toString(String baseMsg, Operation op, Throwable ex) {
//      StringBuilder sb = new StringBuilder(baseMsg);
//      if (op != null) {
//        sb.append("\nOperation: ")
//            .append(op.getAction()).append(" ").append(op.getUri())
//            .append(" - status:").append(op.getStatusCode());
//      }
//      if (ex != null) {
//        sb.append("\nException: ")
//            .append(Utils.toString(ex));
//      }
//      return sb.toString();
//    }
//
//    public static URI createNewUri(URI baseUri, String extraPath, Operation inboundOp, Service hostService) {
//      URI inboundUri = inboundOp.getUri();
//      return createNewUri(baseUri, extraPath, inboundUri.getQuery(), inboundUri.getUserInfo(), inboundUri.getFragment(), hostService);
//    }
//
//    public static URI createNewUri(URI baseUri, String extraPath, String query, Service hostService) {
//      return createNewUri(baseUri, extraPath, query, null, null, hostService);
//    }
//
//    public static URI createNewUri(URI baseUri, String extraPath, String query, String userInfo, String fragment, Service hostService) {
//      URI uri = baseUri.getHost() != null ? baseUri : UriUtils.buildUri(hostService.getHost(), baseUri.getPath());
//      String path = baseUri.getPath() != null ? baseUri.getPath() : "";
//      if (extraPath != null) {
//        path = UriUtils.buildUriPath(path, extraPath);
//      }
//      try {
//        return new URI(
//            uri.getScheme(),
//            userInfo,
//            uri.getHost(),
//            uri.getPort(),
//            path,
//            query,
//            fragment);
//      } catch (URISyntaxException x) {
//        hostService.getHost().log(
//            Level.SEVERE,
//            "%s failed to create uri for [uri=%s] [path=%s] [query=%s] [userInfo=%s] [fragment=%s]",
//            hostService.getClass().getSimpleName(), uri, path, query, userInfo, fragment);
//        throw new IllegalArgumentException(x.getMessage(), x);
//      }
//    }
//
//    /**
//     * Retrieves the value of the HTTP header associated with {@code headerKey}. If the header key
//     * is not found, it will search for a match using {@code headerKey.toLowerCase()} and {@code
//     * StringUtils.capitalize(headerKey)}.
//     *
//     * @return the HTTP header value matching {@code headerKey} using method described above;
//     * Returns {@code null} if a match is never found.
//     */
//    public static String getHeader(Map<String, String> httpHeaders, String headerKey) {
//      String value = null;
//      if (httpHeaders != null && headerKey != null) {
//        value = httpHeaders.get(headerKey);
//        value = value == null ? httpHeaders.get(headerKey.toLowerCase()) : value;
//        value = value == null ? httpHeaders.get(StringUtils.capitalize(headerKey)) : value;
//      }
//      return value;
//    }
//
//    /**
//     * Creates and sends a empty patch request for the @{@link Service}
//     * passed to it.
//     * @param service
//     */
//    public static void sendEmptyPatchRequestForService(Service service) {
//      try {
//        Operation patchOp = Operation
//            .createPatch(service.getHost(), service.getSelfLink())
//            .setBodyNoCloning(new ServiceDocument())
//            .setReferer(service.getSelfLink())
//            .setCompletion((o, e) -> {
//              if (e != null) {
//                service.getHost().log(Level.SEVERE, "Patch request to %s failed with %s",
//                    service.getSelfLink(), e.toString());
//                return;
//              }
//            });
//        service.sendRequest(patchOp);
//      } catch (Throwable e) {
//        service.getHost().log(Level.SEVERE, "Failure while invoking patch on service due to %s",
//            Utils.toString(e));
//      }
//
//    }
//  }
//}
