package com.wavefront.agent.logforwarder.ingestion.client.gateway.model;

import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * @author Manoj Ramakrishnan (rmanoj@vmware.com).
 * @since 9/17/21 4:28 PM
 */
public class ServiceErrorResponse {
    public static final String PROPERTY_NAME_DISABLE_STACK_TRACE_COLLECTION = "xenon.ServiceErrorResponse.disableStackTraceCollection";
    public static final boolean DISABLE_STACK_TRACE_COLLECTION = Boolean.getBoolean("xenon.ServiceErrorResponse.disableStackTraceCollection");
    public static final int ERROR_CODE_INTERNAL_MASK = -2147483648;
    public static final int ERROR_CODE_OUTDATED_SYNCH_REQUEST = -2147483647;
    public static final int ERROR_CODE_STATE_MARKED_DELETED = -2147483646;
    public static final int ERROR_CODE_SERVICE_ALREADY_EXISTS = -2147483645;
    public static final int ERROR_CODE_OWNER_MISMATCH = -2147483643;
    public static final int ERROR_CODE_SERVICE_QUEUE_LIMIT_EXCEEDED = -2147483641;
    public static final int ERROR_CODE_HOST_RATE_LIMIT_EXCEEDED = -2147483640;
    public static final int ERROR_CODE_CLIENT_QUEUE_LIMIT_EXCEEDED = -2147483639;
    public static final int ERROR_CODE_EXTERNAL_AUTH_FAILED = -2147483632;
    public static final String KIND = Utils.buildKind(ServiceErrorResponse.class);
    public String message;
    public String messageId;
    public List<String> stackTrace;
    public int statusCode;
    public EnumSet<ErrorDetail> details;
    public String documentKind;
    protected int errorCode;

    public ServiceErrorResponse() {
      this.documentKind = KIND;
    }

    public static ServiceErrorResponse create(Throwable e, int statusCode) {
      return create(e, statusCode, (EnumSet)null);
    }

    public static ServiceErrorResponse createWithShouldRetry(Throwable e) {
      return create(e, 400, EnumSet.of(ServiceErrorResponse.ErrorDetail.SHOULD_RETRY));
    }

    public static ServiceErrorResponse create(Throwable e, int statusCode, EnumSet<ServiceErrorResponse.ErrorDetail> details) {
      ServiceErrorResponse rsp = new ServiceErrorResponse();
      rsp.message = e.getLocalizedMessage();
      if (!DISABLE_STACK_TRACE_COLLECTION) {
        rsp.stackTrace = new ArrayList();
        StackTraceElement[] var4 = e.getStackTrace();
        int var5 = var4.length;

        for(int var6 = 0; var6 < var5; ++var6) {
          StackTraceElement se = var4[var6];
          rsp.stackTrace.add(se.toString());
        }
      }

      rsp.details = details;
      rsp.statusCode = statusCode;
      return rsp;
    }

    private static boolean isInternalErrorCode(int errorCode) {
      return (errorCode & -2147483648) != 0;
    }

    public int getErrorCode() {
      return this.errorCode;
    }

    public void setErrorCode(int errorCode) {
      if (isInternalErrorCode(errorCode)) {
        throw new IllegalArgumentException("Error code must not use internal xenon errorCode range.");
      } else {
        this.errorCode = errorCode;
      }
    }

    public void setInternalErrorCode(int errorCode) {
      if (!isInternalErrorCode(errorCode)) {
        throw new IllegalArgumentException("Error code must use internal xenon errorCode range.");
      } else {
        this.errorCode = errorCode;
      }
    }

    public static enum ErrorDetail {
      SHOULD_RETRY;

      private ErrorDetail() {
      }
    }
}
