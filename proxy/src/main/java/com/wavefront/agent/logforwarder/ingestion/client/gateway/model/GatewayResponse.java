/*
 * Copyright (c) 2016 VMware, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, without warranties or
 * conditions of any kind, EITHER EXPRESS OR IMPLIED.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.wavefront.agent.logforwarder.ingestion.client.gateway.model;

import com.google.gson.JsonSyntaxException;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.serialization.KryoSerializers;
import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;
import java.net.URI;
import java.util.concurrent.TimeoutException;

/**
 * This class represents a HTTP response from Saas Lgg GatewayClient
 */

public class GatewayResponse extends GatewayOperation<GatewayResponse> {
    private int statusCode;
    protected Throwable failure;

    private GatewayResponse() {
    }

    private GatewayResponse(long id) {
        super(id);
    }

    public static GatewayResponse createResponse(Action action, URI uri) {
        GatewayResponse response = new GatewayResponse();
        response.action = action;
        response.uri = uri;
        return response;
    }

    public static GatewayResponse createResponse(Action action, URI uri, long id) {
        GatewayResponse response = new GatewayResponse(id);
        response.action = action;
        response.uri = uri;
        return response;
    }

    public GatewayResponse setStatusCode(int statusCode) {
        this.statusCode = statusCode;
        if (statusCode <= 400) {
            this.failure = null;
        }

        return this;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public <T> T getBody(Class<T> type) {
        if (this.body != null && this.body.getClass() == type) {
            return (T) this.body;
        } else if (this.body != null && !(this.body instanceof String)) {
            if (this.contentType != null && Utils.isContentTypeKryoBinary(this.contentType) && this.body instanceof byte[]) {
                byte[] bytes = (byte[])((byte[])this.body);
                this.body = KryoSerializers.deserializeDocument(bytes, 0, bytes.length);
                return (T) this.body;
            } else if (this.contentType != null && this.contentType.contains("application/json")) {
                String json = Utils.toJson(this.body);
                return Utils.fromJson(json, type);
            } else {
                throw new IllegalStateException("content type is not JSON: " + this.contentType);
            }
        } else if (this.body != null) {
            if (this.contentType != null && this.contentType.contains("application/json")) {
                try {
                    this.body = Utils.fromJson(this.body, type);
                } catch (JsonSyntaxException var3) {
                    throw new IllegalArgumentException("Unparseable JSON body: " + var3.getMessage(), var3);
                }

                return (T) this.body;
            } else {
                throw new IllegalArgumentException("Unrecognized Content-Type for parsing request body: " + this.contentType);
            }
        } else {
            throw new IllegalStateException();
        }
    }

    public void fail(int statusCode) {
        this.setStatusCode(statusCode);
        switch(statusCode) {
            case 403:
                this.fail(new IllegalAccessError("forbidden"));
                break;
            case 408:
                this.fail(new TimeoutException());
                break;
            default:
                this.fail(new Exception("request failed with " + statusCode + ", no additional details provided"));
        }

    }

    public void fail(Throwable e) {
        this.fail(e, (Object)null);
    }

    public void fail(int statusCode, Throwable e, Object failureBody) {
        this.statusCode = statusCode;
        this.fail(e, failureBody);
    }

    public void fail(Throwable e, Object failureBody) {
        if (this.statusCode < 400) {
            this.statusCode = 500;
        }

        if (e instanceof TimeoutException) {
            this.statusCode = 408;
        }

        if (failureBody != null) {
            this.setBodyNoCloning(failureBody);
        }

        boolean hasErrorResponseBody = false;
        GatewayResponse.GatewayErrorResponse rsp;
        if (this.body != null && this.body instanceof String) {
            if ("application/json".equals(this.contentType)) {
                try {
                    rsp = (GatewayResponse.GatewayErrorResponse)Utils.fromJson(this.body, GatewayResponse.GatewayErrorResponse.class);
                    if (rsp.message != null) {
                        hasErrorResponseBody = true;
                    }
                } catch (Exception var6) {
                }
            } else {
                hasErrorResponseBody = true;
            }
        }

        if (this.body != null && this.body instanceof byte[]) {
            hasErrorResponseBody = true;
        }

        if (this.body == null || !hasErrorResponseBody && !(this.body instanceof GatewayResponse.GatewayErrorResponse)) {
            if (Utils.isValidationError(e)) {
                this.statusCode = 400;
                rsp = GatewayResponse.GatewayErrorResponse.create(e, this.statusCode);
            } else {
                rsp = GatewayResponse.GatewayErrorResponse.create(e, 400);
            }

            rsp.statusCode = this.statusCode;
            ((GatewayResponse)this.setBodyNoCloning(rsp)).setContentType("application/json");
        }

        this.failure = e;
    }

    public Throwable getFailure() {
        return this.failure;
    }

    public interface GatewayResponseHandler {
        void handle(GatewayResponse var1, Throwable var2);
    }

    public static class GatewayErrorResponse {
        private String message;
        private int errorCode;
        private int statusCode;

        private GatewayErrorResponse() {
        }

        public static GatewayResponse.GatewayErrorResponse create(Throwable e, int statusCode) {
            GatewayResponse.GatewayErrorResponse rsp = new GatewayResponse.GatewayErrorResponse();
            rsp.message = e.getLocalizedMessage();
            rsp.statusCode = statusCode;
            return rsp;
        }

        public String getMessage() {
            return this.message;
        }

        public int getErrorCode() {
            return this.errorCode;
        }

        public void setErrorCode(int errorCode) {
            this.errorCode = errorCode;
        }

        public int getStatusCode() {
            return this.statusCode;
        }
    }
}
