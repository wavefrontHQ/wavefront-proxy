package com.wavefront.agent.logforwarder.ingestion.client.gateway.model;

import com.wavefront.agent.logforwarder.ingestion.client.gateway.utils.Utils;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a general HTTP request and response model with
 * Logs gateway
 */
@SuppressWarnings("unchecked")
public abstract class GatewayOperation<T extends GatewayOperation<T>> {
    public static final String CR_LF = "\r\n";
    private static final AtomicLong idCounter = new AtomicLong();
    protected final long id;
    protected URI uri;
    protected Object body;
    protected String contentType;
    protected GatewayOperation.Action action;
    protected final Map<String, String> headers;

    public GatewayOperation(long id) {
        this.id = id;
        this.headers = new HashMap();
        this.contentType = "application/json";
    }

    public GatewayOperation() {
        this(idCounter.incrementAndGet());
    }

    public T setBody(Object body) {
        if (body != null) {
            this.body = Utils.clone(body);
        } else {
            this.body = null;
        }

        return (T) this;
    }

    public T setContentType(String contentType) {
        this.contentType = contentType;
        return (T) this;
    }

    public T setUri(URI uri) {
        this.uri = uri;
        return (T) this;
    }

    public T setAction(GatewayOperation.Action action) {
        this.action = action;
        return (T) this;
    }

    public T addHeaders(Map<String, String> headers) {
        if (headers != null && !headers.isEmpty()) {
            headers.entrySet().forEach((entry) -> {
                this.addHeader((String)entry.getKey(), (String)entry.getValue(), true);
            });
            return (T) this;
        } else {
            return (T) this;
        }
    }

    public T addHeader(String name, String value) {
        this.addHeader(name, value, true);
        return (T) this;
    }

    private T addHeader(String name, String value, boolean normalize) {
        if (normalize) {
            value = this.removeString(value, "\r\n").trim();
            name = name.toLowerCase();
        }

        this.headers.put(name, value);
        return (T) this;
    }

    private String removeString(String value, String delete) {
        return value.replaceAll(delete, "").trim();
    }

    public T removeHeader(String name) {
        this.headers.remove(name);
        return (T) this;
    }

    public T setBodyNoCloning(Object body) {
        this.body = body;
        return (T) this;
    }

    public URI getUri() {
        return this.uri;
    }

    public String getHeader(String name) {
        return this.getHeader(name, true);
    }

    private String getHeader(String name, boolean normalize) {
        String value = (String)this.headers.get(name);
        if (!normalize) {
            return value;
        } else {
            if (value == null) {
                value = (String)this.headers.get(name.toLowerCase());
                if (value == null) {
                    return null;
                }
            }

            return this.removeString(value.trim(), "\r\n");
        }
    }

    public String getContentType() {
        return this.contentType;
    }

    public GatewayOperation.Action getAction() {
        return this.action;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
    }

    public Object getBody() {
        return this.body;
    }

    public long getId() {
        return this.id;
    }

    public static enum Action {
        GET,
        POST,
        PATCH,
        PUT,
        DELETE,
        OPTIONS;

        private Action() {
        }
    }
}
