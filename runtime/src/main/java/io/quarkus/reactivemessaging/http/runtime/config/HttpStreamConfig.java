package io.quarkus.reactivemessaging.http.runtime.config;

import java.util.Locale;

import io.vertx.core.http.HttpMethod;

public class HttpStreamConfig extends StreamConfigBase {
    private final HttpMethod method;

    public HttpStreamConfig(String path, String method, String name, int bufferSize, String deserializerName) {
        super(bufferSize, path, deserializerName);
        this.method = toHttpMethod(method, name);
    }

    public HttpMethod method() {
        return method;
    }

    private HttpMethod toHttpMethod(String method, String connectorName) {
        try {
            return HttpMethod.valueOf(method.toUpperCase(Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Invalid http method '" + method + "' defined for connector " + connectorName);
        }
    }
}
