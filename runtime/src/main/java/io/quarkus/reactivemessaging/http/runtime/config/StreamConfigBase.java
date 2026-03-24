package io.quarkus.reactivemessaging.http.runtime.config;

public class StreamConfigBase {
    private final int bufferSize;
    private final String path;
    private final String deserializerName;

    public StreamConfigBase(int bufferSize, String path, String deserializerName) {
        this.path = path;
        this.bufferSize = bufferSize;
        this.deserializerName = deserializerName;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public String path() {
        return path;
    }

    public String deserializerName() {
        return deserializerName;
    }
}
