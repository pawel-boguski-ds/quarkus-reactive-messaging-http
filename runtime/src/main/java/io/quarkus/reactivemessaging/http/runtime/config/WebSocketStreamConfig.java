package io.quarkus.reactivemessaging.http.runtime.config;

public class WebSocketStreamConfig extends StreamConfigBase {

    private final String messageIdProvider;

    public WebSocketStreamConfig(String path, int bufferSize, String deserializerName,
            String messageIdProvider) {
        super(bufferSize, path, deserializerName);
        this.messageIdProvider = messageIdProvider;
    }

    public String messageIdProvider() {
        return messageIdProvider;
    }
}
