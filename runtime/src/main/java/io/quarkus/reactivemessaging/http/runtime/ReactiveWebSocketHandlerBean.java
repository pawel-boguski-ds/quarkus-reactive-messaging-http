package io.quarkus.reactivemessaging.http.runtime;

import java.util.Collection;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.jboss.logging.Logger;

import io.quarkus.reactivemessaging.http.runtime.config.ReactiveHttpConfig;
import io.quarkus.reactivemessaging.http.runtime.config.WebSocketStreamConfig;
import io.quarkus.reactivemessaging.http.runtime.serializers.DeserializerFactoryBase;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.RoutingContext;

/**
 * a bean that handles incoming web socket messages
 */
@Singleton
public class ReactiveWebSocketHandlerBean extends ReactiveHandlerBeanBase<WebSocketStreamConfig, WebSocketMessage<?>> {

    private static final Logger log = Logger.getLogger(ReactiveWebSocketHandlerBean.class);

    @Inject
    ReactiveHttpConfig config;

    @Inject
    DeserializerFactoryBase deserializerFactory;

    @Inject
    MessageIdProviderFactoryBase messageIdProviderFactory;

    @Override
    protected void handleRequest(RoutingContext event, MultiEmitter<? super WebSocketMessage<?>> emitter,
            StrictQueueSizeGuard guard, WebSocketStreamConfig streamConfig) {
        event.request().toWebSocket(
                webSocket -> {
                    if (webSocket.failed()) {
                        log(webSocket.cause(), "failed to connect web socket");
                    } else {
                        ServerWebSocket serverWebSocket = webSocket.result();
                        serverWebSocket.handler(
                                b -> {
                                    if (emitter == null) {
                                        onUnexpectedError(serverWebSocket, null,
                                                "No consumer subscribed for messages sent to " +
                                                        "Reactive Messaging WebSocket endpoint on path: "
                                                        + streamConfig.path());
                                    } else if (guard.prepareToEmit()) {
                                        try {
                                            Object payload = deserializerFactory
                                                    .getDeserializer(streamConfig.deserializerName())
                                                    .map(d -> d.deserialize(b)).orElse(b);
                                            RequestMetadata requestMetadata = new RequestMetadata(event);
                                            String messageId = getMessageId(streamConfig.messageIdProvider(), payload,
                                                    requestMetadata);
                                            log.tracef("Emitting message with id %s", messageId);
                                            // TODO return result of serverWebSocket.write in WebSocketMessage ack and nack?
                                            emitter.emit(new WebSocketMessage<>(
                                                    payload, requestMetadata,
                                                    () -> onAck(serverWebSocket, messageId),
                                                    error -> onNack(serverWebSocket, error, messageId)));
                                        } catch (Exception error) {
                                            guard.dequeue();
                                            onUnexpectedError(serverWebSocket, error, "Emitting message failed");
                                        }
                                    } else {
                                        serverWebSocket.write(Buffer.buffer("BUFFER_OVERFLOW"));
                                    }
                                });
                    }
                });
    }

    @Override
    protected String description(WebSocketStreamConfig config) {
        return String.format("path %s", config.path());
    }

    @Override
    protected String key(WebSocketStreamConfig config) {
        return config.path();
    }

    @Override
    protected String key(RoutingContext context) {
        return context.currentRoute().getPath();
    }

    @Override
    protected Collection<WebSocketStreamConfig> configs() {
        return config.getWebSocketConfigs();
    }

    private void onAck(ServerWebSocket serverWebSocket, String messageId) {
        String response = "ACK" + (messageId != null ? "\n" + messageId : "");
        serverWebSocket.writeTextMessage(response);
    }

    private void onNack(ServerWebSocket serverWebSocket, Throwable error, String messageId) {
        String response = "NACK" + (messageId != null ? "\n" + messageId : "");
        String logMessage = "Failed to process incoming web socket message."
                + (messageId != null ? "Message id: " + messageId : "");
        log(error, logMessage);
        serverWebSocket.writeTextMessage(response);
    }

    private void onUnexpectedError(ServerWebSocket serverWebSocket, Throwable error, String message) {
        log(error, message);
        // TODO some error message for the client? exception mapper would be best...
        serverWebSocket.close((short) 3500, "Unexpected error while processing the message");
    }

    private String getMessageId(String messageIdProvider, Object payload, RequestMetadata requestMetadata) {
        return messageIdProviderFactory.getMessageIdProvider(messageIdProvider)
                .map(provider -> provider.getMessageId(Message.of(payload, Metadata.of(requestMetadata))))
                .orElse(null);
    }

    private void log(Throwable error, String message) {
        log.error(message + (error != null ? ": " + error.getMessage() : ""));
        log.debug(message, error);
    }

    Multi<WebSocketMessage<?>> getProcessor(String path) {
        Bundle<WebSocketStreamConfig, WebSocketMessage<?>> bundle = processors.get(path);
        if (bundle == null) {
            throw new IllegalStateException("No incoming stream defined for path " + path);
        }
        return bundle.getProcessor();
    }
}
