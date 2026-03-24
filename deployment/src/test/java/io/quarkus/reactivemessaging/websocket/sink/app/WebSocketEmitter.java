package io.quarkus.reactivemessaging.websocket.sink.app;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;

@ApplicationScoped
public class WebSocketEmitter {

    @Inject
    @Channel("my-ws-sink")
    Emitter<Object> emitter;

    @Inject
    @Channel("my-ws-sink-with-ack")
    Emitter<Object> emitterWithAck;

    @Inject
    @Channel("ws-sink-with-serializer")
    Emitter<Object> emitterWithCustomSerializer;

    public void sendMessage(Message<?> message) {
        emitter.send(message);
    }

    public void sendMessageWithAck(Message<?> message) {
        emitterWithAck.send(message);
    }

    public void sendMessageWithCustomSerializer(Message<String> message) {
        emitterWithCustomSerializer.send(message);
    }
}
