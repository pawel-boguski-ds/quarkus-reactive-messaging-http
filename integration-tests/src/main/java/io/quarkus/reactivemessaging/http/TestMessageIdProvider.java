package io.quarkus.reactivemessaging.http;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.quarkus.reactivemessaging.http.runtime.MessageIdProvider;

public class TestMessageIdProvider implements MessageIdProvider {

    @Override
    public String getMessageId(Message<?> message) {
        return message.getPayload().toString();
    }
}
