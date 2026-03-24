package io.quarkus.reactivemessaging.http.runtime;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Provides ID from incoming message.
 */
public interface MessageIdProvider {

    /**
     * Returns ID from incoming message. Message ID must not contain carriage return, new line or ":"
     * characters.
     *
     * @param message incoming message
     * @return message ID
     */
    String getMessageId(Message<?> message);
}
