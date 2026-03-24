package io.quarkus.reactivemessaging.http.runtime;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A base superclass for a MessageIdProviderFactory that is generated in build time.
 */
public abstract class MessageIdProviderFactoryBase {

    private final Map<String, MessageIdProvider> messageIdProviderByClassName = new HashMap<>();

    protected MessageIdProviderFactoryBase() {
        initAdditionalMessageIdProviders();
    }

    /**
     * Get a {@link MessageIdProvider} of a given (class) name.
     *
     * @param name name of the message ID provider
     * @return An optional message ID provider
     */
    public Optional<MessageIdProvider> getMessageIdProvider(String name) {
        return name != null ? Optional.ofNullable(messageIdProviderByClassName.get(name)) : Optional.empty();
    }

    public void addMessageIdProvider(String className, MessageIdProvider messageIdProvider) {
        messageIdProviderByClassName.put(className, messageIdProvider);
    }

    /**
     * Method that initializes additional serializers (used by user's config). Implemented in the
     * generated subclass
     */
    protected abstract void initAdditionalMessageIdProviders();

}
