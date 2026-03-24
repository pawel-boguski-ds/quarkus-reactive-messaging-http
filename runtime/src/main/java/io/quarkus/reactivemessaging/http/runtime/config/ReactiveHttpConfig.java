package io.quarkus.reactivemessaging.http.runtime.config;

import static java.util.regex.Pattern.quote;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.spi.ConfigProviderResolver;

import io.quarkus.reactivemessaging.http.runtime.QuarkusHttpConnector;
import io.quarkus.reactivemessaging.http.runtime.QuarkusWebSocketConnector;

/**
 * Utility class for reading http and web socket connector configuration
 */
@Singleton
public class ReactiveHttpConfig {
    private static final String CONNECTOR = ".connector";
    private static final String CHANNEL_NAME_SEGMENT = "(?:[^.]+|\"[^\"]+\")";

    private static final String MP_MSG_IN = "mp.messaging.incoming.";
    private static final String IN_KEY = "mp.messaging.incoming.%s.%s";
    private static final Pattern IN_PATTERN = Pattern.compile(quote(MP_MSG_IN) + CHANNEL_NAME_SEGMENT + quote(CONNECTOR));

    private static final String MP_MSG_OUT = "mp.messaging.outgoing.";
    private static final String OUT_KEY = "mp.messaging.outgoing.%s.%s";
    private static final Pattern OUT_PATTERN = Pattern.compile(quote(MP_MSG_OUT) + CHANNEL_NAME_SEGMENT + quote(CONNECTOR));

    private List<HttpStreamConfig> httpConfigs;
    private List<WebSocketStreamConfig> websocketConfigs;

    /**
     * @return list of http stream configurations
     */
    public List<HttpStreamConfig> getHttpConfigs() {
        return httpConfigs;
    }

    /**
     * @return list of web socket stream configurations
     */
    public List<WebSocketStreamConfig> getWebSocketConfigs() {
        return websocketConfigs;
    }

    @PostConstruct
    void init() {
        httpConfigs = readIncomingHttpConfigs();
        websocketConfigs = readIncomingWebSocketConfigs();
    }

    /**
     * Reads HTTP config, can be used in the build time
     *
     * @return list of HTTP configurations
     */
    public static List<HttpStreamConfig> readIncomingHttpConfigs() {
        List<HttpStreamConfig> streamConfigs = new ArrayList<>();
        Config config = ConfigProviderResolver.instance().getConfig();
        for (String propertyName : config.getPropertyNames()) {
            String connectorName = getConnectorNameIfMatching(IN_PATTERN, propertyName, IN_KEY, MP_MSG_IN,
                    QuarkusHttpConnector.NAME);
            if (connectorName != null) {
                String method = getConfigProperty(IN_KEY, connectorName, "method", "POST", String.class);
                String path = getConfigProperty(IN_KEY, connectorName, "path", String.class);
                int bufferSize = getConfigProperty(IN_KEY, connectorName, "buffer-size",
                        QuarkusHttpConnector.DEFAULT_SOURCE_BUFFER, Integer.class);
                String deserializerName = getConfigProperty(IN_KEY, connectorName, "deserializer", null, String.class);
                streamConfigs.add(new HttpStreamConfig(path, method, connectorName, bufferSize, deserializerName));
            }
        }
        return streamConfigs;
    }

    /**
     * Reads web socket config, can be used in the build time
     *
     * @return list of web socket configurations
     */
    public static List<WebSocketStreamConfig> readIncomingWebSocketConfigs() {
        List<WebSocketStreamConfig> streamConfigs = new ArrayList<>();
        Config config = ConfigProviderResolver.instance().getConfig();
        for (String propertyName : config.getPropertyNames()) {
            String connectorName = getConnectorNameIfMatching(IN_PATTERN, propertyName, IN_KEY, MP_MSG_IN,
                    QuarkusWebSocketConnector.NAME);

            if (connectorName != null) {
                String path = getConfigProperty(IN_KEY, connectorName, "path", String.class);
                int bufferSize = getConfigProperty(IN_KEY, connectorName, "buffer-size",
                        QuarkusWebSocketConnector.DEFAULT_SOURCE_BUFFER, Integer.class);
                String deserializerName = getConfigProperty(IN_KEY, connectorName, "deserializer", null, String.class);
                String messageIdProvider = getConfigProperty(IN_KEY, connectorName, "message-id-provider", null, String.class);
                streamConfigs.add(new WebSocketStreamConfig(path, bufferSize, deserializerName, messageIdProvider));
            }
        }
        return streamConfigs;
    }

    public static Set<String> readMessageIdProviders() {
        Set<String> messageIdProviders = new HashSet<>();
        messageIdProviders.addAll(readProperties(IN_PATTERN, IN_KEY, MP_MSG_IN, "message-id-provider"));
        messageIdProviders.addAll(readProperties(OUT_PATTERN, OUT_KEY, MP_MSG_OUT, "message-id-provider"));
        return messageIdProviders;
    }

    /**
     * Read custom serializer class names from the configuration
     *
     * @return set of custom serializer class names
     */
    public static Set<String> readSerializers() {
        return readProperties(OUT_PATTERN, OUT_KEY, MP_MSG_OUT, "serializer");
    }

    public static Set<String> readDeserializers() {
        return readProperties(IN_PATTERN, IN_KEY, MP_MSG_IN, "deserializer");
    }

    private static Set<String> readProperties(Pattern pattern, String key, String message, String propertyKey) {
        Set<String> result = new HashSet<>();
        Config config = ConfigProviderResolver.instance().getConfig();
        for (String propertyName : config.getPropertyNames()) {
            String connectorName = getConnectorNameIfMatching(pattern, propertyName, key, message,
                    QuarkusWebSocketConnector.NAME);
            if (connectorName == null) {
                connectorName = getConnectorNameIfMatching(pattern, propertyName, key, message,
                        QuarkusHttpConnector.NAME);
            }
            if (connectorName != null) {
                String serializer = getConfigProperty(key, connectorName, propertyKey, null, String.class);
                if (serializer != null) {
                    result.add(serializer);
                }
            }
        }
        return result;
    }

    private static String getConnectorNameIfMatching(Pattern connectorPropertyPattern,
            String propertyName, String format, String prefix, String expectedConnectorType) {
        Matcher matcher = connectorPropertyPattern.matcher(propertyName);
        if (matcher.matches()) {
            String connectorName = propertyName.substring(prefix.length(), propertyName.length() - CONNECTOR.length());
            String connectorType = getConfigProperty(format, connectorName, "connector", String.class);
            boolean matches = expectedConnectorType.equals(connectorType);
            return matches ? connectorName : null;
        } else {
            return null;
        }
    }

    private static <T> T getConfigProperty(String format, String connectorName, String property, T defValue, Class<T> type) {
        String key = String.format(format, connectorName, property);
        return ConfigProvider.getConfig().getOptionalValue(key, type).orElse(defValue);
    }

    private static <T> T getConfigProperty(String format, String connectorName, String property, Class<T> type) {
        String key = String.format(format, connectorName, property);
        return ConfigProvider.getConfig().getOptionalValue(key, type)
                .orElseThrow(() -> noPropertyFound(connectorName, property));
    }

    private static IllegalStateException noPropertyFound(String key, String propertyName) {
        String message = String.format("No %s defined for reactive http connector '%s'", propertyName, key);
        return new IllegalStateException(message);
    }
}
