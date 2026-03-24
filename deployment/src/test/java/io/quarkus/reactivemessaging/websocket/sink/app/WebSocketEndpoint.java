package io.quarkus.reactivemessaging.websocket.sink.app;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.OnError;
import jakarta.websocket.OnMessage;
import jakarta.websocket.OnOpen;
import jakarta.websocket.Session;
import jakarta.websocket.server.ServerEndpoint;

import org.jboss.logging.Logger;

import io.quarkus.reactivemessaging.utils.VertxFriendlyLock;
import io.vertx.core.Vertx;

@ApplicationScoped
@ServerEndpoint("/ws-target-url")
public class WebSocketEndpoint {
    private static final Logger log = Logger.getLogger(WebSocketEndpoint.class);
    private final List<String> messages = new ArrayList<>();

    private final List<Session> sessions = new ArrayList<>();

    private final VertxFriendlyLock lock;

    @Inject
    WebSocketEndpoint(Vertx vertx) {
        lock = new VertxFriendlyLock(vertx);
    }

    @OnError
    void onError(Throwable error) {
        log.error("Unexpected error in the WebSocketSinkTest", error);
    }

    @OnOpen
    void onOpen(Session session) {
        sessions.add(session);
    }

    @OnMessage
    void consumeMessage(byte[] message) {
        String messageString = new String(message);
        messages.add(messageString);
        lock.triggerWhenUnlocked(() -> {
            if (messageString.endsWith("for ACK test")) {
                sessions.get(0).getAsyncRemote().sendText("ACK\n" + messageString);
            } else if (messageString.endsWith("for NACK test")) {
                sessions.get(0).getAsyncRemote().sendText("NACK\n" + messageString);
            } else {
                sessions.get(0).getAsyncRemote().sendText("ACK");
            }
        }, 10000);
    }

    public void killAllSessions() {
        for (Session session : sessions) {
            try {
                session.close();
            } catch (Exception ignored) {
            }
        }
        try {
            Thread.sleep(1000); // wait for the web socket sessions to be closed
        } catch (InterruptedException ignored) {
        }
        sessions.clear();
    }

    public List<String> getMessages() {
        return messages;
    }

    public void reset() {
        messages.clear();
        lock.reset();
        killAllSessions();
    }

    public int sessionCount() {
        return sessions.size();
    }

    public void pause() {
        lock.lock();
    }

    public void resume() {
        lock.unlock();
    }

}
