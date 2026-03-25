package io.quarkus.reactivemessaging.http;

import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;

@ApplicationScoped
@Path("websocket-ack-helper")
public class WebSocketAckTestHelper {

    @Inject
    Logger log;

    private final List<String> messages = new ArrayList<>();

    @Channel("websocket-sink-with-ack")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 123L)
    Emitter<String> emitterWithAck;

    @Channel("websocket-sink-with-ack-and-inflights")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 123L)
    Emitter<String> emitterWithAckAndInflights;

    @Incoming("websocket-source-with-ack")
    Uni<Void> consumeWithAck(Message<String> message) {
        return consume(message);
    }

    //    @Blocking
    @Incoming("websocket-source-with-ack-and-inflights")
    Uni<Void> consumeWithAckAndInflights(Message<String> message) {
        return consume(message);
    }

    private Uni<Void> consume(Message<String> message) {
        log.infof("Consuming message: %s", message.getPayload());
        if (!VertxContext.isOnDuplicatedContext()) {
            throw new IllegalStateException("Expected to be on a duplicated context");
        }

//        return Uni.createFrom().voidItem()
//                .onItem().delayIt().by(Duration.ofMillis(2000))
        return Uni.createFrom().item(() -> {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            })
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .onItem().transformToUni(done -> {
                messages.add(message.getPayload());
                log.infof("Acking message %s", message.getPayload());
                return Uni.createFrom().completionStage(message.ack());
            });
    }

    @POST
    @Path("add-with-ack")
    public void addWithAck(String message) {
        log.tracef("Sending message with ack: %s", message);
        emitterWithAck.send(message);
    }

    @POST
    @Path("add-with-ack-and-inflights")
    public void addWithAckAndInflights(String message) {
        log.tracef("Sending message with ack and inflights: %s", message);
        emitterWithAckAndInflights.send(message);
    }

    @GET
    @Path("get-messages")
    public String getMessages() {
        return String.join(",", messages);
    }

    @DELETE
    public void clear() {
        messages.clear();
    }
}
