package io.quarkus.reactivemessaging.http.runtime;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniRetry;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.providers.helpers.SenderProcessor;

abstract class AbstractSink {

    private final SenderProcessor processor;
    private final Flow.Subscriber<? extends Message<?>> subscriber;

    public AbstractSink(Logger log, String url,
            int maxRetries, double jitter, Optional<Duration> delay,
            long inflights, boolean waitForCompletion) {
        if (inflights <= 0) {
            throw new IllegalArgumentException("Inflights must be greater than 0, but was " + inflights);
        }
        this.processor = new SenderProcessor(inflights, waitForCompletion, m -> {
            Uni<Void> send = send(m);

            log.debugf("maxRetries: %d for %s", maxRetries, url);
            if (maxRetries > 0) {
                UniRetry<Void> retry = send.onFailure().retry();
                if (delay.isPresent()) {
                    retry = retry.withBackOff(delay.get()).withJitter(jitter);
                }
                send = retry.atMost(maxRetries);
            }

            return send
                    .onItemOrFailure().transformToUni((result, error) -> {
                        // TODO move m.ack() after ack
                        if (error != null) {
                            return Uni.createFrom().completionStage(
                                    m.nack(error).thenRun(() -> log.debugf(error, "Error responding to %s", url)));
                        }
                        return Uni.createFrom()
                                .completionStage(m.ack().thenRun(() -> log.tracef("Responded with success to %s", url)));
                    });
        });
        //        this.subscriber = MultiUtils.via(processor, multi -> multi
        //            .onFailure().invoke(f -> log.debugf("Unable to dispatch message to %s", url))
        //            .onItem().transformToUniAndMerge(this::waitForAckOrNack)
        //        );
        this.subscriber = MultiUtils.via(processor, multi -> {
            multi = multi
                .onItem().invoke(message -> log.tracef("Message send to %s", url))
                .onFailure().invoke(f -> log.debugf("Unable to dispatch message to %s", url));
            if (hasAckSupport()) {
//                return multi.onItem().transformToUniAndMerge(this::waitForAckOrNack);
                return multi.onItem().transformToUni(this::waitForAckOrNack).merge(Integer.MAX_VALUE);
            }
            return multi;
        });
    }

    protected boolean hasAckSupport() {
        return false;
    }

    protected abstract Uni<Void> send(Message<?> message);

    /**
     * Can be overloaded if sink implementation use async ack/nack.
     *
     * @param message message to be acked or nacked
     * @return Uni completed when ack occurred. For nack failure should be propagated.
     */
    protected Uni<Message<?>> waitForAckOrNack(Message<?> message) {
        return Uni.createFrom().item(message);
    }

    Flow.Subscriber<? extends Message<?>> sink() {
        return subscriber;
    }

    void close() {
        if (processor != null) {
            processor.cancel();
        }
    }
}
