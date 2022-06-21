package com.reactivebbq.loyalty;

import akka.actor.AbstractActorWithStash;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.fasterxml.jackson.annotation.JsonCreator;

import static akka.pattern.Patterns.pipe;

import java.util.concurrent.CompletableFuture;

class LoyaltyActor extends AbstractActorWithStash {
    private final LoggingAdapter log = Logging.getLogger(
        getContext().getSystem(),
        this
    );

    interface Command extends SerializableMessage {}
    interface Event extends SerializableMessage {}

    static class ApplyLoyaltyAdjustment implements Command {
        private final LoyaltyAdjustment adjustment;

        LoyaltyAdjustment getAdjustment() {
            return adjustment;
        }

        @JsonCreator
        ApplyLoyaltyAdjustment(LoyaltyAdjustment adjustment) {
            this.adjustment = adjustment;
        }
    }

    static class GetLoyaltyInformation implements Command {}

    static class LoyaltyAdjustmentApplied implements Event {
        private final LoyaltyAdjustment adjustment;

        LoyaltyAdjustment getAdjustment() {
            return adjustment;
        }

        @JsonCreator
        LoyaltyAdjustmentApplied(LoyaltyAdjustment adjustment) {
            this.adjustment = adjustment;
        }
    }

    static class LoyaltyAdjustmentRejected implements Event {
        private final LoyaltyAdjustment adjustment;
        private final String reason;

        LoyaltyAdjustment getAdjustment() {
            return adjustment;
        }

        String getReason() {
            return reason;
        }

        LoyaltyAdjustmentRejected(LoyaltyAdjustment adjustment, String reason) {
            this.adjustment = adjustment;
            this.reason = reason;
        }
    }

    static Props create(LoyaltyRepository loyaltyRepository) {
        return Props.create(
            LoyaltyActor.class,
            () -> new LoyaltyActor(loyaltyRepository)
        );
    }

    private final LoyaltyId loyaltyId = new LoyaltyId(getSelf().path().name());
    private final LoyaltyRepository loyaltyRepository;
    private LoyaltyInformation loyaltyInformation = LoyaltyInformation.empty;


    private LoyaltyActor(LoyaltyRepository loyaltyRepository) {
        this.loyaltyRepository = loyaltyRepository;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();

        CompletableFuture<LoyaltyInformation> info = loyaltyRepository
            .findLoyalty(loyaltyId)
            .handle((loyaltyInfo, ex) -> {
                if(loyaltyInfo != null) {
                    log.info("Loyalty Information Loaded For " +
                        loyaltyId.getValue());
                    return loyaltyInfo;
                } else {
                    log.info("Creating New Loyalty Account For " +
                        loyaltyId.getValue());
                    return LoyaltyInformation.empty;
                }
            });

        pipe(info, getContext().getDispatcher()).to(getSelf());
    }

    @Override
    public Receive createReceive() {
        return initializing();
    }

    private Receive initializing() {
        return receiveBuilder()
            .match(
                LoyaltyInformation.class,
                loyaltyInfo -> {
                    loyaltyInformation = loyaltyInfo;
                    getContext().become(running());
                    unstashAll();
                }
            )
            .matchAny((msg) -> stash())
            .build();
    }

    private Receive running() {
        return receiveBuilder()
            .match(
                ApplyLoyaltyAdjustment.class,
                this::handle
            )
            .match(
                GetLoyaltyInformation.class,
                this::handle
            )
            .build();
    }

    private void handle(ApplyLoyaltyAdjustment cmd) {
        LoyaltyAdjustment adjustment = cmd.getAdjustment();

        if(adjustment instanceof Deduct &&
            adjustment.getPoints() > loyaltyInformation.getCurrentTotal()) {

            log.info("Insufficient Points For "+loyaltyId.getValue());

            getSender().tell(
                new LoyaltyAdjustmentRejected(
                    adjustment,
                    "Insufficient Points"
                ),
                getSelf()
            );
        } else {

            log.info("Applying " + adjustment.getClass().getSimpleName() +
                " "+adjustment.getBalanceAdjustment() + " for " +
                loyaltyId.getValue());

            loyaltyInformation = loyaltyInformation.applyAdjustment(adjustment);

            CompletableFuture<LoyaltyAdjustmentApplied> result =
                loyaltyRepository.updateLoyalty(loyaltyId, loyaltyInformation)
                    .thenApply((done) ->
                        new LoyaltyAdjustmentApplied(adjustment)
                    );

            pipe(result, getContext().getDispatcher()).to(getSender());
        }

    }

    private void handle(GetLoyaltyInformation ignored) {
        log.info("Retrieving Loyalty Information For "+loyaltyId.getValue());
        getSender().tell(loyaltyInformation, getSelf());
    }
}
