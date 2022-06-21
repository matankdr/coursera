package com.reactivebbq.loyalty;

import akka.actor.ActorRef;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.Route;
import akka.http.scaladsl.model.StatusCodes;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.PathMatchers.integerSegment;
import static akka.http.javadsl.server.PathMatchers.segment;
import static akka.pattern.Patterns.ask;

class LoyaltyRoutes extends AllDirectives {
    private final ActorRef loyaltyActors;
    private final Duration timeout = Duration.ofSeconds(5);

    LoyaltyRoutes(ActorRef loyaltyActors) {
        this.loyaltyActors = loyaltyActors;
    }

    Route createRoutes() {
        return pathPrefix("loyalty", () ->
            pathPrefix(segment(), (id) ->
                concat(
                    pathPrefix("award", () ->
                        path(integerSegment(), (value) ->
                            post(() -> awardLoyalty(id, value))
                        )
                    ),
                    pathPrefix("deduct", () ->
                        path(integerSegment(), (value) ->
                            post(() -> deductLoyalty(id, value))
                        )
                    ),
                    pathEnd(() ->
                        get(() -> getLoyalty(id))
                    )
                )
            )
        );
    }

    private Route awardLoyalty(String id, int value) {
        LoyaltyId loyaltyId = new LoyaltyId(id);
        LoyaltyActor.ApplyLoyaltyAdjustment command =
                new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(value));

        CompletionStage<LoyaltyActor.Event> result =
            ask(
                loyaltyActors,
                new LoyaltyActorSupervisor.Deliver(command, loyaltyId),
                timeout
            ).thenApply((obj) -> (LoyaltyActor.Event) obj);

        return onComplete(result, maybeResult ->
            maybeResult.map(this::complete)
            .get()
        );
    }

    private Route deductLoyalty(String id, int value) {
        LoyaltyId loyaltyId = new LoyaltyId(id);
        LoyaltyActor.ApplyLoyaltyAdjustment command =
                new LoyaltyActor.ApplyLoyaltyAdjustment(new Deduct(value));

        CompletionStage<LoyaltyActor.Event> result =
            ask(
                loyaltyActors,
                new LoyaltyActorSupervisor.Deliver(command, loyaltyId),
                timeout
            ).thenApply((obj) -> (LoyaltyActor.Event) obj);

        return onComplete(result, maybeResult ->
            maybeResult.map(this::complete)
            .get()
        );
    }

    private Route complete(LoyaltyActor.Event event) {
        if(event instanceof LoyaltyActor.LoyaltyAdjustmentApplied) {
            LoyaltyActor.LoyaltyAdjustmentApplied applied =
                (LoyaltyActor.LoyaltyAdjustmentApplied) event;
            LoyaltyAdjustment adjustment = applied.getAdjustment();
            return complete(
                StatusCodes.OK(),
                "Applied: "+adjustment.getClass().getSimpleName() +
                        " " + adjustment.getPoints()
            );
        } else {
            LoyaltyActor.LoyaltyAdjustmentRejected rejected =
                (LoyaltyActor.LoyaltyAdjustmentRejected) event;
            LoyaltyAdjustment adjustment = rejected.getAdjustment();
            return complete(
                StatusCodes.BadRequest(),
                "Rejected: "+adjustment.getClass().getSimpleName() +
                        " " + adjustment.getPoints()
            );
        }
    }

    private Route getLoyalty(String id) {
        LoyaltyId loyaltyId = new LoyaltyId(id);
        LoyaltyActor.GetLoyaltyInformation command =
            new LoyaltyActor.GetLoyaltyInformation();

        CompletionStage<LoyaltyInformation> result =
            ask(
                loyaltyActors,
                new LoyaltyActorSupervisor.Deliver(command, loyaltyId),
                timeout
            ).thenApply((obj) -> (LoyaltyInformation) obj);

        return onComplete(result, maybeResult ->
            maybeResult.map(this::complete)
            .get()
        );
    }

    private Route complete(LoyaltyInformation info) {
        StringBuilder adjustments = new StringBuilder();

        for(LoyaltyAdjustment adj : info.getAdjustments()) {
            if(adj instanceof Award)
                adjustments
                    .append("- Award ")
                    .append(adj.getPoints())
                    .append("\n");
            else
                adjustments
                    .append("- Deduct ")
                    .append(adj.getPoints())
                    .append("\n");
        }

        return complete(
            "Current Balance: "+info.getCurrentTotal()+"\n"+
                "History:\n"+
                adjustments
        );
    }
}
