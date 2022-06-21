package com.reactivebbq.loyalty;

import akka.actor.ActorRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static akka.pattern.Patterns.ask;

import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LoyaltyActorSupervisorTest extends AkkaTest {

    private ActorRef supervisor;

    @BeforeEach
    void setup() {
        LoyaltyRepository loyaltyRepository =
            new InMemoryLoyaltyRepository(Executors.newSingleThreadExecutor());
        supervisor = system.actorOf(LoyaltyActorSupervisor.create(loyaltyRepository));
    }

    @Test
    void deliver_shouldCreateAnActorAndSendItTheCommand() {
        LoyaltyId id = new LoyaltyId("Id");

        supervisor.tell(new LoyaltyActorSupervisor.Deliver(
            new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)),
            id
        ), ActorRef.noSender());

        LoyaltyInformation result = (LoyaltyInformation) ask(
            supervisor,
            new LoyaltyActorSupervisor.Deliver(
                new LoyaltyActor.GetLoyaltyInformation(),
                id
            ),
            timeout
        ).toCompletableFuture().join();

        assertEquals(10, result.getCurrentTotal());
        assertEquals(1, result.getAdjustments().size());
    }

    @Test
    void deliver_shouldReuseExistingActors() {
        LoyaltyId id = new LoyaltyId("Id");

        supervisor.tell(new LoyaltyActorSupervisor.Deliver(
            new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)),
            id
        ), ActorRef.noSender());

        supervisor.tell(new LoyaltyActorSupervisor.Deliver(
                new LoyaltyActor.ApplyLoyaltyAdjustment(new Deduct(5)),
                id
        ), ActorRef.noSender());

        LoyaltyInformation result = (LoyaltyInformation) ask(
                supervisor,
                new LoyaltyActorSupervisor.Deliver(
                        new LoyaltyActor.GetLoyaltyInformation(),
                        id
                ),
                timeout
        ).toCompletableFuture().join();

        assertEquals(5, result.getCurrentTotal());
        assertEquals(2, result.getAdjustments().size());
    }

}
