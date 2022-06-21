package com.reactivebbq.loyalty;

import akka.Done;
import akka.actor.ActorRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static akka.pattern.Patterns.ask;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LoyaltyActorTest extends AkkaTest {
    class BrokenLoyaltyRepository implements LoyaltyRepository {
        @Override
        public CompletableFuture<Done> updateLoyalty(LoyaltyId loyaltyId, LoyaltyInformation loyaltyInformation) {
            CompletableFuture<Done> future = new CompletableFuture<>();
            future.completeExceptionally(new Exception("Boom"));
            return future;
        }

        @Override
        public CompletableFuture<LoyaltyInformation> findLoyalty(LoyaltyId loyaltyId) {
            CompletableFuture<LoyaltyInformation> future = new CompletableFuture<>();
            future.completeExceptionally(new Exception("Bam"));
            return future;
        }
    }

    private LoyaltyId loyaltyId;
    private LoyaltyRepository loyaltyRepository;
    private BrokenLoyaltyRepository brokenRepository;
    private Lazy<ActorRef> loyaltyActor;

    class Lazy<T> {
        private final Supplier<T> supplier;
        private T value;

        Lazy(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        T get() {
            if(value == null)
                value = supplier.get();

            return value;
        }
    }

    @BeforeEach
    void setup() {
        loyaltyId = new LoyaltyId(UUID.randomUUID().toString());
        loyaltyRepository = new InMemoryLoyaltyRepository(Executors.newSingleThreadExecutor());
        brokenRepository = new BrokenLoyaltyRepository();
        loyaltyActor = new Lazy<>(() ->
            system.actorOf(LoyaltyActor.create(loyaltyRepository), loyaltyId.getValue())
        );
    }

    @Test
    void theActor_shouldLoadItsStateFromTheRepoOnStartup() {
        LoyaltyInformation state = LoyaltyInformation.empty
            .applyAdjustment(new Award(10));

        loyaltyRepository.updateLoyalty(loyaltyId, state).join();

        LoyaltyInformation result = (LoyaltyInformation) ask(loyaltyActor.get(), new LoyaltyActor.GetLoyaltyInformation(), timeout)
                .toCompletableFuture()
                .join();

        assertEquals(state.getCurrentTotal(), result.getCurrentTotal());
    }

    @Test
    void applyLoyaltyAdjustment_shouldReturnACorrespondingEvent() {
        LoyaltyActor.LoyaltyAdjustmentApplied result = (LoyaltyActor.LoyaltyAdjustmentApplied) ask(loyaltyActor.get(), new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)), timeout)
                .toCompletableFuture()
                .join();

        assertEquals(10, result.getAdjustment().getBalanceAdjustment());
    }

    @Test
    void applyLoyaltyAdjustment_shouldApplyAllAdjustments() {
        loyaltyActor.get().tell(new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)), ActorRef.noSender());
        loyaltyActor.get().tell(new LoyaltyActor.ApplyLoyaltyAdjustment(new Deduct(5)), ActorRef.noSender());
        loyaltyActor.get().tell(new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(20)), ActorRef.noSender());

        LoyaltyInformation result = (LoyaltyInformation) ask(loyaltyActor.get(), new LoyaltyActor.GetLoyaltyInformation(), timeout).toCompletableFuture().join();

        assertEquals(25, result.getCurrentTotal());
    }

    @Test
    void applyLoyaltyAdjustment_shouldUpdateTheLoyaltyRepository() {
        ask(loyaltyActor.get(), new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)), timeout)
            .toCompletableFuture()
            .join();

        LoyaltyInformation result = loyaltyRepository.findLoyalty(loyaltyId).join();

        assertEquals(10, result.getCurrentTotal());
    }

    @Test
    void applyLoyaltyAdjustment_shouldFailToDeductIfThereIsInsufficientPoints() {
        ask(loyaltyActor.get(), new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)), timeout)
                .toCompletableFuture()
                .join();

        LoyaltyActor.LoyaltyAdjustmentRejected result = (LoyaltyActor.LoyaltyAdjustmentRejected)
                ask(
                    loyaltyActor.get(),
                    new LoyaltyActor.ApplyLoyaltyAdjustment(new Deduct(15)),
                    timeout
                )
                .toCompletableFuture()
                .join();

        assertEquals(15, result.getAdjustment().getPoints());
        assertEquals("Insufficient Points", result.getReason());
    }

    @Test
    void applyLoyaltyAdjustment_shouldFailIfItCantWriteToTheRepo() {
        loyaltyActor = new Lazy<>(() ->
            system.actorOf(LoyaltyActor.create(brokenRepository), loyaltyId.getValue())
        );

        assertThrows(CompletionException.class, () ->
                ask(loyaltyActor.get(), new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)), timeout)
            .toCompletableFuture()
            .join()
        );
    }

    @Test
    void getLoyaltyInformation_shouldReturnEmptyIfNoAdjustmentsHaveBeenApplied() {
        LoyaltyInformation result = (LoyaltyInformation) ask(loyaltyActor.get(), new LoyaltyActor.GetLoyaltyInformation(), timeout)
            .toCompletableFuture()
            .join();

        assertEquals(0, result.getCurrentTotal());
        assertEquals(0, result.getAdjustments().size());
    }

    @Test
    void getLoyaltyInformation_shouldReturnUpdatedInfoIfAnAdjustmentHasBeenApplied() {
       loyaltyActor.get().tell(new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(10)), ActorRef.noSender());

       LoyaltyInformation result = (LoyaltyInformation) ask(loyaltyActor.get(), new LoyaltyActor.GetLoyaltyInformation(), timeout)
           .toCompletableFuture()
           .join();

       assertEquals(1, result.getAdjustments().size());
       assertEquals(10, result.getCurrentTotal());
    }
}
