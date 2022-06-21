package com.reactivebbq.loyalty;

import akka.actor.ActorSystem;
import org.junit.jupiter.api.AfterAll;

import java.time.Duration;

abstract class AkkaTest {
    final ActorSystem system = ActorSystem.create();
    final Duration timeout = Duration.ofSeconds(5);

    @AfterAll
    void teardown() {
        system.terminate();
        system.getWhenTerminated().toCompletableFuture().join();
    }
}
