package com.reactivebbq.loyalty;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class LoyaltyRepositoryTest {
    abstract LoyaltyRepository getLoyaltyRepository();

    private LoyaltyId createLoyaltyId() {
        return new LoyaltyId(UUID.randomUUID().toString());
    }

    @Test
    void findLoyalty_shouldReturnNothingIfTheIdDoesntExist() {
        assertThrows(CompletionException.class, () ->
            getLoyaltyRepository().findLoyalty(createLoyaltyId()).join()
        );
    }

    @Test
    void findLoyalty_shouldReturnTheLoyaltyIfItExists() {
        LoyaltyInformation info = LoyaltyInformation.empty
                .applyAdjustment(new Award(10))
                .applyAdjustment(new Deduct(5));
        LoyaltyId id = createLoyaltyId();

        getLoyaltyRepository().updateLoyalty(id, info).join();

        LoyaltyInformation result = getLoyaltyRepository().findLoyalty(id).join();

        assertEquals(info.getCurrentTotal(), result.getCurrentTotal());
    }

    @Test
    void findLoyalty_shouldReturnTheCorrectLoyaltyIfMultiplesExist() {
        LoyaltyInformation info1 = LoyaltyInformation.empty
                .applyAdjustment(new Award(10))
                .applyAdjustment(new Deduct(5));
        LoyaltyId id1 = createLoyaltyId();

        LoyaltyInformation info2 = LoyaltyInformation.empty
                .applyAdjustment(new Award(5))
                .applyAdjustment(new Deduct(3));
        LoyaltyId id2 = createLoyaltyId();

        getLoyaltyRepository().updateLoyalty(id1, info1).join();
        getLoyaltyRepository().updateLoyalty(id2, info2).join();

        LoyaltyInformation result = getLoyaltyRepository().findLoyalty(id1).join();

        assertEquals(info1.getCurrentTotal(), result.getCurrentTotal());
    }

    @Test
    void updateLoyalty_shouldOverwriteAndExistingValue() {
        LoyaltyId id  = createLoyaltyId();
        LoyaltyInformation info1 = LoyaltyInformation.empty
                .applyAdjustment(new Award(10))
                .applyAdjustment(new Deduct(5));

        LoyaltyInformation info2 = LoyaltyInformation.empty
                .applyAdjustment(new Award(5))
                .applyAdjustment(new Deduct(3));

        getLoyaltyRepository().updateLoyalty(id, info1).join();
        getLoyaltyRepository().updateLoyalty(id, info2).join();

        LoyaltyInformation result = getLoyaltyRepository().findLoyalty(id).join();

        assertEquals(info2.getCurrentTotal(), result.getCurrentTotal());
    }

}

class InMemoryLoyaltyRepositoryTest extends LoyaltyRepositoryTest {
    private final LoyaltyRepository loyaltyRepository = new InMemoryLoyaltyRepository(Executors.newSingleThreadExecutor());

    @Override
    LoyaltyRepository getLoyaltyRepository() {
        return loyaltyRepository;
    }

    InMemoryLoyaltyRepositoryTest() {
        super();
    }
}

class FileBasedLoyaltyRepositoryTest extends LoyaltyRepositoryTest {
    private static Path tmpDir;
    private static LoyaltyRepository loyaltyRepository;

    @Override
    LoyaltyRepository getLoyaltyRepository() {
        return loyaltyRepository;
    }

    @BeforeAll
    static void setup() throws IOException {
        tmpDir = Files.createTempDirectory("filebasedrepotest");
        loyaltyRepository = new FileBasedLoyaltyRepository(tmpDir, Executors.newSingleThreadExecutor());
    }

    @AfterAll
    static void tearDown() throws IOException {
        Files.walk(tmpDir).forEach(file -> {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ignored) {}
        });

        Files.deleteIfExists(tmpDir);
    }
}






