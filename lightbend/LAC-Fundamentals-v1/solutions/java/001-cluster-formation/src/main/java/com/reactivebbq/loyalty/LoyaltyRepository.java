package com.reactivebbq.loyalty;

import akka.Done;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

interface LoyaltyRepository {
    CompletableFuture<Done> updateLoyalty(
        LoyaltyId loyaltyId,
        LoyaltyInformation loyaltyInformation
    );

    CompletableFuture<LoyaltyInformation> findLoyalty(LoyaltyId loyaltyId);

}

class InMemoryLoyaltyRepository implements LoyaltyRepository {

    private final Map<LoyaltyId, LoyaltyInformation> data;
    private final Executor executor;

    InMemoryLoyaltyRepository(Executor executor) {
        data = new HashMap<>();
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Done> updateLoyalty(
        LoyaltyId loyaltyId,
        LoyaltyInformation loyaltyInformation
    ) {
        return CompletableFuture.supplyAsync(() -> {
            data.put(loyaltyId, loyaltyInformation);
            return Done.getInstance();
        });
    }

    @Override
    public CompletableFuture<LoyaltyInformation> findLoyalty(
        LoyaltyId loyaltyId
    ) {
        return CompletableFuture.supplyAsync(() -> {
            if(data.containsKey(loyaltyId)) {
                return data.get(loyaltyId);
            } else {
                throw new NoSuchElementException("The Id was not found: " +
                    loyaltyId.getValue());
            }
        }, executor);
    }
}

class FileBasedLoyaltyRepository implements LoyaltyRepository {

    private final Path rootPath;
    private final Executor executor;

    FileBasedLoyaltyRepository(Path rootPath, Executor executor)
        throws IOException {

        this.rootPath = rootPath;
        this.executor = executor;

        Files.createDirectories(rootPath);
    }

    @Override
    public CompletableFuture<Done> updateLoyalty(
        LoyaltyId loyaltyId,
        LoyaltyInformation loyaltyInformation
    ) {
        return CompletableFuture.supplyAsync(() -> {
            ArrayList<String> strings = new ArrayList<>();

            for(LoyaltyAdjustment adj : loyaltyInformation.getAdjustments()) {
              strings.add(Integer.toString(adj.getBalanceAdjustment()));
            }

            try {
                File file = new File(rootPath.toFile(), loyaltyId.getValue());
                Files.write(
                    Paths.get(file.getAbsolutePath()),
                    String.join(",", strings).getBytes()
                );
            } catch (IOException ex) {
                throw new CompletionException(ex);
            }

            return Done.getInstance();
        }, executor);
    }

    @Override
    public CompletableFuture<LoyaltyInformation> findLoyalty(
        LoyaltyId loyaltyId
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                File file = new File(rootPath.toFile(), loyaltyId.getValue());

                String fileContents = new String(
                    Files.readAllBytes(
                        Paths.get(file.getAbsolutePath())
                    )
                );

                LoyaltyInformation loyaltyInfo = LoyaltyInformation.empty;

                for (String str : fileContents.split(",")) {
                    int points = Integer.parseInt(str);
                    if(points >=0)
                        loyaltyInfo = loyaltyInfo.applyAdjustment(
                            new Award(points)
                        );
                    else
                        loyaltyInfo = loyaltyInfo.applyAdjustment(
                            new Deduct(-points)
                        );
                }

                return loyaltyInfo;
            } catch (IOException ex) {
                throw new CompletionException(ex);
            }

        }, executor);
    }
}
