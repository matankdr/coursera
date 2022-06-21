package com.reactivebbq.loyalty;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.ArrayList;
import java.util.List;

class LoyaltyInformation implements SerializableMessage {
    static final LoyaltyInformation empty = new LoyaltyInformation(new ArrayList<>());

    private final List<LoyaltyAdjustment> adjustments;

    List<LoyaltyAdjustment> getAdjustments() {
        return adjustments;
    }

    @JsonCreator
    private LoyaltyInformation(List<LoyaltyAdjustment> adjustments) {
        this.adjustments = new ArrayList<>(adjustments);
    }

    int getCurrentTotal() {
        return adjustments.stream()
                .map(LoyaltyAdjustment::getBalanceAdjustment)
                .reduce(0, (sum, adj) -> sum + adj);
    }

    LoyaltyInformation applyAdjustment(LoyaltyAdjustment adjustment) {
        List<LoyaltyAdjustment> updated = new ArrayList<>(adjustments);
        updated.add(adjustment);
        return new LoyaltyInformation(updated);
    }
}
