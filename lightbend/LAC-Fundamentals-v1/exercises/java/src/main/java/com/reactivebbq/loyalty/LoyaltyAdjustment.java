package com.reactivebbq.loyalty;

import com.fasterxml.jackson.annotation.JsonCreator;

interface LoyaltyAdjustment extends SerializableMessage {
    int getBalanceAdjustment();
    int getPoints();
}

class Award implements LoyaltyAdjustment {
    private int points;

    @Override
    public int getBalanceAdjustment() {
        return points;
    }

    @Override
    public int getPoints() {
        return points;
    }

    @JsonCreator
    Award(int points) {
        assert points > 0;
        this.points = points;
    }
}

class Deduct implements LoyaltyAdjustment {
    private int points;

    @Override
    public int getBalanceAdjustment() {
        return -points;
    }

    @Override
    public int getPoints() {
        return points;
    }

    @JsonCreator
    Deduct(int points) {
        assert points > 0;
        this.points = points;
    }
}
