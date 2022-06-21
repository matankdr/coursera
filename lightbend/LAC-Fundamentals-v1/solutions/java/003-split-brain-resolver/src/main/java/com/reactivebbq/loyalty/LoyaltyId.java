package com.reactivebbq.loyalty;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Objects;

class LoyaltyId implements SerializableMessage {
    private final String value;

    String getValue() {
        return value;
    }

    @JsonCreator
    LoyaltyId(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoyaltyId loyaltyId = (LoyaltyId) o;
        return Objects.equals(value, loyaltyId.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
