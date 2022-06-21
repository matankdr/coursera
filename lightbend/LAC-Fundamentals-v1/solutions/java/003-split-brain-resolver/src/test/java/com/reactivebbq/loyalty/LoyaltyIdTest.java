package com.reactivebbq.loyalty;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LoyaltyIdTest {

    @Test
    void equals_shouldReturnTrueForSameId() {
        LoyaltyId id1 = new LoyaltyId("same");
        LoyaltyId id2 = new LoyaltyId("same");

        assertEquals(id1, id2);
    }

    @Test
    void equals_shouldReturnFalseForDifferentId() {
        LoyaltyId id1 = new LoyaltyId("same");
        LoyaltyId id2 = new LoyaltyId("different");

        assertNotEquals(id1, id2);
    }

}
