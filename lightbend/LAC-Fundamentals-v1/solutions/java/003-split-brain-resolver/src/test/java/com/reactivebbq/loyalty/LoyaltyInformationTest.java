package com.reactivebbq.loyalty;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LoyaltyInformationTest {

    @Test
    void currentTotal_shouldReturnZero_ifThereAreNoTransactions() {
        LoyaltyInformation loyalty = LoyaltyInformation.empty;

        assertEquals(0, loyalty.getCurrentTotal());
    }

    @Test
    void currentTotal_shouldReturnTheValueOfAnAward_ifItIsTheOnlyTransaction() {
        LoyaltyInformation loyalty = LoyaltyInformation.empty
                .applyAdjustment(new Award(10));

        assertEquals(10, loyalty.getCurrentTotal());
    }

    @Test
    void currentTotal_shouldReturnTheValueOfADeduct_ifItIsTheOnlyTransaction() {
        LoyaltyInformation loyalty = LoyaltyInformation.empty
                .applyAdjustment(new Deduct(10));

        assertEquals(-10, loyalty.getCurrentTotal());
    }

    @Test
    void currentTotal_shouldReturnTheCombinedValueOfAllAdjustments() {
        LoyaltyInformation loyalty = LoyaltyInformation.empty
                .applyAdjustment(new Award(100))
                .applyAdjustment(new Deduct(50))
                .applyAdjustment(new Award(30))
                .applyAdjustment(new Deduct(20));

        assertEquals(60, loyalty.getCurrentTotal());
    }

    @Test
    void applyAdjustment_shouldAddTheAdjustmentToTheList() {
        LoyaltyInformation loyaltyInformation = LoyaltyInformation.empty
                .applyAdjustment(new Award(10));

        assertEquals(1, loyaltyInformation.getAdjustments().size());
        assertEquals(10, loyaltyInformation.getAdjustments().get(0).getBalanceAdjustment());
    }

    @Test
    void applyAdjustment_shouldApplyMultipleAdjustments() {
        LoyaltyInformation loyaltyInformation = LoyaltyInformation.empty
                .applyAdjustment(new Award(10))
                .applyAdjustment(new Deduct(20))
                .applyAdjustment(new Award(30));

        assertEquals(3, loyaltyInformation.getAdjustments().size());
        assertEquals(10, loyaltyInformation.getAdjustments().get(0).getBalanceAdjustment());
        assertEquals(-20, loyaltyInformation.getAdjustments().get(1).getBalanceAdjustment());
        assertEquals(30, loyaltyInformation.getAdjustments().get(2).getBalanceAdjustment());
    }

}
