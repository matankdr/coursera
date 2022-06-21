package com.reactivebbq.loyalty;

import akka.actor.ActorRef;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.testkit.JUnitRouteTest;
import akka.http.javadsl.testkit.TestRoute;
import akka.testkit.TestActor;
import akka.testkit.TestProbe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LoyaltyRoutesTest extends JUnitRouteTest {

    private TestProbe loyaltyActorSupervisor;
    private TestRoute route;

    private void setAutoPilot(Object message, Object response) {
        loyaltyActorSupervisor.setAutoPilot(new TestActor.AutoPilot() {
            @Override
            public TestActor.AutoPilot run(ActorRef sender, Object msg) {
                if(msg.getClass() == message.getClass()) {
                    sender.tell(response, loyaltyActorSupervisor.ref());
                }

                return keepRunning();
            }
        });
    }

    @BeforeEach
    void setup() {
        systemResource().before();
        loyaltyActorSupervisor =  TestProbe.apply(system());
        LoyaltyRoutes routes = new LoyaltyRoutes(loyaltyActorSupervisor.ref());
        route = testRoute(routes.createRoutes());
    }

    @Test
    void loyalty_id_shouldReturnTheResultsFromTheSupervisor() {
        LoyaltyId loyaltyId = new LoyaltyId("someId");
        LoyaltyActorSupervisor.Deliver expectedRequest = new LoyaltyActorSupervisor.Deliver(
            new LoyaltyActor.GetLoyaltyInformation(),
            loyaltyId
        );

        String expectedResponse = "Current Balance: 10\nHistory:\n- Award 10\n";

        setAutoPilot(expectedRequest, LoyaltyInformation.empty.applyAdjustment(new Award(10)));

        route.run(HttpRequest.GET("/loyalty/"+loyaltyId.getValue()))
            .assertStatusCode(StatusCodes.OK)
            .assertContentType(ContentTypes.TEXT_PLAIN_UTF8)
            .assertEntity(expectedResponse);
    }

    @Test
    void loyalty_id_award_points_shouldIndicateTheAdjustmentWasAppliedIfItSucceeds() {
        LoyaltyId loyaltyId = new LoyaltyId("someId");
        int points = 30;
        LoyaltyActorSupervisor.Deliver expectedRequest = new LoyaltyActorSupervisor.Deliver(
            new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(points)),
            loyaltyId
        );

        LoyaltyActor.LoyaltyAdjustmentApplied expectedResponse = new LoyaltyActor.LoyaltyAdjustmentApplied(new Award(points));

        setAutoPilot(expectedRequest, expectedResponse);

        route.run(HttpRequest.POST("/loyalty/"+loyaltyId.getValue()+"/award/"+points))
            .assertStatusCode(StatusCodes.OK)
            .assertContentType(ContentTypes.TEXT_PLAIN_UTF8)
            .assertEntity("Applied: Award "+expectedResponse.getAdjustment().getPoints());
    }

    @Test
    void loyalty_id_award_points_shouldIndicateTheAdjustmentWasNotAppliedIfItFailed() {
        LoyaltyId loyaltyId = new LoyaltyId("someId");
        int points = 30;
        LoyaltyActorSupervisor.Deliver expectedRequest = new LoyaltyActorSupervisor.Deliver(
                new LoyaltyActor.ApplyLoyaltyAdjustment(new Award(points)),
                loyaltyId
        );

        LoyaltyActor.LoyaltyAdjustmentRejected expectedResponse = new LoyaltyActor.LoyaltyAdjustmentRejected(new Award(points), "reason");

        setAutoPilot(expectedRequest, expectedResponse);

        route.run(HttpRequest.POST("/loyalty/"+loyaltyId.getValue()+"/award/"+points))
            .assertStatusCode(StatusCodes.BAD_REQUEST)
            .assertContentType(ContentTypes.TEXT_PLAIN_UTF8)
            .assertEntity("Rejected: Award "+expectedResponse.getAdjustment().getPoints());
    }

    @Test
    void loyalty_id_deduct_points_shouldIndicateTheAdjustmentWasAppliedIfItSucceeds() {
        LoyaltyId loyaltyId = new LoyaltyId("someId");
        int points = 30;
        LoyaltyActorSupervisor.Deliver expectedRequest = new LoyaltyActorSupervisor.Deliver(
                new LoyaltyActor.ApplyLoyaltyAdjustment(new Deduct(points)),
                loyaltyId
        );

        LoyaltyActor.LoyaltyAdjustmentApplied expectedResponse = new LoyaltyActor.LoyaltyAdjustmentApplied(new Deduct(points));

        setAutoPilot(expectedRequest, expectedResponse);

        route.run(HttpRequest.POST("/loyalty/"+loyaltyId.getValue()+"/deduct/"+points))
                .assertStatusCode(StatusCodes.OK)
                .assertContentType(ContentTypes.TEXT_PLAIN_UTF8)
                .assertEntity("Applied: Deduct "+expectedResponse.getAdjustment().getPoints());
    }

    @Test
    void loyalty_id_deduct_points_shouldIndicateTheAdjustmentWasNotAppliedIfItFailed() {
        LoyaltyId loyaltyId = new LoyaltyId("someId");
        int points = 30;
        LoyaltyActorSupervisor.Deliver expectedRequest = new LoyaltyActorSupervisor.Deliver(
                new LoyaltyActor.ApplyLoyaltyAdjustment(new Deduct(points)),
                loyaltyId
        );

        LoyaltyActor.LoyaltyAdjustmentRejected expectedResponse = new LoyaltyActor.LoyaltyAdjustmentRejected(new Deduct(points), "reason");

        setAutoPilot(expectedRequest, expectedResponse);

        route.run(HttpRequest.POST("/loyalty/"+loyaltyId.getValue()+"/deduct/"+points))
                .assertStatusCode(StatusCodes.BAD_REQUEST)
                .assertContentType(ContentTypes.TEXT_PLAIN_UTF8)
                .assertEntity("Rejected: Deduct "+expectedResponse.getAdjustment().getPoints());
    }

    @AfterEach
    void teardown() {
        systemResource().after();
    }

}
