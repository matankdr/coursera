package com.reactivebbq.loyalty;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.sharding.ShardRegion;
import akka.japi.pf.ReceiveBuilder;

import java.io.Serializable;

class LoyaltyActorSupervisor extends AbstractActor {

    static ShardRegion.MessageExtractor messageExtractor = new ShardRegion.MessageExtractor() {
        @Override
        public String shardId(Object message) {
            if(message instanceof Deliver)
                return Integer.toString(
                    ((Deliver) message).to.getValue().hashCode() % 30
                );
            else
                return null;
        }

        @Override
        public String entityId(Object message) {
            if(message instanceof Deliver)
                return ((Deliver) message).to.getValue();
            else
                return null;
        }

        @Override
        public Object entityMessage(Object message) {
            if(message instanceof Deliver)
                return ((Deliver) message).command;
            else
                return null;
        }
    };

    static class Deliver implements SerializableMessage {
        private final LoyaltyActor.Command command;
        private final LoyaltyId to;

        LoyaltyActor.Command getCommand() {
            return command;
        }

        LoyaltyId getTo() {
            return to;
        }

        Deliver(LoyaltyActor.Command command, LoyaltyId to) {
            this.command = command;
            this.to = to;
        }
    }

    static Props create(LoyaltyRepository loyaltyRepository) {
        return Props.create(
            LoyaltyActorSupervisor.class,
            () -> new LoyaltyActorSupervisor(loyaltyRepository)
        );
    }

    private final LoyaltyRepository loyaltyRepository;

    private LoyaltyActorSupervisor(LoyaltyRepository loyaltyRepository) {
        this.loyaltyRepository = loyaltyRepository;
    }

    private ActorRef createLoyaltyActor(String name) {
        return getContext().actorOf(LoyaltyActor.create(loyaltyRepository), name);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create().match(
            Deliver.class,
            this::handle
        ).build();
    }

    private void handle(Deliver cmd) {
        ActorRef loyaltyActor = getContext()
            .child(cmd.getTo().getValue())
            .getOrElse(() -> createLoyaltyActor(cmd.getTo().getValue()));

        loyaltyActor.forward(cmd.getCommand(), getContext());
    }
}
