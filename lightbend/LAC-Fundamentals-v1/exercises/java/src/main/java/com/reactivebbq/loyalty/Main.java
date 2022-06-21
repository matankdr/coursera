package com.reactivebbq.loyalty;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.stream.Materializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static ActorSystem system;
    private static Materializer materializer;
    private static LoyaltyRepository loyaltyRepository;
    private static ActorRef loyaltyActorSupervisor;

    public static void main(String[] args) {
        loadConfigOverrides(args);

        initializeActorSystem();
        initializeRepositories();
        initializeActors();
        initializeHttpServer();
    }

    private static void loadConfigOverrides(String[] args) {
        String regex = "-D(\\S+)=(\\S+)";
        Pattern pattern = Pattern.compile(regex);

        for (String arg : args) {
            Matcher matcher = pattern.matcher(arg);

            while(matcher.find()) {
                String key = matcher.group(1);
                String value = matcher.group(2);
                logger.info("Config Override: "+key+" = "+value);
                System.setProperty(key, value);
            }
        }
    }

    private static void initializeActorSystem() {
        system = ActorSystem.create("Loyalty");
        materializer = Materializer.createMaterializer(system);
    }

    private static void initializeRepositories() {
        Path rootPath = Paths.get("tmp");
        try {
            loyaltyRepository = new FileBasedLoyaltyRepository(
                rootPath,
                system.getDispatcher()
            );
        } catch(IOException ex) {
            logger.error("Unable to initialize Loyalty Repo", ex);
        }
    }

    private static void initializeActors() {
        loyaltyActorSupervisor = system.actorOf(
            LoyaltyActorSupervisor.create(loyaltyRepository)
        );

        // TODO: Uncomment to enable cluster sharding.
        // loyaltyActorSupervisor = ClusterSharding.get(system).start(
        //     "loyalty",
        //     LoyaltyActor.create(loyaltyRepository),
        //     ClusterShardingSettings.create(system),
        //     LoyaltyActorSupervisor.messageExtractor
        // );
    }

    private static void initializeHttpServer() {
        LoyaltyRoutes routes = new LoyaltyRoutes(loyaltyActorSupervisor);

        int httpPort = system.settings()
            .config()
            .getInt("akka.http.server.default-http-port");

        Http.get(system).bindAndHandle(
            routes.createRoutes().flow(system, materializer),
            ConnectHttp.toHost("localhost", httpPort),
            materializer
        );
    }
}
