package com.swirlds.recordserver;

import io.helidon.common.LogConfig;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import io.helidon.health.HealthSupport;
import io.helidon.health.checks.HealthChecks;
import io.helidon.media.jsonp.JsonpSupport;
import io.helidon.metrics.MetricsSupport;
import io.helidon.webserver.Routing;
import io.helidon.webserver.WebServer;

/**
 * The application main class.
 */
public final class Main {

    /**
     * Cannot be instantiated.
     */
    private Main() {}

    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(final String[] args) {
        // load logging configuration
        LogConfig.configureRuntime();

        // By default, this will pick up application.yaml from the classpath
        Config config = Config.create();

        WebServer server = WebServer
                .builder(Routing.builder()
                        .register(MetricsSupport.create()) // Metrics at "/metrics"
                        .register(HealthSupport.builder()
                                .addLiveness(HealthChecks.healthChecks()) // Adds a convenient set of checks
                                .build()) // Health at "/health"
                        .register("/api/v1/balances", new BalancesService(config))
                        .register("/api/v1/blocks", new BlocksService(config))
                        .register("/api/v1/tokens", new TokensService(config))
                        .register("/api/v1/transactions", new TransactionsService(config))
                        .build())
                .config(config.get("server"))
                .addMediaSupport(JsonpSupport.create())
                .build();

        Single<WebServer> webserver = server.start();

        // Try to start the server. If successful, print some info and arrange to
        // print a message at shutdown. If unsuccessful, print the exception.
        webserver.thenAccept(ws -> {
            System.out.println("WEB server is up! http://localhost:" + ws.port() + "/simple-greet");
            ws.whenShutdown().thenRun(() -> System.out.println("WEB server is DOWN. Good bye!"));
        })
        .exceptionallyAccept(t -> {
            System.err.println("Startup failed: " + t.getMessage());
            t.printStackTrace(System.err);
        });
    }
}
