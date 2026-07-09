import org.apache.camel.builder.RouteBuilder;

/**
 * Continuous load generators — one per bridge route.
 * Each timer fires every 50 ms (~20 msg/s), well above the per-route SLA so the
 * rate limiter is clearly exercised: HTTP routes return ~75% 429s, Kafka consumer
 * is throttled to the configured limit via blocking backpressure.
 *
 * HTTP loaders call the bridge via the internal ClusterIP service (port 80 → pod 8080).
 * The rate-limit logic in the bridge is identical whether the request arrives on
 * HTTP or HTTPS — the Redis bucket is keyed by route, not by protocol.
 *
 * Deploy:
 *   kamel run MultiRouteLoaders.java --name multi-route-loaders -n camel-k \
 *     --config secret:kafka-scram-credentials \
 *     -d camel:http \
 *     -t prometheus.enabled=true -t prometheus.pod-monitor=false
 */
public class MultiRouteLoaders extends RouteBuilder {

    private static final String KAFKA_PARAMS =
        "brokers=kafka.kafka.svc.cluster.local:9092"
        + "&securityProtocol=SASL_PLAINTEXT"
        + "&saslMechanism=SCRAM-SHA-256"
        + "&saslJaasConfig=RAW(org.apache.kafka.common.security.scram.ScramLoginModule required "
        + "username=\"{{kafka.user}}\" password=\"{{kafka.password}}\";)";

    private static final String BRIDGE = "http:multi-route-bridge.camel-k.svc.cluster.local";

    @Override
    public void configure() throws Exception {

        // ── Loader 1: kafka-to-kafka ─────────────────────────────────────────────
        from("timer:kk-load?period=50")
            .routeId("load-kafka-to-kafka")
            .setBody(simple("kk-${date:now:yyyyMMdd-HHmmss.SSS}"))
            .to("kafka:kk-source?" + KAFKA_PARAMS);

        // ── Loader 2: https-to-https ─────────────────────────────────────────────
        from("timer:hh-load?period=50")
            .routeId("load-https-to-https")
            .setBody(simple("hh-${date:now:yyyyMMdd-HHmmss.SSS}"))
            .setHeader("Content-Type", constant("text/plain"))
            .to(BRIDGE + "/ingest/hh?httpMethod=POST&bridgeEndpoint=true&throwExceptionOnFailure=false");

        // ── Loader 3: https-to-kafka ─────────────────────────────────────────────
        from("timer:hk-load?period=50")
            .routeId("load-https-to-kafka")
            .setBody(simple("hk-${date:now:yyyyMMdd-HHmmss.SSS}"))
            .setHeader("Content-Type", constant("text/plain"))
            .to(BRIDGE + "/ingest/hk?httpMethod=POST&bridgeEndpoint=true&throwExceptionOnFailure=false");
    }
}
