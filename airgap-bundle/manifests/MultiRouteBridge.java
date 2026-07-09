import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

import redis.clients.jedis.JedisPooled;

/**
 * Three independent bridge routes, each with its own Redis token-bucket SLA.
 *
 *  kafka-to-kafka  : Kafka consumer (kk-source) → rate-limited 10 msg/s → Kafka (kk-sink)
 *                    Blocking backpressure: consumer thread waits for a token, nothing dropped.
 *
 *  https-to-https  : HTTPS inbound on :8443 (pod TLS, NodePort 30444)
 *                    → rate-limited 5 req/s → HTTP forward to echo-server → 202 to caller
 *                    The "HTTPS" is the pod-terminated TLS on the listener; the echo
 *                    forward uses plain HTTP (in-cluster). Swap http: for https: plus
 *                    a trust context when forwarding to an external TLS endpoint.
 *
 *  https-to-kafka  : HTTPS inbound on :8443 (same listener, different path)
 *                    → rate-limited 5 req/s → Kafka (hk-sink) → 202 to caller
 *
 * Loaders: MultiRouteLoaders sends one message every 200 ms per route.
 *
 * Deploy:
 *   kamel run MultiRouteBridge.java --name multi-route-bridge -n camel-k \
 *     --config secret:kafka-scram-credentials \
 *     -d mvn:redis.clients:jedis:5.2.0 -d camel:http \
 *     --resource secret:ratelimit-tls@/etc/tls \
 *     -p quarkus.http.ssl-port=8443 \
 *     -p quarkus.http.ssl.certificate.files=/etc/tls/tls.crt \
 *     -p quarkus.http.ssl.certificate.key-files=/etc/tls/tls.key \
 *     -t prometheus.enabled=true -t prometheus.pod-monitor=false
 */
public class MultiRouteBridge extends RouteBuilder {

    private static final String KAFKA_PARAMS =
        "brokers=kafka.kafka.svc.cluster.local:9092"
        + "&securityProtocol=SASL_PLAINTEXT"
        + "&saslMechanism=SCRAM-SHA-256"
        + "&saslJaasConfig=RAW(org.apache.kafka.common.security.scram.ScramLoginModule required "
        + "username=\"{{kafka.user}}\" password=\"{{kafka.password}}\";)";

    @Override
    public void configure() throws Exception {
        RedisTokenBucket limiter = new RedisTokenBucket(
            System.getenv().getOrDefault("REDIS_HOST", "redis.redis.svc.cluster.local"),
            Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379")));

        // ── Route 1: kafka-to-kafka ───────────────────────────────────────────────
        from("kafka:kk-source?groupId=kk-bridge&autoOffsetReset=earliest&" + KAFKA_PARAMS)
            .routeId("kafka-to-kafka")
            .process(e -> limiter.acquireBlocking("kk", 10.0, 20))
            .to("kafka:kk-sink?" + KAFKA_PARAMS);

        // ── Route 2: https-to-https ───────────────────────────────────────────────
        from("platform-http:/ingest/hh")
            .routeId("https-to-https")
            .process(e -> {
                if (!limiter.tryAcquire("hh", 5.0, 10)) {
                    e.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 429);
                    e.getMessage().setHeader("Retry-After", "1");
                    e.getMessage().setBody("rate limit exceeded: https-to-https\n");
                    e.setRouteStop(true);
                }
            })
            .removeHeaders("Camel*")
            .setHeader("Content-Type", constant("text/plain"))
            .to("http:echo-server.camel-k.svc.cluster.local/echo"
                + "?httpMethod=POST&bridgeEndpoint=true&throwExceptionOnFailure=false")
            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
            .setBody(constant("forwarded\n"));

        // ── Route 3: https-to-kafka ───────────────────────────────────────────────
        from("platform-http:/ingest/hk")
            .routeId("https-to-kafka")
            .process(e -> {
                if (!limiter.tryAcquire("hk", 5.0, 10)) {
                    e.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 429);
                    e.getMessage().setHeader("Retry-After", "1");
                    e.getMessage().setBody("rate limit exceeded: https-to-kafka\n");
                    e.setRouteStop(true);
                }
            })
            .to("kafka:hk-sink?" + KAFKA_PARAMS)
            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
            .setBody(constant("accepted\n"));
    }

    // ── Redis token bucket (same atomic Lua script as RateLimitedRoutes) ─────────

    static final class RedisTokenBucket {

        private static final String LUA = """
            local key = KEYS[1]
            local rate = tonumber(ARGV[1])
            local capacity = tonumber(ARGV[2])
            local requested = tonumber(ARGV[3])
            local t = redis.call('TIME')
            local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
            local data = redis.call('HMGET', key, 'tokens', 'ts')
            local tokens = tonumber(data[1])
            local ts = tonumber(data[2])
            if tokens == nil or ts == nil then
              tokens = capacity
              ts = now
            end
            local elapsed = now - ts
            if elapsed < 0 then elapsed = 0 end
            tokens = tokens + elapsed * rate
            if tokens > capacity then tokens = capacity end
            local allowed = 0
            if tokens >= requested then
              tokens = tokens - requested
              allowed = 1
            end
            redis.call('HSET', key, 'tokens', tokens, 'ts', now)
            redis.call('EXPIRE', key, math.max(60, math.ceil(capacity / rate) * 2))
            return allowed
            """;

        private final JedisPooled jedis;

        RedisTokenBucket(String host, int port) {
            this.jedis = new JedisPooled(host, port);
        }

        boolean tryAcquire(String client, double ratePerSecond, int burstCapacity) {
            Object result = jedis.eval(LUA,
                List.of("ratelimit:" + client),
                List.of(Double.toString(ratePerSecond), Integer.toString(burstCapacity), "1"));
            return ((Long) result) == 1L;
        }

        void acquireBlocking(String client, double ratePerSecond, int burstCapacity) {
            while (!tryAcquire(client, ratePerSecond, burstCapacity)) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted waiting for rate-limit token", ie);
                }
            }
        }
    }
}
