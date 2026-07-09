import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;

import redis.clients.jedis.JedisPooled;

/**
 * Per-client rate limiting (SLA) enforced globally via Redis, independent of pod count.
 *
 * Algorithm: token bucket, executed as an atomic Lua script in Redis. Every pod of every
 * replica competes for the same bucket, so the aggregate throughput per client stays at the
 * configured rate no matter how many replicas run. Time is taken from the Redis server
 * (TIME inside the script), so pod clock skew cannot distort the limit.
 *
 * Source semantics:
 *  - Kafka consumers BLOCK until a token is available (backpressure, no data loss).
 *  - HTTP/HTTPS reject over-limit requests with 429 + Retry-After (client retries).
 *
 * Run with:
 *   kamel run rate-limit/RateLimitedRoutes.java --name rate-limited-bridge \
 *     --config secret:kafka-scram-credentials -d mvn:redis.clients:jedis:5.2.0
 */
public class RateLimitedRoutes extends RouteBuilder {

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

        // client-a: Kafka source, SLA 5 msg/s, burst up to 10
        from("kafka:client-a-topic?groupId=client-a&autoOffsetReset=earliest&" + KAFKA_PARAMS)
            .routeId("client-a-kafka")
            .process(e -> limiter.acquireBlocking("client-a", 5.0, 10))
            .to("kafka:client-a-sink?" + KAFKA_PARAMS);

        // client-b: Kafka source, SLA 2 msg/s, burst up to 4
        from("kafka:client-b-topic?groupId=client-b&autoOffsetReset=earliest&" + KAFKA_PARAMS)
            .routeId("client-b-kafka")
            .process(e -> limiter.acquireBlocking("client-b", 2.0, 4))
            .to("kafka:client-b-sink?" + KAFKA_PARAMS);

        // client-c: HTTP(S) source, SLA 3 req/s, burst up to 6 — over-limit requests get 429
        from("platform-http:/ingest/client-c")
            .routeId("client-c-http")
            .process(e -> {
                if (!limiter.tryAcquire("client-c", 3.0, 6)) {
                    e.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 429);
                    e.getMessage().setHeader("Retry-After", "1");
                    e.getMessage().setBody("rate limit exceeded for client-c\n");
                    e.setRouteStop(true);
                }
            })
            .to("kafka:client-c-sink?" + KAFKA_PARAMS)
            .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(202))
            .setBody(constant("accepted\n"));
    }

    static final class RedisTokenBucket {

        // KEYS[1] bucket key; ARGV: rate (tokens/s), capacity, requested.
        // Refills continuously from Redis server time, atomically takes tokens when available.
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

        /** Non-blocking: true if a token was taken, false if the client is over its SLA. */
        boolean tryAcquire(String client, double ratePerSecond, int burstCapacity) {
            Object result = jedis.eval(LUA,
                List.of("ratelimit:" + client),
                List.of(Double.toString(ratePerSecond), Integer.toString(burstCapacity), "1"));
            return ((Long) result) == 1L;
        }

        /** Blocking: waits until a token is available (used for Kafka backpressure). */
        void acquireBlocking(String client, double ratePerSecond, int burstCapacity) {
            while (!tryAcquire(client, ratePerSecond, burstCapacity)) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("interrupted while waiting for rate-limit token", ie);
                }
            }
        }
    }
}
