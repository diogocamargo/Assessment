package org.codurance;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import lombok.extern.log4j.Log4j;
import org.codurance.dao.DistributedKeyValueStore;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

/**
 * A distributed rate limiter capable of handling high throughput situations
 * using Hazelcast-based distributed map for tracking API call rates.
 * <p>
 * The rate limiter initializes a Hazelcast IMap for distributed handling of
 * rate limits, enabling consistent data sharing across a cluster. This class
 * ensures that the service can handle a high volume of requests without
 * allowing any client to exceed the allowed rate limit.
 */
@Log4j
public class DistributedHighThroughputRateLimiter {

    protected static int RATE_LIMITER_SECONDS = 60;

    protected static int BATCH_DELTA = 10;

    private DistributedKeyValueStore store;

    private ConcurrentLinkedDeque<Runnable> incrementCalls = new ConcurrentLinkedDeque<>();

    /**
     * Thread safe
     *
     */
    protected static IMap<String, Integer> rateLimiters;

    static {
        if (Objects.isNull(rateLimiters)) {
            initializeHazelCastMap();
        }
    }

    public DistributedHighThroughputRateLimiter(DistributedKeyValueStore store) {
        this.store = store;

    }


    /**
     * Use of Hazelcast due to throughput is expected to be high
     */
    private static void initializeHazelCastMap() {
        Config config = new Config();
        config.setClusterName("DistributedHighThroughput");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        // Get a distributed map from the cluster
        rateLimiters = hz.getMap("keyMapCalls");
    }

    /**
     * Increments the value associated with the given key by a specified delta and sets an expiration time for the key.
     * The operation is added to a batch of tasks to be processed, and the function returns the result of the batch
     * processing attempt.
     *
     * @param key               the key for which the value should be incremented
     * @param delta             the amount by which to increment the value
     * @param expirationSeconds the time in seconds after which the key should expire
     * @return true if the batch of tasks was processed successfully; false otherwise
     */
    public boolean incrementByAndExpire(String key, int delta, int expirationSeconds) {
        updateCallLimiter(key);
        incrementCalls.addLast(() -> {
            try {
                store.incrementByAndExpire(key, delta, expirationSeconds);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
        return processTasks();
    }

    /**
     * Processes a batch of tasks if the number of pending tasks meets or exceeds a predefined threshold.
     * Iterates through the queue of tasks, executing each task and catching any runtime exceptions
     * that may occur during execution.
     *
     * @return true if all tasks were processed successfully; false if any task failed during execution.
     */
    private boolean processTasks() {
        boolean success = true;
        if (incrementCalls.size() >= BATCH_DELTA) {
            while (!incrementCalls.isEmpty()) {
                Runnable task = incrementCalls.pollFirst(); // Get and remove first task
                if (task != null) {
                    try {
                        task.run();
                    } catch (RuntimeException e) {
                        success = false;
                    }
                }
            }
        }
        return success;
    }

    /**
     * Updates the call limiter for a specific key by incrementing the current count and
     * setting the time-to-live (TTL) for the key in the rate limiter map.
     *
     * @param key the unique identifier for which the call limiter should be updated
     */
    private static void updateCallLimiter(String key) {
        rateLimiters.putIfAbsent(key, 0);
        Integer numCalls = rateLimiters.get(key);
        if (numCalls == null) {
            numCalls = 0;
        }
        numCalls++;
        //Sync hazelcast map
        rateLimiters.put(key, numCalls, RATE_LIMITER_SECONDS, TimeUnit.SECONDS);

    }

    public CompletableFuture<Boolean> isAllowed(String key, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            if (!rateLimiters.containsKey(key)) {
                return true;
            }
            Integer numCalls = rateLimiters.get(key);
            if (numCalls <= limit) {
                return true;
            }
            return false;
        });
    }
}
