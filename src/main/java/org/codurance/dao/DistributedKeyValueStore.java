package org.codurance.dao;

import java.util.concurrent.CompletableFuture;

public interface DistributedKeyValueStore {
    /**
     * Increment a given key by some count and set the duration that the counter should persist. At the end of
     * the specified duration, the counter is automatically deleted. If a key does not exist, it will be
     * initialized to zero for you prior to incrementing the value. The expiration is only set when the
     * counter is initialized; that is, the parameter is ignored after a key is
     * incremented until it expires, at which time the it may be set again.
     * @param key
     * @param delta
     * @param expirationSeconds
     * @return
     * @throws Exception
     */
    public CompletableFuture<Integer> incrementByAndExpire(String key, int delta, int expirationSeconds) throws Exception;
}
