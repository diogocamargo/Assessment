package org.codurance;

import org.codurance.dao.DistributedKeyValueStore;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DistributedHighThroughputRateLimiterTest {

    private DistributedKeyValueStore mockStore;

    private DistributedHighThroughputRateLimiter rateLimiter;

    @BeforeEach
    void setup() {
        // Setup a mock for DistributedKeyValueStore
        mockStore = Mockito.mock(DistributedKeyValueStore.class);

        // Initialize the rate limiter with the mock store
        rateLimiter = new DistributedHighThroughputRateLimiter(mockStore);
        //Reset map
        rateLimiter.rateLimiters.keySet().forEach(s -> rateLimiter.rateLimiters.remove(s));
        //Rest time
        rateLimiter.RATE_LIMITER_SECONDS = 60;
        rateLimiter.BATCH_DELTA = 10;
    }

    @Test
    public void testIncrementByAndExpire_Success() throws Exception {

        Mockito.when(mockStore.incrementByAndExpire(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(CompletableFuture.completedFuture(1));

        // Execute the method being tested
        boolean result = rateLimiter.incrementByAndExpire("testKey", 1, 60);

        // Assert the expected outcome
        assertTrue(result, "The incrementByAndExpire should return true when the operation succeeds");
    }

    @Test
    public void testIncrementByAndExpire_Failure() throws Exception {
        Mockito.when(mockStore.incrementByAndExpire(Mockito.anyString(), Mockito.anyInt(), Mockito.anyInt()))
                .thenThrow(new RuntimeException("Store exception"));

        //Set 1  as batch size
        rateLimiter.BATCH_DELTA = 1;

        // Execute the method being tested
        boolean result = rateLimiter.incrementByAndExpire("testKey", 1, 60);


        // Assert the expected outcome
        assertFalse(result, "The incrementByAndExpire should return false when an exception is thrown");
    }


    @Test
    public void testIsAllowed_FirstCall_True() throws ExecutionException, InterruptedException {
        CompletableFuture<Boolean> allowed = rateLimiter.isAllowed("xyz", 500);
        assertTrue(allowed.get().booleanValue());
    }

    @Test
    public void testIsAllowed_True() throws ExecutionException, InterruptedException {
        rateLimiter.incrementByAndExpire("xyz", 1, 60);
        rateLimiter.incrementByAndExpire("xyz", 1, 60);
        CompletableFuture<Boolean> allowed = rateLimiter.isAllowed("xyz", 2);
        assertTrue(allowed.get().booleanValue());
    }

    @Test
    public void testIsAllowed_False() throws ExecutionException, InterruptedException {
        rateLimiter.incrementByAndExpire("xyz", 1, 60);
        rateLimiter.incrementByAndExpire("xyz", 1, 60);
        CompletableFuture<Boolean> allowed = rateLimiter.isAllowed("xyz", 1);
        assertFalse(allowed.get().booleanValue());
    }

    @Test
    public void testIsAllowed_TimeLimit_True() throws ExecutionException, InterruptedException {
        //Set limit as 1s
        rateLimiter.RATE_LIMITER_SECONDS = 1;
        rateLimiter.incrementByAndExpire("xyz", 1, 60);
        rateLimiter.incrementByAndExpire("xyz", 1, 60);
        Thread.sleep(2000);
        CompletableFuture<Boolean> allowed = rateLimiter.isAllowed("xyz", 1);
        assertTrue(allowed.get().booleanValue());
    }

    @Test
    public void testIsAllowed_True_MultipleThreads() {

        List<CompletableFuture<Boolean>> futures = getCompletableFutures(500);

        // Collect the results from the completed futures
        Optional<Boolean> findFalse = futures.stream()
                .map(CompletableFuture::join) // Use join() to get results after allOf() is done
                .filter(aBoolean -> Boolean.FALSE.equals(aBoolean))
                .findAny();

        assertFalse(findFalse.isPresent(), "All Calls were allowed");
    }

    @Test
    public void testIsAllowed_False_MultipleThreads() {

        List<CompletableFuture<Boolean>> futures = getCompletableFutures(501);

        // Collect the results from the completed futures
        Optional<Boolean> findFalse = futures.stream()
                .map(CompletableFuture::join) // Use join() to get results after allOf() is done
                .filter(aBoolean -> Boolean.FALSE.equals(aBoolean))
                .findAny();

        assertTrue(findFalse.isPresent(), "One call was not allowed");
    }

    private @NonNull List<CompletableFuture<Boolean>> getCompletableFutures(int totalCalls) {
        List<CompletableFuture<Boolean>> futures = IntStream.range(0, totalCalls).mapToObj(index -> {
            rateLimiter.incrementByAndExpire("xyz", 1, 60);
            return rateLimiter.isAllowed("xyz", 500);
        }).collect(Collectors.toList());

        // Combine all futures into a single CompletableFuture<Void>
        CompletableFuture<Void> allOf = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        // Wait for completion (blocking call, acceptable in unit tests)
        allOf.join();
        return futures;
    }

}



