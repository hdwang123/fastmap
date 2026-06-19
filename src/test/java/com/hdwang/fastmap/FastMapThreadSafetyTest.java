package com.hdwang.fastmap;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class FastMapThreadSafetyTest {

    @Test(timeout = 5000)
    public void defaultMethodsMustNotDeadlockWhenExpirationIsEnabled() {
        FastMap<String, Integer> map = new FastMap<>();
        map.put("key", 1);

        assertEquals(Integer.valueOf(1), map.getOrDefault("key", 0));
        AtomicInteger sum = new AtomicInteger();
        map.forEach((key, value) -> sum.addAndGet(value));
        assertEquals(1, sum.get());
    }

    @Test
    public void replaceAllMustKeepSortedAndHashIndexesConsistent() {
        FastMap<Integer, Integer> map = new FastMap<>(false, true);
        map.put(1, 10);
        map.put(2, 20);

        map.replaceAll((key, value) -> value + 1);

        assertEquals(Integer.valueOf(11), map.get(1));
        assertEquals(Integer.valueOf(21), map.get(2));
        assertEquals(Integer.valueOf(11), map.subMap(1, 2).get(1));
        assertEquals(Integer.valueOf(21), map.subMap(2, 3).get(2));
    }

    @Test
    public void entrySetMustWriteThroughWithoutBreakingIndexes() {
        FastMap<Integer, Integer> map = new FastMap<>(false, true);
        map.put(1, 10);
        Map.Entry<Integer, Integer> entry = map.entrySet().iterator().next();

        assertEquals(Integer.valueOf(10), entry.setValue(99));
        assertEquals(Integer.valueOf(99), map.get(1));
        assertEquals(Integer.valueOf(99), map.subMap(1, 2).get(1));
    }

    @Test
    public void removeAndClearMustCancelOldExpirationMetadata() throws Exception {
        FastMap<String, String> removedMap = new FastMap<>();
        removedMap.put("key", "old");
        removedMap.expire("key", 80L);
        removedMap.remove("key");
        removedMap.put("key", "new");

        FastMap<String, String> clearedMap = new FastMap<>();
        clearedMap.put("key", "old");
        clearedMap.expire("key", 80L);
        clearedMap.clear();
        clearedMap.put("key", "new");

        Thread.sleep(140L);
        assertEquals("new", removedMap.get("key"));
        assertEquals("new", clearedMap.get("key"));
    }

    @Test
    public void expirationForMissingKeyMustNotAffectLaterInsert() throws Exception {
        FastMap<String, String> map = new FastMap<>();

        assertNull(map.expire("key", 50L));
        map.put("key", "value");
        Thread.sleep(90L);

        assertEquals("value", map.get("key"));
    }

    @Test
    public void renewedExpirationMustIgnoreTheOldDeadline() throws Exception {
        FastMap<String, String> map = new FastMap<>();
        AtomicInteger callbackCount = new AtomicInteger();
        map.put("key", "value");
        map.expire("key", 60L, (key, value) -> callbackCount.incrementAndGet());

        Thread.sleep(30L);
        map.expire("key", 250L);
        Thread.sleep(90L);

        assertEquals("value", map.get("key"));
        assertEquals(0, callbackCount.get());
    }

    @Test
    public void nullValueExpirationMustInvokeCallbackExactlyOnce() throws Exception {
        FastMap<String, String> map = new FastMap<>();
        CountDownLatch callback = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();
        map.put("key", null);
        map.expire("key", 30L, (key, value) -> {
            assertNull(value);
            callbackCount.incrementAndGet();
            callback.countDown();
        });

        assertTrue("expiration callback was not invoked",
                callback.await(2, TimeUnit.SECONDS));
        Thread.sleep(50L);
        assertEquals(1, callbackCount.get());
        assertFalse(map.containsKey("key"));
    }

    @Test
    public void concurrentComputeMustBeAtomic() throws Exception {
        final int threads = 12;
        final int incrementsPerThread = 3000;
        FastMap<String, Integer> map = new FastMap<>(false);
        map.put("counter", 0);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                try {
                    start.await();
                    for (int j = 0; j < incrementsPerThread; j++) {
                        map.compute("counter", (key, value) -> value + 1);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
        }

        start.countDown();
        executor.shutdown();
        assertTrue("workers did not finish", executor.awaitTermination(10, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(Integer.valueOf(threads * incrementsPerThread), map.get("counter"));
    }

    @Test
    public void concurrentReadWriteExpireAndRenewMustRemainStable() throws Exception {
        final int keyCount = 100;
        final int workers = 8;
        final int iterations = 2500;
        FastMap<Integer, Integer> map = new FastMap<>(true, true);
        for (int i = 0; i < keyCount; i++) {
            map.put(i, i);
            map.expire(i, 200L);
        }

        ExecutorService executor = Executors.newFixedThreadPool(workers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int worker = 0; worker < workers; worker++) {
            final int workerId = worker;
            executor.execute(() -> {
                try {
                    start.await();
                    for (int i = 0; i < iterations; i++) {
                        int key = (i + workerId) % keyCount;
                        map.put(key, i);
                        map.expire(key, 200L);
                        map.get(key);
                        map.ttl(key);
                        map.keySet();
                        map.entrySet();
                        map.subMap(0, keyCount);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
        }

        start.countDown();
        executor.shutdown();
        assertTrue("workers did not finish", executor.awaitTermination(20, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertEquals(map.keySet().size(), map.entrySet().size());
        assertEquals(map.keySet().size(), map.subMap(0, keyCount).size());
    }
}
