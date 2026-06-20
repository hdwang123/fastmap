package com.hdwang.fastmap;

import org.junit.Test;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FastMapAdditionalSafetyTest {

    /**
     * 验证排序写入失败时，HashMap和TreeMap都不会残留数据。
     */
    @Test
    public void failedSortedPutMustNotModifyEitherIndex() {
        FastMap<Object, String> map = new FastMap<>(false, true);

        try {
            map.put(new Object(), "value");
            fail("natural ordering should reject a non-comparable key");
        } catch (ClassCastException expected) {
            assertTrue(map.isEmpty());
            assertTrue(map.entrySet().isEmpty());
        }
    }

    /**
     * 验证putAll中存在非法排序Key时，整次批量写入保持原子性。
     */
    @Test
    public void putAllMustBeAtomicWhenASortedKeyIsInvalid() {
        FastMap<Object, String> map = new FastMap<>(false, true);
        map.put("valid", "old");
        Map<Object, String> additions = new java.util.LinkedHashMap<>();
        additions.put("another", "new");
        additions.put(new Object(), "invalid");

        try {
            map.putAll(additions);
            fail("putAll should reject the invalid key");
        } catch (ClassCastException expected) {
            assertEquals(1, map.size());
            assertEquals("old", map.get("valid"));
            assertFalse(map.containsKey("another"));
        }
    }

    /**
     * 验证Comparator与equals不一致时会拒绝写入冲突Key。
     */
    @Test
    public void comparatorInconsistentWithEqualsMustBeRejected() {
        Comparator<String> sameLength = (left, right) ->
                Integer.compare(left.length(), right.length());
        FastMap<String, Integer> map = new FastMap<>(false, sameLength);
        map.put("a", 1);

        try {
            map.put("b", 2);
            fail("comparator collision should be rejected");
        } catch (IllegalArgumentException expected) {
            assertEquals(1, map.size());
            assertEquals(Integer.valueOf(1), map.get("a"));
        }
    }

    /**
     * 验证初始化方法不会作为公开API暴露，避免重复初始化破坏状态。
     */
    @Test
    public void initializationMustNotBePublic() {
        for (Method method : FastMap.class.getDeclaredMethods()) {
            assertFalse("initialization method must not be public",
                    method.getName().equals("initialize")
                            && Modifier.isPublic(method.getModifiers()));
            assertNotEquals("legacy public init method must not exist", "init", method.getName());
        }
    }

    /**
     * 验证keySet、values和entrySet视图的删除操作会写回原Map。
     */
    @Test
    public void mapViewsMustBeBackedByTheMap() {
        FastMap<Integer, String> map = new FastMap<>(false, true);
        map.put(1, "one");
        map.put(2, "two");
        map.put(3, "three");

        assertTrue(map.keySet().remove(1));
        assertFalse(map.containsKey(1));
        assertTrue(map.values().remove("two"));
        assertFalse(map.containsKey(2));
        assertTrue(map.entrySet().remove(new AbstractMap.SimpleEntry<>(3, "three")));
        assertTrue(map.isEmpty());
    }

    /**
     * 验证Map视图迭代器的remove操作会删除原Map中的数据。
     */
    @Test
    public void mapViewIteratorRemoveMustWriteThrough() {
        FastMap<Integer, String> map = new FastMap<>(false, true);
        map.put(1, "one");
        Iterator<Integer> iterator = map.keySet().iterator();

        assertEquals(Integer.valueOf(1), iterator.next());
        iterator.remove();

        assertTrue(map.isEmpty());
    }

    /**
     * 验证equals和hashCode遵循标准Map内容比较契约。
     */
    @Test
    public void equalsAndHashCodeMustFollowMapContract() {
        FastMap<Integer, String> first = new FastMap<>(false);
        FastMap<Integer, String> second = new FastMap<>(false, true);
        first.put(1, "value");
        second.put(1, "value");

        assertEquals(first, second);
        assertEquals(second, first);
        assertEquals(first.hashCode(), second.hashCode());
    }

    /**
     * 验证全局过期实例注册表使用弱引用，避免阻止实例被回收。
     */
    @Test
    public void expirableRegistryMustUseWeakReferences() throws Exception {
        new FastMap<>();
        Field field = FastMap.class.getDeclaredField("allExpirableFastMaps");
        field.setAccessible(true);
        List<?> references = (List<?>) field.get(null);

        assertFalse(references.isEmpty());
        assertTrue(references.get(references.size() - 1) instanceof WeakReference);
    }

    /**
     * 验证续期会取消旧的定时回调，并只在新截止时间触发一次。
     */
    @Test
    public void renewalMustCancelThePreviousScheduledCallback() throws Exception {
        FastMap<String, String> map = new FastMap<>();
        CountDownLatch callback = new CountDownLatch(1);
        AtomicInteger count = new AtomicInteger();
        map.put("key", "value");
        map.expire("key", 40L, (key, value) -> {
            count.incrementAndGet();
            callback.countDown();
        });
        map.expire("key", 140L);

        Thread.sleep(80L);
        assertEquals(0, count.get());
        assertEquals("value", map.get("key"));
        assertTrue(callback.await(2, TimeUnit.SECONDS));
        assertEquals(1, count.get());
    }

    /**
     * 验证单个过期回调抛出异常不会影响其他回调继续执行。
     */
    @Test
    public void callbackFailureMustNotPreventOtherCallbacks() throws Exception {
        FastMap<String, String> map = new FastMap<>();
        CountDownLatch successfulCallback = new CountDownLatch(1);
        map.put("failing", "value");
        map.put("successful", "value");
        map.expire("failing", 20L, (key, value) -> {
            throw new IllegalStateException("expected test failure");
        });
        map.expire("successful", 30L, (key, value) -> successfulCallback.countDown());

        assertTrue(successfulCallback.await(2, TimeUnit.SECONDS));
    }

    /**
     * 验证toString不会输出已经过期的数据。
     */
    @Test
    public void toStringMustNotExposeExpiredEntries() throws Exception {
        FastMap<String, String> map = new FastMap<>();
        map.put("key", "value");
        map.expire("key", 20L);

        Thread.sleep(40L);
        assertEquals("{}", map.toString());
    }

    /**
     * 验证超大TTL不会因时间加法溢出而立即过期。
     */
    @Test
    public void veryLargeTtlMustNotOverflowIntoImmediateExpiration() {
        FastMap<String, String> map = new FastMap<>();
        map.put("key", "value");

        map.expire("key", Long.MAX_VALUE);

        assertEquals("value", map.get("key"));
        assertTrue(map.ttl("key") > 0);
    }

    /**
     * 验证Map包含自身引用时，toString不会无限递归。
     */
    @Test
    public void toStringMustHandleSelfReference() {
        FastMap<String, Object> map = new FastMap<>(false);
        map.put("self", map);

        assertEquals("{self=(this Map)}", map.toString());
    }
}
