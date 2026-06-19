package com.hdwang.fastmap;

import java.util.*;
import java.lang.ref.WeakReference;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 一个支持等值查找、范围查找、数据过期、键排序等功能的线程安全Map，适合做本地缓存。
 *
 * @author wanghuidong
 * 时间： 2022/6/26 10:10
 */
public class FastMap<K, V> implements IFastMap<K, V> {

    /**
     * 保存数据，主要运用于等值查找
     */
    private HashMap<K, V> dataHashMap = new HashMap<>();

    /**
     * 保存数据，主要运用于范围查找
     */
    private TreeMap<K, V> dataTreeMap = null;

    /**
     * 按照时间顺序保存了会过期key集合，为了实现快速删除，结构：时间戳->key列表
     */
    private final TreeMap<Long, List<K>> expireKeysMap = new TreeMap<>();

    /**
     * 保存会过期key的过期时间
     */
    private final HashMap<K, Long> keyExpireMap = new HashMap<>();

    /**
     * 保存键过期的回调函数
     */
    private final HashMap<K, ExpireCallback<K, V>> keyExpireCallbackMap = new HashMap<>();

    /**
     * 保存带回调过期任务，续期或删除时取消旧任务。
     */
    private final HashMap<K, ScheduledFuture<?>> keyExpireFutureMap = new HashMap<>();

    /**
     * 是否启用排序（默认不启用）
     */
    private final boolean enableSort;

    /**
     * 是否启用数据过期功能（默认启用）
     */
    private final boolean enableExpire;

    /**
     * The comparator used to maintain order in this tree map, or
     * null if it uses the natural ordering of its keys.
     *
     * @serial
     */
    private final Comparator<? super K> comparator;

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    //数据写锁
    private final Lock dataWriteLock = readWriteLock.writeLock();

    //数据读锁
    private final Lock dataReadLock = readWriteLock.readLock();

    /**
     * 定时执行服务(全局共享线程池)
     */
    private static volatile ScheduledThreadPoolExecutor scheduledExecutorService;

    /**
     * 保存所有的启用过期功能的FastMap实例, 用于定期清理过期数据
     */
    private static final List<WeakReference<FastMap<?, ?>>> allExpirableFastMaps =
            new CopyOnWriteArrayList<>();

    /**
     * 定期清理过期数据线程编号
     */
    private final static AtomicInteger expireTaskThreadNumber = new AtomicInteger(0);

    /**
     * 过期回调线程编号
     */
    private final static AtomicInteger callbackThreadNumber = new AtomicInteger(0);

    /**
     * 受控的过期回调线程池，避免大量数据同时过期时无限创建线程。
     */
    private static final ThreadPoolExecutor callbackExecutor = new ThreadPoolExecutor(
            1,
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1024),
            runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("expireCallbackThread-" + callbackThreadNumber.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            },
            new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * 100万，1毫秒=100万纳秒
     */
    private static final int ONE_MILLION = 100_0000;

    /**
     * 默认构造器，启用过期，不启用排序
     */
    public FastMap() {
        this.comparator = null;
        this.enableSort = false;
        this.enableExpire = true;
        this.initialize();
    }

    /**
     * 构造器，enableExpire配置是否启用过期，不启用排序
     *
     * @param enableExpire 是否启用过期
     */
    public FastMap(boolean enableExpire) {
        this.comparator = null;
        this.enableSort = false;
        this.enableExpire = enableExpire;
        this.initialize();
    }

    /**
     * 构造器，enableExpire配置是否启用过期，enableSort配置是否启用排序
     *
     * @param enableExpire 是否启用过期
     * @param enableSort   是否启用排序
     */
    public FastMap(boolean enableExpire, boolean enableSort) {
        this.comparator = null;
        this.enableExpire = enableExpire;
        this.enableSort = enableSort;
        this.initialize();
    }

    /**
     * 构造器，enableExpire配置是否启用过期，启用排序，排序器由自己传入
     *
     * @param enableExpire 是否启用过期
     * @param comparator   排序器
     */
    public FastMap(boolean enableExpire, Comparator<? super K> comparator) {
        this.enableExpire = enableExpire;
        this.comparator = comparator;
        this.enableSort = true;
        this.initialize();
    }

    /**
     * 初始化
     */
    private void initialize() {
        dataTreeMap = newTreeMap();

        //启用数据过期功能
        if (this.enableExpire) {
            //保存 启用过期功能的FastMap 实例
            allExpirableFastMaps.add(new WeakReference<>(this));

            //双重校验构造一个单例的scheduledExecutorService
            if (scheduledExecutorService == null) {
                synchronized (FastMap.class) {
                    if (scheduledExecutorService == null) {
                        //启用定时器，定时删除过期key,1秒后启动，定时1秒, 因为时间间隔计算基于nanoTime,比timer.schedule更靠谱
                        scheduledExecutorService = new ScheduledThreadPoolExecutor(1, runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setName("expireTask-" + expireTaskThreadNumber.getAndIncrement());
                            thread.setDaemon(true);
                            return thread;
                        });
                        scheduledExecutorService.setRemoveOnCancelPolicy(true);
//                        System.out.println("ScheduledExecutorService created.");

                        //执行定期清理过期数据任务,清理所有FastMap实例中的过期数据
                        scheduledExecutorService.scheduleWithFixedDelay(() -> {
                            for (WeakReference<FastMap<?, ?>> reference : allExpirableFastMaps) {
                                FastMap<?, ?> map = reference.get();
                                if (map == null) {
                                    allExpirableFastMaps.remove(reference);
                                    continue;
                                }
                                try {
                                    map.clearExpireData("expireTask");
                                } catch (Throwable throwable) {
                                    reportBackgroundFailure("expire cleanup failed", throwable);
                                }
                            }
                        }, 1, 1, TimeUnit.SECONDS);
                    }
                }
            }
        }
//        System.out.println("FastMap init succeed. hashcode=" + this.hashCode());
    }


    @Override
    public Comparator<? super K> comparator() {
        return this.comparator;
    }

    @Override
    public Map<K, V> subMap(K fromKey, K toKey) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("subMap");
        try {
            dataReadLock.lock();
            SortedMap<K, V> sortedMap = this.dataTreeMap.subMap(fromKey, toKey);

            //转成LinkedHashMap，解决并发时的遍历问题
            return getLinkedMap(sortedMap);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public Map<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("subMap");
        try {
            dataReadLock.lock();
            SortedMap<K, V> sortedMap = this.dataTreeMap.subMap(fromKey, fromInclusive, toKey, toInclusive);

            //转成LinkedHashMap，解决并发时的遍历问题
            return getLinkedMap(sortedMap);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public Map<K, V> headMap(K toKey) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("headMap");
        try {
            dataReadLock.lock();
            SortedMap<K, V> sortedMap = this.dataTreeMap.headMap(toKey);
            //转成LinkedHashMap，解决并发时的遍历问题
            return getLinkedMap(sortedMap);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public Map<K, V> headMap(K toKey, boolean inclusive) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("headMap");
        try {
            dataReadLock.lock();
            SortedMap<K, V> sortedMap = this.dataTreeMap.headMap(toKey, inclusive);
            //转成LinkedHashMap，解决并发时的遍历问题
            return getLinkedMap(sortedMap);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public Map<K, V> tailMap(K fromKey) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("tailMap");
        try {
            dataReadLock.lock();
            SortedMap<K, V> sortedMap = this.dataTreeMap.tailMap(fromKey);
            //转成LinkedHashMap，解决并发时的遍历问题
            return getLinkedMap(sortedMap);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public Map<K, V> tailMap(K fromKey, boolean inclusive) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("tailMap");
        try {
            dataReadLock.lock();
            SortedMap<K, V> sortedMap = this.dataTreeMap.tailMap(fromKey, inclusive);
            //转成LinkedHashMap，解决并发时的遍历问题
            return getLinkedMap(sortedMap);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public K firstKey() {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("firstKey");
        try {
            dataReadLock.lock();
            return this.dataTreeMap.firstKey();
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public K lastKey() {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.clearExpireData("lastKey");
        try {
            dataReadLock.lock();
            return this.dataTreeMap.lastKey();
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public int size() {
        //先删除过期数据
        this.clearExpireData("size");
        try {
            dataReadLock.lock();
            return this.dataHashMap.size();
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        //先删除过期数据
        this.clearExpireData("isEmpty");
        try {
            dataReadLock.lock();
            return this.dataHashMap.isEmpty();
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        //先删除过期数据
        this.clearExpireData("containsKey");
        try {
            dataReadLock.lock();
            return this.dataHashMap.containsKey(key);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        //先删除过期数据
        this.clearExpireData("containsValue");
        try {
            dataReadLock.lock();
            return this.dataHashMap.containsValue(value);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public V get(Object key) {
        //先删除过期数据
        this.clearExpireData("get");
        try {
            dataReadLock.lock();
            return this.dataHashMap.get(key);
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            dataWriteLock.lock();
            if (!enableSort) {
                return this.dataHashMap.put(key, value);
            }

            validateSortedKey(this.dataHashMap, key);
            boolean treeContainedKey = this.dataTreeMap.containsKey(key);
            V previousTreeValue = this.dataTreeMap.put(key, value);
            try {
                return this.dataHashMap.put(key, value);
            } catch (RuntimeException | Error failure) {
                try {
                    if (treeContainedKey) {
                        this.dataTreeMap.put(key, previousTreeValue);
                    } else {
                        this.dataTreeMap.remove(key);
                    }
                } catch (RuntimeException | Error rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public V remove(Object key) {
        try {
            dataWriteLock.lock();
            if (!enableSort) {
                V value = this.dataHashMap.remove(key);
                if (enableExpire) {
                    removeExpireMetadata(key);
                }
                return value;
            }

            boolean treeContainedKey = this.dataTreeMap.containsKey(key);
            V previousTreeValue = this.dataTreeMap.remove(key);
            V value;
            try {
                value = this.dataHashMap.remove(key);
            } catch (RuntimeException | Error failure) {
                try {
                    if (treeContainedKey) {
                        @SuppressWarnings("unchecked")
                        K typedKey = (K) key;
                        this.dataTreeMap.put(typedKey, previousTreeValue);
                    }
                } catch (RuntimeException | Error rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
            if (enableExpire) {
                removeExpireMetadata(key);
            }
            return value;
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        Objects.requireNonNull(m, "map");
        try {
            dataWriteLock.lock();
            if (!enableSort) {
                this.dataHashMap.putAll(m);
                return;
            }

            HashMap<K, V> newHashMap = new HashMap<>(this.dataHashMap);
            TreeMap<K, V> newTreeMap = newTreeMap();
            newTreeMap.putAll(this.dataTreeMap);
            for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
                K key = entry.getKey();
                validateSortedKey(newHashMap, key);
                newTreeMap.put(key, entry.getValue());
                newHashMap.put(key, entry.getValue());
            }
            this.dataHashMap = newHashMap;
            this.dataTreeMap = newTreeMap;
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public void clear() {
        try {
            dataWriteLock.lock();
            this.dataHashMap.clear();
            if (enableSort) {
                this.dataTreeMap.clear();
            }
            if (enableExpire) {
                for (ScheduledFuture<?> future : this.keyExpireFutureMap.values()) {
                    future.cancel(false);
                }
                this.expireKeysMap.clear();
                this.keyExpireMap.clear();
                this.keyExpireCallbackMap.clear();
                this.keyExpireFutureMap.clear();
            }
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public Set<K> keySet() {
        return new KeySetView();
    }

    @Override
    public Collection<V> values() {
        return new ValuesView();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySetView();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        this.clearExpireData("getOrDefault");
        try {
            dataReadLock.lock();
            V v = this.dataHashMap.get(key);
            return (v != null || this.dataHashMap.containsKey(key))
                    ? v
                    : defaultValue;
        } finally {
            dataReadLock.unlock();
        }
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        Objects.requireNonNull(action);
        for (Map.Entry<K, V> entry : entrySet()) {
            action.accept(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        Objects.requireNonNull(function);
        this.clearExpireData("replaceAll");
        try {
            dataWriteLock.lock();
            for (Map.Entry<K, V> entry : this.dataHashMap.entrySet()) {
                V newValue = function.apply(entry.getKey(), entry.getValue());
                entry.setValue(newValue);
                if (enableSort) {
                    this.dataTreeMap.put(entry.getKey(), newValue);
                }
            }
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        try {
            dataWriteLock.lock();
            V v = get(key);
            if (v == null) {
                v = put(key, value);
            }
            return v;
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public boolean remove(Object key, Object value) {
        try {
            dataWriteLock.lock();
            Object curValue = get(key);
            if (!Objects.equals(curValue, value) ||
                    (curValue == null && !containsKey(key))) {
                return false;
            }
            remove(key);
            return true;
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        try {
            dataWriteLock.lock();
            Object curValue = get(key);
            if (!Objects.equals(curValue, oldValue) ||
                    (curValue == null && !containsKey(key))) {
                return false;
            }
            put(key, newValue);
            return true;
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public V replace(K key, V value) {
        try {
            dataWriteLock.lock();
            V curValue;
            if (((curValue = get(key)) != null) || containsKey(key)) {
                curValue = put(key, value);
            }
            return curValue;
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        try {
            dataWriteLock.lock();
            Objects.requireNonNull(mappingFunction);
            V v;
            if ((v = get(key)) == null) {
                V newValue;
                if ((newValue = mappingFunction.apply(key)) != null) {
                    put(key, newValue);
                    return newValue;
                }
            }

            return v;
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        try {
            dataWriteLock.lock();
            Objects.requireNonNull(remappingFunction);
            V oldValue;
            if ((oldValue = get(key)) != null) {
                V newValue = remappingFunction.apply(key, oldValue);
                if (newValue != null) {
                    put(key, newValue);
                    return newValue;
                } else {
                    remove(key);
                    return null;
                }
            } else {
                return null;
            }
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        try {
            dataWriteLock.lock();
            Objects.requireNonNull(remappingFunction);
            V oldValue = get(key);

            V newValue = remappingFunction.apply(key, oldValue);
            if (newValue == null) {
                // delete mapping
                if (oldValue != null || containsKey(key)) {
                    // something to remove
                    remove(key);
                    return null;
                } else {
                    // nothing to do. Leave things as they were.
                    return null;
                }
            } else {
                // add or replace old mapping
                put(key, newValue);
                return newValue;
            }
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        try {
            dataWriteLock.lock();
            Objects.requireNonNull(remappingFunction);
            Objects.requireNonNull(value);
            V oldValue = get(key);
            V newValue = (oldValue == null) ? value :
                    remappingFunction.apply(oldValue, value);
            if (newValue == null) {
                remove(key);
            } else {
                put(key, newValue);
            }
            return newValue;
        } finally {
            dataWriteLock.unlock();
        }
    }


    @Override
    public Long expire(K key, Long ms) {
        return this.expire(key, ms, null);
    }

    @Override
    public Long expire(K key, Long ms, ExpireCallback<K, V> callback) {
        if (!enableExpire) {
            throw new RuntimeException("未启用过期功能");
        }
        Objects.requireNonNull(ms, "ms");
        if (ms < 0) {
            throw new IllegalArgumentException("ms must be greater than or equal to 0");
        }
        try {
            dataWriteLock.lock();
            if (!this.dataHashMap.containsKey(key)) {
                return null;
            }

            //判断是否已经设置过过期时间
            Long expireTime = this.keyExpireMap.get(key);
            ExpireCallback<K, V> effectiveCallback = callback != null
                    ? callback
                    : this.keyExpireCallbackMap.get(key);
            ScheduledFuture<?> previousFuture = this.keyExpireFutureMap.remove(key);
            if (previousFuture != null) {
                previousFuture.cancel(false);
            }
            if (expireTime != null) {
                //清除之前设置的过期时间
                this.keyExpireMap.remove(key);
                List<K> keys = this.expireKeysMap.get(expireTime);
                if (keys != null) {
                    keys.remove(key);
                    if (keys.isEmpty()) {
                        this.expireKeysMap.remove(expireTime);
                    }
                }
            }
            //使用nanoTime消除系统时间的影响，转成毫秒存储降低timeKey数量,过期时间精确到毫秒级别
            expireTime = saturatedAdd(System.nanoTime() / ONE_MILLION, ms);
            this.keyExpireMap.put(key, expireTime);
            List<K> keys = this.expireKeysMap.get(expireTime);
            if (keys == null) {
                keys = new ArrayList<>();
                keys.add(key);
                this.expireKeysMap.put(expireTime, keys);
            } else {
                keys.add(key);
            }
            if (effectiveCallback != null) {
                //设置的过期回调函数
                this.keyExpireCallbackMap.put(key, effectiveCallback);
                WeakReference<FastMap<K, V>> mapReference = new WeakReference<>(this);
                ScheduledFuture<?> future = this.scheduledExecutorService.schedule(() -> {
                    FastMap<K, V> map = mapReference.get();
                    if (map != null) {
                        try {
                            map.clearExpireData("keyExpireCallback");
                        } catch (Throwable throwable) {
                            reportBackgroundFailure("key expiration cleanup failed", throwable);
                        }
                    }
                }, ms, TimeUnit.MILLISECONDS);
                this.keyExpireFutureMap.put(key, future);
            } else {
                this.keyExpireCallbackMap.remove(key);
            }

            //假定系统时间不修改前提下的过期时间
            return saturatedAdd(System.currentTimeMillis(), ms);
        } finally {
            dataWriteLock.unlock();
        }
    }

    @Override
    public Long ttl(K key) {
        if (!enableExpire) {
            throw new RuntimeException("未启用过期功能");
        }
        this.clearExpireData("ttl");
        try {
            dataReadLock.lock();
            if (!this.dataHashMap.containsKey(key)) {
                return null;
            }
            Long expireTime = this.keyExpireMap.get(key);
            if (expireTime == null) {
                return null;
            }
            return (expireTime - System.nanoTime() / ONE_MILLION);
        } finally {
            dataReadLock.unlock();
        }
    }

    /**
     * 清理过期的数据
     * 调用时机：
     * 1.调用FastMap相关查询接口
     * 2.每秒定时调用
     * 3.设置了过期回调函数的key的延时任务调用
     */
    private void clearExpireData(String flag) {
        if (!enableExpire) {
            return;
        }
        Long curTimestamp = System.nanoTime() / ONE_MILLION;
        List<ExpiredEntry<K, V>> expiredEntries = new ArrayList<>();
        try {
            dataWriteLock.lock();
            NavigableMap<Long, List<K>> expiredKeysMap =
                    this.expireKeysMap.headMap(curTimestamp, true);
            List<Long> expiredTimes = new ArrayList<>(expiredKeysMap.keySet());
            for (Long expireTime : expiredTimes) {
                List<K> storedKeys = this.expireKeysMap.get(expireTime);
                if (storedKeys == null) {
                    continue;
                }
                for (K key : new ArrayList<>(storedKeys)) {
                    if (!Objects.equals(this.keyExpireMap.get(key), expireTime)) {
                        continue;
                    }
                    boolean existed = this.dataHashMap.containsKey(key);
                    V value = this.dataHashMap.remove(key);
                    if (enableSort) {
                        this.dataTreeMap.remove(key);
                    }
                    ExpireCallback<K, V> callback = this.keyExpireCallbackMap.remove(key);
                    ScheduledFuture<?> future = this.keyExpireFutureMap.remove(key);
                    if (future != null) {
                        future.cancel(false);
                    }
                    this.keyExpireMap.remove(key);
                    if (existed && callback != null) {
                        expiredEntries.add(new ExpiredEntry<>(key, value, callback));
                    }
                }
                this.expireKeysMap.remove(expireTime);
            }
        } finally {
            dataWriteLock.unlock();
        }

        for (ExpiredEntry<K, V> entry : expiredEntries) {
            callbackExecutor.execute(() -> {
                try {
                    entry.callback.onExpire(entry.key, entry.value);
                } catch (Throwable throwable) {
                    reportBackgroundFailure("expiration callback failed", throwable);
                }
            });
        }
    }

    private void removeExpireMetadata(Object key) {
        ScheduledFuture<?> future = this.keyExpireFutureMap.remove(key);
        if (future != null) {
            future.cancel(false);
        }
        Long expireTime = this.keyExpireMap.remove(key);
        this.keyExpireCallbackMap.remove(key);
        if (expireTime == null) {
            return;
        }
        List<K> keys = this.expireKeysMap.get(expireTime);
        if (keys != null) {
            keys.remove(key);
            if (keys.isEmpty()) {
                this.expireKeysMap.remove(expireTime);
            }
        }
    }

    private List<Map.Entry<K, V>> entrySnapshot() {
        clearExpireData("entrySnapshot");
        try {
            dataReadLock.lock();
            Map<K, V> source = enableSort ? dataTreeMap : dataHashMap;
            List<Map.Entry<K, V>> snapshot = new ArrayList<>(source.size());
            for (Map.Entry<K, V> entry : source.entrySet()) {
                snapshot.add(new AbstractMap.SimpleImmutableEntry<>(entry));
            }
            return snapshot;
        } finally {
            dataReadLock.unlock();
        }
    }

    private abstract class SnapshotIterator<T> implements Iterator<T> {
        private final Iterator<Map.Entry<K, V>> iterator = entrySnapshot().iterator();
        private K currentKey;
        private boolean canRemove;

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        protected Map.Entry<K, V> nextEntry() {
            Map.Entry<K, V> entry = iterator.next();
            currentKey = entry.getKey();
            canRemove = true;
            return entry;
        }

        @Override
        public void remove() {
            if (!canRemove) {
                throw new IllegalStateException();
            }
            FastMap.this.remove(currentKey);
            canRemove = false;
        }
    }

    private final class KeySetView extends AbstractSet<K> {
        @Override
        public Iterator<K> iterator() {
            return new SnapshotIterator<K>() {
                @Override
                public K next() {
                    return nextEntry().getKey();
                }
            };
        }

        @Override
        public int size() {
            return FastMap.this.size();
        }

        @Override
        public boolean contains(Object key) {
            return FastMap.this.containsKey(key);
        }

        @Override
        public boolean remove(Object key) {
            boolean existed = FastMap.this.containsKey(key);
            if (existed) {
                FastMap.this.remove(key);
            }
            return existed;
        }

        @Override
        public void clear() {
            FastMap.this.clear();
        }
    }

    private final class ValuesView extends AbstractCollection<V> {
        @Override
        public Iterator<V> iterator() {
            return new SnapshotIterator<V>() {
                @Override
                public V next() {
                    return nextEntry().getValue();
                }
            };
        }

        @Override
        public int size() {
            return FastMap.this.size();
        }

        @Override
        public boolean contains(Object value) {
            return FastMap.this.containsValue(value);
        }

        @Override
        public boolean remove(Object value) {
            for (Map.Entry<K, V> entry : entrySnapshot()) {
                if (Objects.equals(entry.getValue(), value)
                        && FastMap.this.remove(entry.getKey(), entry.getValue())) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void clear() {
            FastMap.this.clear();
        }
    }

    private final class EntrySetView extends AbstractSet<Map.Entry<K, V>> {
        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return new SnapshotIterator<Map.Entry<K, V>>() {
                @Override
                public Map.Entry<K, V> next() {
                    Map.Entry<K, V> entry = nextEntry();
                    return new WriteThroughEntry(entry.getKey(), entry.getValue());
                }
            };
        }

        @Override
        public int size() {
            return FastMap.this.size();
        }

        @Override
        public boolean contains(Object object) {
            if (!(object instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
            Object value = FastMap.this.get(entry.getKey());
            return Objects.equals(value, entry.getValue())
                    && (value != null || FastMap.this.containsKey(entry.getKey()));
        }

        @Override
        public boolean remove(Object object) {
            if (!(object instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
            return FastMap.this.remove(entry.getKey(), entry.getValue());
        }

        @Override
        public void clear() {
            FastMap.this.clear();
        }
    }

    private final class WriteThroughEntry implements Map.Entry<K, V> {
        private final K key;
        private V value;

        private WriteThroughEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V previous = FastMap.this.put(key, value);
            this.value = value;
            return previous;
        }

        @Override
        public boolean equals(Object object) {
            if (!(object instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
            return Objects.equals(key, entry.getKey())
                    && Objects.equals(value, entry.getValue());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(key) ^ Objects.hashCode(value);
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }

    private static final class ExpiredEntry<K, V> {
        private final K key;
        private final V value;
        private final ExpireCallback<K, V> callback;

        private ExpiredEntry(K key, V value, ExpireCallback<K, V> callback) {
            this.key = key;
            this.value = value;
            this.callback = callback;
        }
    }

    private TreeMap<K, V> newTreeMap() {
        return comparator == null ? new TreeMap<>() : new TreeMap<>(comparator);
    }

    private void validateSortedKey(Map<K, V> existingData, K key) {
        for (K existingKey : existingData.keySet()) {
            int comparison = compareKeys(existingKey, key);
            if (comparison == 0 && !Objects.equals(existingKey, key)) {
                throw new IllegalArgumentException(
                        "Comparator considers different keys equal: "
                                + existingKey + " and " + key);
            }
            if (comparison != 0 && Objects.equals(existingKey, key)) {
                throw new IllegalArgumentException(
                        "Comparator is inconsistent with equals for key: " + key);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private int compareKeys(K left, K right) {
        if (comparator != null) {
            return comparator.compare(left, right);
        }
        return ((Comparable<? super K>) left).compareTo(right);
    }

    private static void reportBackgroundFailure(String message, Throwable throwable) {
        Thread.UncaughtExceptionHandler handler = Thread.getDefaultUncaughtExceptionHandler();
        if (handler != null) {
            Thread thread = Thread.currentThread();
            try {
                handler.uncaughtException(thread, new RuntimeException(message, throwable));
            } catch (Throwable ignored) {
                // Background maintenance must continue even if an error handler fails.
            }
        }
    }

    private static long saturatedAdd(long left, long right) {
        return right > Long.MAX_VALUE - left ? Long.MAX_VALUE : left + right;
    }

    /**
     * 转换SortedMap为LinkedHashMap
     *
     * @param sortedMap 排序map
     * @return LinkedHashMap 链接map
     */
    private Map<K, V> getLinkedMap(SortedMap<K, V> sortedMap) {
        return new LinkedHashMap<>(sortedMap);
    }

    /**
     * 重写toString函数
     *
     * @return FastMap对象字符串表示
     */
    public final String toString() {
        Iterator<Map.Entry<K, V>> iterator = entrySnapshot().iterator();
        if (!iterator.hasNext()) {
            return "{}";
        }

        StringBuilder builder = new StringBuilder();
        builder.append('{');
        while (true) {
            Map.Entry<K, V> entry = iterator.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            builder.append(key == this ? "(this Map)" : key);
            builder.append('=');
            builder.append(value == this ? "(this Map)" : value);
            if (!iterator.hasNext()) {
                return builder.append('}').toString();
            }
            builder.append(',').append(' ');
        }
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Map)) {
            return false;
        }
        Map<?, ?> other = (Map<?, ?>) object;
        if (other.size() != size()) {
            return false;
        }
        try {
            for (Map.Entry<K, V> entry : entrySnapshot()) {
                K key = entry.getKey();
                V value = entry.getValue();
                if (!Objects.equals(value, other.get(key))) {
                    return false;
                }
                if (value == null && !other.containsKey(key)) {
                    return false;
                }
            }
            return true;
        } catch (ClassCastException | NullPointerException ignored) {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (Map.Entry<K, V> entry : entrySnapshot()) {
            hashCode += entry.hashCode();
        }
        return hashCode;
    }
}
