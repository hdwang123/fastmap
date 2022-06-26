package com.hdwang.fastmap;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author wanghuidong
 * 时间： 2022/6/26 10:10
 */
public class FastMap<K, V> implements IFastMap<K, V> {

    /**
     * 主要运用于等值查找
     */
    private HashMap<K, V> hashMap = new HashMap<>();

    /**
     * 主要运用于范围查找
     */
    private TreeMap<K, V> treeMap = new TreeMap<>();

    /**
     * 按照时间顺序保存了会过期key集合，为了实现快速删除，结构：时间戳->key列表
     */
    private TreeMap<Long, List<K>> expireKeysMap = new TreeMap<>();

    /**
     * 保存会过期key的过期时间
     */
    private HashMap<K, Long> keyExpireMap = new HashMap<>();

    /**
     * 是否启用排序
     */
    boolean enableSort = false;

    /**
     * The comparator used to maintain order in this tree map, or
     * null if it uses the natural ordering of its keys.
     *
     * @serial
     */
    private final Comparator<? super K> comparator;

    private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    //数据写锁
    private final Lock writeLock = readWriteLock.writeLock();

    //数据读锁
    private final Lock readLock = readWriteLock.readLock();

    private ReentrantReadWriteLock expireKeysReadWriteLock = new ReentrantReadWriteLock();

    //过期key写锁
    private final Lock expireKeysWriteLock = expireKeysReadWriteLock.writeLock();

    //过期key读锁
    private final Lock expireKeysReadLock = expireKeysReadWriteLock.readLock();

    private final static AtomicInteger nextSerialNumber = new AtomicInteger(0);

    private static int serialNumber() {
        return nextSerialNumber.getAndIncrement();
    }

    /**
     * 默认构造器，不启用排序
     */
    public FastMap() {
        this.comparator = null;
        this.enableSort = false;
        this.init();
    }

    /**
     * 构造器，enableSort配置是否启用排序
     *
     * @param enableSort 是否启用排序
     */
    public FastMap(boolean enableSort) {
        this.comparator = null;
        this.enableSort = enableSort;
        this.init();
    }

    /**
     * 构造器，启用排序，排序器由自己传入
     *
     * @param comparator 排序器
     */
    public FastMap(Comparator<? super K> comparator) {
        this.comparator = comparator;
        this.enableSort = true;
        this.init();
    }

    /**
     * 初始化
     */
    public void init() {
        //启用定时器，定时删除过期key,1秒后启动，定时1秒
        Timer timer = new Timer("expireTask-" + serialNumber(), true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                removeExpireData("timer");
            }
        }, 1000, 1000);

    }


    @Override
    public Comparator<? super K> comparator() {
        return this.comparator;
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("subMap");
        try {
            readLock.lock();
            return this.treeMap.subMap(fromKey, toKey);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("headMap");
        try {
            readLock.lock();
            return this.treeMap.headMap(toKey);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("tailMap");
        try {
            readLock.lock();
            return this.treeMap.tailMap(fromKey);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public K firstKey() {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("firstKey");
        try {
            readLock.lock();
            return this.treeMap.firstKey();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public K lastKey() {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("lastKey");
        try {
            readLock.lock();
            return this.treeMap.lastKey();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int size() {
        //先删除过期数据
        this.removeExpireData("size");
        try {
            readLock.lock();
            return this.hashMap.size();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        //先删除过期数据
        this.removeExpireData("isEmpty");
        try {
            readLock.lock();
            return this.hashMap.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        //先删除过期数据
        this.removeExpireData("containsKey");
        try {
            readLock.lock();
            return this.hashMap.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        //先删除过期数据
        this.removeExpireData("containsValue");
        try {
            readLock.lock();
            return this.hashMap.containsValue(value);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public V get(Object key) {
        //先删除过期数据
        this.removeExpireData("get");
        try {
            readLock.lock();
            return this.hashMap.get(key);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            writeLock.lock();
            V val = this.hashMap.put(key, value);
            if (enableSort) {
                val = this.treeMap.put(key, value);
            }
            return val;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public V remove(Object key) {
        try {
            writeLock.lock();
            V val = this.hashMap.remove(key);
            if (enableSort) {
                val = this.treeMap.remove(key);
            }
            return val;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        try {
            writeLock.lock();
            this.hashMap.putAll(m);
            if (enableSort) {
                this.treeMap.putAll(m);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clear() {
        try {
            writeLock.lock();
            this.hashMap.clear();
            if (enableSort) {
                this.treeMap.clear();
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Set<K> keySet() {
        //先删除过期数据
        this.removeExpireData("keySet");
        try {
            readLock.lock();
            if (enableSort) {
                return this.treeMap.keySet();
            }
            return this.hashMap.keySet();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<V> values() {
        //先删除过期数据
        this.removeExpireData("values");
        try {
            readLock.lock();
            if (enableSort) {
                return this.treeMap.values();
            }
            return this.hashMap.values();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        //先删除过期数据
        this.removeExpireData("entrySet");
        try {
            readLock.lock();
            if (enableSort) {
                return this.treeMap.entrySet();
            }
            return this.hashMap.entrySet();
        } finally {
            readLock.unlock();
        }
    }


    @Override
    public Long expire(K key, Long ms) {
        try {
            expireKeysWriteLock.lock();

            //判断是否已经设置过过期时间
            Long expireTime = this.keyExpireMap.get(key);
            if (expireTime != null) {
                //清除之前设置的过期时间
                this.keyExpireMap.remove(key);
                this.expireKeysMap.get(expireTime).remove(key);
            }
            expireTime = System.currentTimeMillis() + ms;
            this.keyExpireMap.put(key, expireTime);
            List<K> keys = this.expireKeysMap.get(expireTime);
            if (keys == null) {
                keys = new ArrayList<>();
                keys.add(key);
                this.expireKeysMap.put(expireTime, keys);
            } else {
                keys.add(key);
            }
            return expireTime;
        } finally {
            expireKeysWriteLock.unlock();
        }
    }

    @Override
    public Long ttl(K key) {
        V val = this.get(key);
        if (val == null) {
            //数据不存在,存活时间返回null
            return null;
        }
        try {
            expireKeysReadLock.lock();
            Long expireTime = this.keyExpireMap.get(key);
            return expireTime - System.currentTimeMillis();
        } finally {
            expireKeysReadLock.unlock();
        }
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("subMap");
        try {
            readLock.lock();
            return this.treeMap.subMap(fromKey, fromInclusive, toKey, toInclusive);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SortedMap<K, V> headMap(K toKey, boolean inclusive) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("headMap");
        try {
            readLock.lock();
            return this.treeMap.headMap(toKey, inclusive);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey, boolean inclusive) {
        if (!enableSort) {
            throw new RuntimeException("未启用排序");
        }
        //先删除过期数据
        this.removeExpireData("tailMap");
        try {
            readLock.lock();
            return this.treeMap.tailMap(fromKey, inclusive);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 删除过期的数据
     */
    private void removeExpireData(String flag) {
        //查找过期key
        Long curTimestamp = System.currentTimeMillis();
        SortedMap<Long, List<K>> expiredKeysMap;
        try {
            expireKeysReadLock.lock();
            //过期时间在【从前至此刻】区间内的都为过期的key
            expiredKeysMap = this.expireKeysMap.headMap(curTimestamp, true);
            System.out.println(String.format("thread:%s caller:%s removeExpireData【curTime=%s,expiredKeysMap=%s】", Thread.currentThread().getName(), flag, curTimestamp, expiredKeysMap));
        } finally {
            expireKeysReadLock.unlock();
        }

        //删除过期数据
        List<Long> timeKeys = new ArrayList<>();
        List<K> keys = new ArrayList<>();
        for (Entry<Long, List<K>> entry : expiredKeysMap.entrySet()) {
            timeKeys.add(entry.getKey());
            for (K key : entry.getValue()) {
                this.remove(key);

                keys.add(key);
            }
        }

        //清理过期key
        try {
            expireKeysWriteLock.lock();
            //删除过期key集合
            for (Long key : timeKeys) {
                this.expireKeysMap.remove(key);
            }

            for (K key : keys) {
                //删除过期key的过期时间戳
                this.keyExpireMap.remove(key);
            }
        } finally {
            expireKeysWriteLock.unlock();
        }
    }
}
