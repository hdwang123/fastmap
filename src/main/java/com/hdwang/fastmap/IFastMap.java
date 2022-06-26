package com.hdwang.fastmap;

import java.util.SortedMap;

/**
 * @author wanghuidong
 * 时间： 2022/6/26 11:26
 */
public interface IFastMap<K, V> extends SortedMap<K, V> {

    /**
     * 设置过期时间（n毫秒后）
     *
     * @param key 指定Key
     * @param ms  指定毫秒数后过期
     * @return 具体的过期时刻
     */
    Long expire(K key, Long ms);

    /**
     * 获取Key的存活时间
     *
     * @param key 指定Key
     * @return 存活时间（还有多少毫秒时间存活,NULL表示key不存在，负数表示已经过期的毫秒数）
     */
    Long ttl(K key);

    /**
     * 获取指定范围数据
     *
     * @param fromKey       开始键
     * @param fromInclusive 开始键是否包含
     * @param toKey         结束键
     * @param toInclusive   结束键是否包含
     * @return 指定范围数据
     */
    SortedMap<K, V> subMap(K fromKey, boolean fromInclusive,
                           K toKey, boolean toInclusive);

    /**
     * 获取头部范围数据（从第一个键到指定键）
     *
     * @param toKey     指定的结束键
     * @param inclusive 结束键是否包含
     * @return 指定范围数据
     */
    SortedMap<K, V> headMap(K toKey, boolean inclusive);

    /**
     * 获取尾部范围数据（从指定键到最后一个键）
     *
     * @param fromKey   指定的开始键
     * @param inclusive 开始就是否包含
     * @return 指定范围数据
     */
    SortedMap<K, V> tailMap(K fromKey, boolean inclusive);
}
