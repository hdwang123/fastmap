package com.hdwang.fastmap;

import java.util.Comparator;
import java.util.Map;

/**
 * 一个支持等值查找、范围查找、数据过期、键排序等功能的线程安全Map，适合做本地缓存。
 *
 * @author wanghuidong
 * 时间： 2022/6/26 11:26
 */
public interface IFastMap<K, V> extends Map<K, V> {

    /**
     * 获取排序器
     *
     * @return 排序器
     */
    Comparator<? super K> comparator();

    /**
     * 获取指定范围数据
     *
     * @param fromKey 开始键(包含)
     * @param toKey   结束键（不包含）
     * @return 指定范围数据
     */
    Map<K, V> subMap(K fromKey, K toKey);

    /**
     * 获取指定范围数据
     *
     * @param fromKey       开始键
     * @param fromInclusive 开始键是否包含
     * @param toKey         结束键
     * @param toInclusive   结束键是否包含
     * @return 指定范围数据
     */
    Map<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive);

    /**
     * 获取头部范围数据（从第一个键到指定键）
     *
     * @param toKey 指定的结束键（不包含）
     * @return 指定范围数据
     */
    Map<K, V> headMap(K toKey);

    /**
     * 获取头部范围数据（从第一个键到指定键）
     *
     * @param toKey     指定的结束键
     * @param inclusive 结束键是否包含
     * @return 指定范围数据
     */
    Map<K, V> headMap(K toKey, boolean inclusive);

    /**
     * 获取尾部范围数据（从指定键到最后一个键）
     *
     * @param fromKey 指定的开始键（包含）
     * @return 指定范围数据
     */
    Map<K, V> tailMap(K fromKey);

    /**
     * 获取尾部范围数据（从指定键到最后一个键）
     *
     * @param fromKey   指定的开始键
     * @param inclusive 开始键是否包含
     * @return 指定范围数据
     */
    Map<K, V> tailMap(K fromKey, boolean inclusive);

    /**
     * 获取第一个键
     *
     * @return 第一个键
     */
    K firstKey();

    /**
     * 获取最后一个键
     *
     * @return 最后一个键
     */
    K lastKey();


    /**
     * 设置过期时间（n毫秒后），重复调用可以重置过期时间
     *
     * @param key 指定Key
     * @param ms  指定毫秒数后过期
     * @return 具体的过期时刻
     */
    Long expire(K key, Long ms);

    /**
     * 设置过期时间（n毫秒后）和过期回调函数,重复调用可以重置过期时间和回调函数。
     * <p>
     * 如果仅需重置过期时间可调用: expire(K key, Long ms);
     * </p>
     *
     * @param key      指定key
     * @param ms       指定毫秒数后过期
     * @param callback 过期回调函数
     * @return 具体的过期时刻
     */
    Long expire(K key, Long ms, ExpireCallback<K, V> callback);

    /**
     * 获取Key的存活时间
     *
     * @param key 指定Key
     * @return 存活时间（还有多少毫秒时间存活,NULL表示key不存在或过期时间未设置，负数表示已经过期的毫秒数）
     */
    Long ttl(K key);


}
