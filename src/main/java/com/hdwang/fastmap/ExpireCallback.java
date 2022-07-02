package com.hdwang.fastmap;

/**
 * 键过期回调函数
 *
 * @author wanghuidong
 * 时间： 2022/7/2 13:14
 */
@FunctionalInterface
public interface ExpireCallback<K, V> {

    /**
     * 当键过期时执行的函数
     *
     * @param key 过期的键
     * @param val 过期的值
     */
    void onExpire(K key, V val);
}
